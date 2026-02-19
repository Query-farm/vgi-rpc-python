"""Stream types, method metadata, and protocol introspection."""

from __future__ import annotations

import abc
import functools
import inspect
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType, TracebackType
from typing import (
    Annotated,
    Any,
    Self,
    get_args,
    get_origin,
    get_type_hints,
)

import pyarrow as pa

from vgi_rpc.log import Level, Message
from vgi_rpc.metadata import SERVER_ID_KEY, encode_metadata, merge_metadata
from vgi_rpc.rpc._common import (
    _EMPTY_SCHEMA,
    CallContext,
    MethodType,
)
from vgi_rpc.utils import ArrowSerializableDataclass, _infer_arrow_type, _is_optional_type, empty_batch

# ---------------------------------------------------------------------------
# Stream return types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AnnotatedBatch:
    """A RecordBatch paired with its custom metadata.

    Used as both input and output for all batch I/O in stream methods.
    """

    batch: pa.RecordBatch
    custom_metadata: pa.KeyValueMetadata | None = None
    _release_fn: Callable[[], None] | None = field(default=None, repr=False, compare=False, hash=False)

    def release(self) -> None:
        """Release associated shared memory region.

        After calling release(), the batch data may be overwritten —
        accessing batch column data after release is undefined behavior.
        No-op if the batch did not come from shared memory.
        """
        if self._release_fn is not None:
            self._release_fn()

    @classmethod
    def from_pydict(
        cls,
        data: dict[str, Any],
        schema: pa.Schema | None = None,
    ) -> AnnotatedBatch:
        """Create from a Python dict, optionally with a schema."""
        batch = pa.RecordBatch.from_pydict(data, schema=schema)
        return cls(batch=batch)


class OutputCollector:
    """Accumulates output batches during a produce/process call.

    Enforces that exactly one data batch is emitted per call (plus any number
    of log batches).  Batches are stored in a single ordered list because
    interleaving order matters for the wire protocol (logs must precede the
    data batch they annotate).
    """

    __slots__ = (
        "_batches",
        "_data_batch_idx",
        "_finished",
        "_output_schema",
        "_prior_data_bytes",
        "_producer_mode",
        "_server_id",
    )

    def __init__(
        self,
        output_schema: pa.Schema,
        *,
        prior_data_bytes: int = 0,
        server_id: str | None = None,
        producer_mode: bool = True,
    ) -> None:
        """Initialize with the output schema for this stream.

        Args:
            output_schema: The Arrow schema for data batches.
            prior_data_bytes: Cumulative data bytes from earlier produce/process calls in this stream.
            server_id: Optional server identifier injected into log batch metadata.
            producer_mode: When ``True`` (default), ``finish()`` is allowed.
                Set to ``False`` for exchange streams where ``finish()`` is
                not permitted.

        """
        self._output_schema = output_schema
        self._batches: list[AnnotatedBatch] = []
        self._finished: bool = False
        self._data_batch_idx: int | None = None
        self._prior_data_bytes = prior_data_bytes
        self._producer_mode = producer_mode
        self._server_id = server_id

    @property
    def output_schema(self) -> pa.Schema:
        """The output schema for this stream."""
        return self._output_schema

    @property
    def finished(self) -> bool:
        """Whether finish() has been called."""
        return self._finished

    @property
    def total_data_bytes(self) -> int:
        """Cumulative data bytes emitted across the stream so far.

        Includes bytes from prior produce/process calls plus the current
        data batch (if one has been emitted).  Measured via
        ``pa.RecordBatch.get_total_buffer_size()``.
        """
        current = (
            self._batches[self._data_batch_idx].batch.get_total_buffer_size() if self._data_batch_idx is not None else 0
        )
        return self._prior_data_bytes + current

    @property
    def batches(self) -> list[AnnotatedBatch]:
        """The accumulated batches."""
        return self._batches

    # --- Data emission (exactly one per call) ---

    @property
    def data_batch(self) -> AnnotatedBatch:
        """Return the single data batch, or raise if none was emitted."""
        if self._data_batch_idx is None:
            raise RuntimeError("No data batch was emitted")
        return self._batches[self._data_batch_idx]

    def validate(self) -> None:
        """Assert that exactly one data batch was emitted.

        Raises:
            RuntimeError: If no data batch was emitted.

        """
        if self._data_batch_idx is None:
            raise RuntimeError("No data batch was emitted")

    def merge_data_metadata(self, metadata: pa.KeyValueMetadata) -> None:
        """Merge extra metadata into the data batch.

        Raises:
            RuntimeError: If no data batch was emitted.

        """
        if self._data_batch_idx is None:
            raise RuntimeError("No data batch was emitted")
        ab = self._batches[self._data_batch_idx]
        self._batches[self._data_batch_idx] = AnnotatedBatch(
            batch=ab.batch,
            custom_metadata=merge_metadata(ab.custom_metadata, metadata),
        )

    def emit(
        self,
        batch: pa.RecordBatch,
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Emit a pre-built data batch. Raises if a data batch was already emitted."""
        if self._data_batch_idx is not None:
            raise RuntimeError("Only one data batch may be emitted per call")
        self._data_batch_idx = len(self._batches)
        custom_metadata = encode_metadata(metadata) if metadata else None
        self._batches.append(AnnotatedBatch(batch=batch, custom_metadata=custom_metadata))

    def emit_arrays(
        self,
        arrays: Sequence[pa.Array[Any]],
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Build a RecordBatch from arrays using output_schema and emit it."""
        batch = pa.RecordBatch.from_arrays(arrays, schema=self._output_schema)
        self.emit(batch, metadata=metadata)

    def emit_pydict(
        self,
        data: dict[str, Any],
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Build a RecordBatch from a Python dict using output_schema and emit it."""
        batch = pa.RecordBatch.from_pydict(data, schema=self._output_schema)
        self.emit(batch, metadata=metadata)

    # --- Logging (zero or more per call) ---

    def emit_client_log_message(self, msg: Message) -> None:
        """Append a zero-row client-directed log batch (used by CallContext.emit_client_log in produce/process)."""
        md = msg.add_to_metadata()
        if self._server_id is not None:
            md[SERVER_ID_KEY.decode()] = self._server_id
        custom_metadata = encode_metadata(md)
        self._batches.append(AnnotatedBatch(batch=empty_batch(self._output_schema), custom_metadata=custom_metadata))

    def client_log(self, level: Level, message: str, **extra: str) -> None:
        """Emit a zero-row client-directed log batch with log metadata."""
        self.emit_client_log_message(Message(level, message, **extra))

    # --- Stream completion (producer streams only) ---

    def finish(self) -> None:
        """Signal stream completion for producer streams.

        Producer streams (``input_schema == _EMPTY_SCHEMA``) call this
        to signal that no more data will be produced.

        Raises:
            RuntimeError: If called on an exchange stream (``producer_mode=False``).

        """
        if not self._producer_mode:
            raise RuntimeError(
                "finish() is not allowed on exchange streams; "
                "exchange streams must emit exactly one data batch per call"
            )
        self._finished = True


class StreamState(ArrowSerializableDataclass, abc.ABC):
    """Base class for stream state objects.

    Subclasses must be dataclasses that define ``process(input, out, ctx)``
    which is called once per input batch.

    For producer streams, ``input`` is a zero-row empty-schema tick
    batch that can be ignored; call ``out.finish()`` to signal stream end.

    For exchange streams, ``input`` is real data.
    State is mutated in-place across calls.

    Extends ``ArrowSerializableDataclass`` so that state can be serialized
    between requests (required for HTTP transport).
    """

    @abc.abstractmethod
    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process an input batch and emit output into the collector."""
        ...


class ProducerState(StreamState, abc.ABC):
    """Base class for producer stream state objects.

    Subclasses implement ``produce(out, ctx)`` instead of ``process()``,
    eliminating the phantom ``input`` parameter that producer streams
    must otherwise ignore.

    Call ``out.finish()`` from ``produce()`` to signal end of stream.
    """

    @abc.abstractmethod
    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce output batches into the collector.

        Args:
            out: The output collector to emit batches into.
            ctx: The call context for this request.

        """
        ...

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Delegate to ``produce()``, ignoring the tick input."""
        self.produce(out, ctx)


class ExchangeState(StreamState, abc.ABC):
    """Base class for exchange stream state objects.

    Subclasses implement ``exchange(input, out, ctx)`` instead of
    ``process()``.  Exchange streams must emit exactly one data batch
    per call and must not call ``out.finish()``.
    """

    @abc.abstractmethod
    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process an input batch and emit exactly one output batch.

        Args:
            input: The input batch from the client.
            out: The output collector to emit the response batch into.
            ctx: The call context for this request.

        """
        ...

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Delegate to ``exchange()``."""
        self.exchange(input, out, ctx)


_TICK_BATCH = AnnotatedBatch(batch=empty_batch(_EMPTY_SCHEMA))
"""Cached zero-row empty-schema batch used as tick input for producer streams."""


@dataclass(frozen=True)
class Stream[S: StreamState, H: (ArrowSerializableDataclass | None) = None]:
    """Return type for stream RPC methods.

    Bundles the output schema with a state object whose ``process(input, out, ctx)``
    method is called once per input batch.

    For producer streams, ``input_schema`` is ``_EMPTY_SCHEMA`` (the default)
    and the client iterates via ``__iter__`` / ``tick()``.  For exchange
    streams, ``input_schema`` is set to the real input schema and the client
    uses ``exchange()`` / context manager.

    The optional second type parameter ``H`` specifies a header type — an
    ``ArrowSerializableDataclass`` that is sent once before the main data
    stream begins.  Use ``Stream[MyState, MyHeader]`` to declare a header;
    omit it (``Stream[MyState]``) for streams without headers.

    Client-side stub methods provide accurate types for IDE autocompletion.
    """

    output_schema: pa.Schema
    state: S
    input_schema: pa.Schema = _EMPTY_SCHEMA
    header: H | None = None

    def __iter__(self) -> Iterator[AnnotatedBatch]:
        """Iterate over output batches (client-side stub for producer streams).

        Raises:
            NotImplementedError: Always — this is a server-side type.
                Use ``StreamSession`` on the client.

        """
        raise NotImplementedError("Stream is a server-side type; iterate StreamSession on the client")

    def __enter__(self) -> Self:
        """Enter context manager (client-side stub for exchange streams).

        Raises:
            NotImplementedError: Always — this is a server-side type.
                Use ``StreamSession`` on the client.

        """
        raise NotImplementedError("Stream is a server-side type; use StreamSession on the client")

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager (client-side stub).

        Raises:
            NotImplementedError: Always — this is a server-side type.

        """
        raise NotImplementedError

    def exchange(self, input: AnnotatedBatch) -> AnnotatedBatch:
        """Exchange an input batch for an output batch (client-side stub).

        Args:
            input: The input batch to send.

        Raises:
            NotImplementedError: Always — this is a server-side type.
                Use ``StreamSession`` on the client.

        """
        raise NotImplementedError

    def tick(self) -> AnnotatedBatch:
        """Send a tick and receive output (client-side stub for producer streams).

        Raises:
            NotImplementedError: Always — this is a server-side type.
                Use ``StreamSession`` on the client.

        """
        raise NotImplementedError

    def close(self) -> None:
        """Close the stream (client-side stub).

        Raises:
            NotImplementedError: Always — this is a server-side type.
                Use ``StreamSession`` on the client.

        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# RpcMethodInfo
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RpcMethodInfo:
    """Metadata for a single RPC method, derived from Protocol type hints.

    Produced by :func:`rpc_methods` when introspecting a Protocol class.
    Each instance describes one method's wire-protocol details: its Arrow
    schemas, parameter types and defaults, and the original docstring.

    Attributes:
        name: Method name as it appears on the Protocol.
        params_schema: Arrow schema for the serialized request parameters.
        result_schema: Arrow schema for the serialized response (unary only;
            empty schema for stream methods).
        result_type: The raw Python return-type annotation (e.g. ``float``,
            ``Stream[MyState]``).
        method_type: Whether this is a ``UNARY`` or ``STREAM`` call.
        has_return: ``True`` when the unary method returns a value (``False``
            for ``-> None`` or stream methods).
        doc: The method's docstring from the Protocol class, or ``None`` if
            no docstring was provided.
        param_defaults: Mapping of parameter name to default value for
            parameters that have defaults in the Protocol signature.
        param_types: Mapping of parameter name to its Python type annotation
            (excludes ``self`` and ``return``).
        header_type: For stream methods with a header, the concrete
            ``ArrowSerializableDataclass`` subclass for the header.
            ``None`` when the method has no header.

    """

    name: str
    params_schema: pa.Schema
    result_schema: pa.Schema
    result_type: Any
    method_type: MethodType
    has_return: bool
    doc: str | None
    param_defaults: dict[str, Any] = field(default_factory=dict)
    param_types: dict[str, Any] = field(default_factory=dict)
    header_type: type[ArrowSerializableDataclass] | None = None


# ---------------------------------------------------------------------------
# Protocol introspection
# ---------------------------------------------------------------------------


def _unwrap_annotated(hint: Any) -> Any:
    """Unwrap Annotated[T, ...] to T, or return hint unchanged."""
    if get_origin(hint) is Annotated:
        return get_args(hint)[0]
    return hint


def _classify_return_type(hint: Any) -> tuple[MethodType, Any, bool]:
    """Classify a return type hint into a MethodType.

    Returns (method_type, result_type, has_return).
    Handles both bare ``Stream`` and generic forms like ``Stream[MyState]``.
    """
    # Bare class reference
    if hint is Stream:
        return MethodType.STREAM, hint, False

    # Generic form: Stream[S]
    origin = get_origin(hint)
    if origin is Stream:
        return MethodType.STREAM, hint, False

    # Everything else -> UNARY
    has_return = hint is not type(None) and hint is not None
    return MethodType.UNARY, hint, has_return


def _build_params_schema(hints: dict[str, Any]) -> pa.Schema:
    """Build an Arrow schema from method parameter type hints (excluding 'self' and 'return')."""
    fields: list[pa.Field[pa.DataType]] = []
    for name, hint in hints.items():
        if name in ("self", "return"):
            continue
        inner, is_nullable = _is_optional_type(hint)
        base = _unwrap_annotated(inner)
        if isinstance(base, type) and issubclass(base, ArrowSerializableDataclass):
            fields.append(pa.field(name, pa.binary(), nullable=is_nullable))
        else:
            arrow_type = _infer_arrow_type(inner)  # handles Annotated natively
            fields.append(pa.field(name, arrow_type, nullable=is_nullable))
    return pa.schema(fields)


def _build_result_schema(result_type: Any) -> pa.Schema:
    """Build a single-field Arrow schema for a unary result type."""
    if result_type is type(None) or result_type is None:
        return _EMPTY_SCHEMA

    # ArrowSerializableDataclass — serialize whole dataclass as binary blob
    base = _unwrap_annotated(result_type)
    if isinstance(base, type) and issubclass(base, ArrowSerializableDataclass):
        return pa.schema([pa.field("result", pa.binary())])

    inner, is_nullable = _is_optional_type(result_type)
    arrow_type = _infer_arrow_type(inner)  # handles Annotated natively
    return pa.schema([pa.field("result", arrow_type, nullable=is_nullable)])


_UNSUPPORTED_PARAM_KINDS: dict[int, str] = {
    inspect.Parameter.POSITIONAL_ONLY: "positional-only (before '/')",
    inspect.Parameter.VAR_POSITIONAL: "*args",
    inspect.Parameter.VAR_KEYWORD: "**kwargs",
}


def _validate_protocol_params(protocol: type, method_name: str, sig: inspect.Signature) -> None:
    """Validate that protocol method parameters are compatible with the wire protocol.

    The RPC wire protocol identifies parameters by name (Arrow schema columns),
    so all parameters must be passable as keyword arguments. This rejects
    positional-only, ``*args``, and ``**kwargs`` parameters.

    Raises:
        TypeError: If any parameter uses an unsupported kind.

    """
    errors: list[str] = []
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        label = _UNSUPPORTED_PARAM_KINDS.get(param.kind)
        if label is not None:
            errors.append(f"  - '{name}' is {label}")
    if errors:
        detail = "\n".join(errors)
        raise TypeError(
            f"{protocol.__name__}.{method_name}() has parameters incompatible"
            f" with the RPC wire protocol (all parameters must be keyword-passable):\n{detail}"
        )


def _get_param_defaults(protocol: type, method_name: str) -> dict[str, Any]:
    """Extract default values for method parameters."""
    method = getattr(protocol, method_name, None)
    if method is None:
        return {}
    sig = inspect.signature(method)
    defaults: dict[str, Any] = {}
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        if param.default is not inspect.Parameter.empty:
            defaults[name] = param.default
    return defaults


@functools.lru_cache(maxsize=64)
def rpc_methods(protocol: type) -> Mapping[str, RpcMethodInfo]:
    """Introspect a Protocol class and return RpcMethodInfo for each method.

    Skips underscore-prefixed names and non-callable attributes.
    """
    result: dict[str, RpcMethodInfo] = {}

    # Get method names from Protocol — look at annotations and callables
    for name in dir(protocol):
        if name.startswith("_"):
            continue
        attr = getattr(protocol, name, None)
        if attr is None or not callable(attr):
            continue

        try:
            method_hints = get_type_hints(attr, include_extras=True)
        except (NameError, AttributeError) as exc:
            raise TypeError(f"Failed to resolve type hints for {protocol.__name__}.{name}(): {exc}") from exc

        _validate_protocol_params(protocol, name, inspect.signature(attr))

        return_hint = method_hints.get("return", type(None))
        method_type, result_type, has_return = _classify_return_type(return_hint)

        # For unary methods, build result schema from the result type
        result_schema = _build_result_schema(result_type) if method_type == MethodType.UNARY else _EMPTY_SCHEMA

        # Extract header type from Stream[S, H] annotations
        header_type: type[ArrowSerializableDataclass] | None = None
        if method_type == MethodType.STREAM:
            stream_args = get_args(return_hint)
            if len(stream_args) >= 2:
                h_arg = stream_args[1]
                if isinstance(h_arg, type) and issubclass(h_arg, ArrowSerializableDataclass):
                    header_type = h_arg

        param_defaults = _get_param_defaults(protocol, name)
        params_schema = _build_params_schema(method_hints)
        param_types = {k: v for k, v in method_hints.items() if k not in ("self", "return")}

        doc = getattr(attr, "__doc__", None)

        result[name] = RpcMethodInfo(
            name=name,
            params_schema=params_schema,
            result_schema=result_schema,
            result_type=result_type,
            method_type=method_type,
            has_return=has_return,
            doc=doc,
            param_defaults=param_defaults,
            param_types=param_types,
            header_type=header_type,
        )

    return MappingProxyType(result)


# ---------------------------------------------------------------------------
# Implementation validation
# ---------------------------------------------------------------------------


def _format_signature(info: RpcMethodInfo) -> str:
    """Format a protocol method signature for error messages."""
    params = ", ".join(f"{n}: {getattr(t, '__name__', str(t))}" for n, t in info.param_types.items())
    return f"{info.name}({params})"


def _validate_implementation(
    protocol: type,
    implementation: object,
    methods: Mapping[str, RpcMethodInfo],
) -> None:
    """Validate that *implementation* conforms to *protocol*.

    Checks that every method declared in the protocol exists on the
    implementation, is callable, and has a compatible parameter list.
    The special ``ctx`` parameter is allowed on implementations
    even when not present in the protocol.

    Raises:
        TypeError: If one or more validation errors are found.  The
            message lists every problem so the developer can fix them
            all in one pass.

    """
    errors: list[str] = []

    for name, info in methods.items():
        method = getattr(implementation, name, None)

        if method is None:
            errors.append(f"missing method {_format_signature(info)}")
            continue

        if not callable(method):
            errors.append(f"'{name}' exists but is not callable")
            continue

        impl_sig = inspect.signature(method)
        impl_params = {k: v for k, v in impl_sig.parameters.items() if k != "self"}
        proto_param_names = set(info.param_types.keys())

        errors.extend(
            f"'{name}()' missing parameter '{param_name}'"
            for param_name in proto_param_names
            if param_name not in impl_params
        )

        for param_name, param in impl_params.items():
            if param_name in proto_param_names:
                continue
            if param_name == "ctx":
                continue
            if param.default is inspect.Parameter.empty and param.kind not in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                errors.append(f"'{name}()' has required parameter '{param_name}' not defined in {protocol.__name__}")

    if errors:
        impl_name = type(implementation).__name__
        header = f"{impl_name} does not implement {protocol.__name__}:"
        detail = "\n".join(f"  - {e}" for e in errors)
        raise TypeError(f"{header}\n{detail}")
