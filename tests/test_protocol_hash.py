# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The protocol hash and its canonical-JSON preimage.

The hash is only worth carrying if the same protocol hashes the same in every
port.  The previous definition hashed ``schema.serialize()`` bytes and the docs
disclaimed cross-language stability, which made the field comparable only
against itself.  These tests pin the properties that make it a contract: it
changes when the wire surface changes, it does not change when anything else
does, and its preimage is a structure a failing port can diff.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar, Protocol

import pyarrow as pa
import pytest

from vgi_rpc.rpc import RpcServer
from vgi_rpc.rpc._protocol_hash import (
    HASH_DOMAIN,
    canonical_json,
    compute_protocol_hash,
    protocol_description,
)


class _Base(Protocol):
    """Baseline protocol the mutation tests vary one field at a time from."""

    protocol_name: ClassVar[str] = "demo.Hash.v1"

    def echo(self, value: str) -> str:
        """Return the value."""
        ...


class _BaseImpl:
    def echo(self, value: str) -> str:
        return value


def _hash(protocol: type, impl: object) -> str:
    srv = RpcServer(protocol, impl)
    binding = srv.bindings[srv.protocol_name]
    return compute_protocol_hash(binding.name, binding.methods)


class TestCanonicalJson:
    """RFC 8785, restricted to the subset the preimage actually uses."""

    def test_keys_are_sorted(self) -> None:
        """Insertion order must not reach the bytes."""
        assert canonical_json({"b": "2", "a": "1"}) == b'{"a":"1","b":"2"}'

    def test_no_insignificant_whitespace(self) -> None:
        """Separators are fixed; a pretty-printing port would diverge."""
        assert b" " not in canonical_json({"a": ["1", "2"]})

    def test_arrays_keep_their_order(self) -> None:
        """Arrays are ordered data, not sets -- schema field order depends on it."""
        assert canonical_json(["b", "a"]) == b'["b","a"]'

    def test_non_ascii_is_emitted_as_utf8_not_escaped(self) -> None:
        r"""JCS emits the character; ``ensure_ascii`` would emit ``\u00e9``.

        Two ports disagreeing here produce different bytes for the same
        structure, which is the whole failure the canonical form prevents.
        """
        assert canonical_json({"name": "café"}) == '{"name":"café"}'.encode()

    def test_control_characters_use_the_short_escapes(self) -> None:
        r"""JCS mandates \n over \u000a where a short form exists."""
        assert canonical_json({"a": "x\ny"}) == b'{"a":"x\\ny"}'

    def test_quote_and_backslash_are_escaped(self) -> None:
        """The two escapes every JSON writer must agree on."""
        assert canonical_json({"a": '"\\'}) == b'{"a":"\\"\\\\"}'

    def test_booleans_survive(self) -> None:
        """The shape flags are booleans and must not become strings."""
        assert canonical_json({"a": True, "b": False}) == b'{"a":true,"b":false}'

    @pytest.mark.parametrize("bad", [1, 1.5, {"a": 2}, ["x", 3]])
    def test_numbers_are_refused(self, bad: Any) -> None:
        """Number canonicalisation is JCS's hardest rule; the preimage avoids it.

        Every numeric parameter is folded into a type token, so a number
        reaching here means one leaked out of its token.
        """
        with pytest.raises(TypeError, match="carries no numbers"):
            canonical_json(bad)

    def test_the_output_is_parseable_json(self) -> None:
        """A failing port should be able to diff the preimage, not guess at it."""
        blob = canonical_json({"protocol": "p", "methods": []})
        assert json.loads(blob) == {"protocol": "p", "methods": []}


class TestStability:
    """The hash follows the wire surface and nothing else."""

    def test_is_deterministic(self) -> None:
        """Two servers over the same protocol agree."""
        assert _hash(_Base, _BaseImpl()) == _hash(_Base, _BaseImpl())

    def test_is_domain_separated(self) -> None:
        """The tag keeps the digest from colliding with another use of sha256."""
        assert HASH_DOMAIN == b"vgi_rpc.protocol_hash.v1|"

    def test_docstrings_do_not_change_it(self) -> None:
        """Prose varies between ports and builds without changing the wire."""

        class _Documented(Protocol):
            """A quite differently documented protocol."""

            protocol_name: ClassVar[str] = "demo.Hash.v1"

            def echo(self, value: str) -> str:
                """Echo the supplied value back to the caller, verbatim, always."""
                ...

        assert _hash(_Documented, _BaseImpl()) == _hash(_Base, _BaseImpl())

    def test_the_protocol_name_changes_it(self) -> None:
        """The name carries the major version, so a major bump is a new hash."""

        class _V2(Protocol):
            protocol_name: ClassVar[str] = "demo.Hash.v2"

            def echo(self, value: str) -> str:
                """Return the value."""
                ...

        assert _hash(_V2, _BaseImpl()) != _hash(_Base, _BaseImpl())

    def test_a_renamed_parameter_changes_it(self) -> None:
        """The case a version gate exists for: same method name, new signature."""

        class _Renamed(Protocol):
            protocol_name: ClassVar[str] = "demo.Hash.v1"

            def echo(self, text: str) -> str:
                """Return the value."""
                ...

        class _RenamedImpl:
            def echo(self, text: str) -> str:
                return text

        assert _hash(_Renamed, _RenamedImpl()) != _hash(_Base, _BaseImpl())

    def test_a_retyped_parameter_changes_it(self) -> None:
        """Arrow has no field numbers, so a retype is not self-detecting."""

        class _Retyped(Protocol):
            protocol_name: ClassVar[str] = "demo.Hash.v1"

            def echo(self, value: int) -> str:
                """Return the value."""
                ...

        class _RetypedImpl:
            def echo(self, value: int) -> str:
                return str(value)

        assert _hash(_Retyped, _RetypedImpl()) != _hash(_Base, _BaseImpl())

    def test_an_added_method_changes_it(self) -> None:
        """Growing the surface is a change even though old calls still work."""

        class _Grown(Protocol):
            protocol_name: ClassVar[str] = "demo.Hash.v1"

            def echo(self, value: str) -> str:
                """Return the value."""
                ...

            def ping(self) -> str:
                """Return a greeting."""
                ...

        class _GrownImpl:
            def echo(self, value: str) -> str:
                return value

            def ping(self) -> str:
                return "pong"

        assert _hash(_Grown, _GrownImpl()) != _hash(_Base, _BaseImpl())

    def test_method_declaration_order_does_not_change_it(self) -> None:
        """Methods are sorted, so source order is not part of the surface."""

        class _OrderA(Protocol):
            protocol_name: ClassVar[str] = "demo.Order.v1"

            def a(self) -> str:
                """Return an a."""
                ...

            def b(self) -> str:
                """Return a b."""
                ...

        class _OrderB(Protocol):
            protocol_name: ClassVar[str] = "demo.Order.v1"

            def b(self) -> str:
                """Return a b."""
                ...

            def a(self) -> str:
                """Return an a."""
                ...

        class _OrderImpl:
            def a(self) -> str:
                return "a"

            def b(self) -> str:
                return "b"

        assert _hash(_OrderA, _OrderImpl()) == _hash(_OrderB, _OrderImpl())


class TestPreimageShape:
    """The preimage is the artifact a failing port diffs, so its shape is pinned."""

    def test_methods_are_sorted_by_name(self) -> None:
        """A port iterating a hash map must still produce this order."""
        srv = RpcServer(_Base, _BaseImpl())
        binding = srv.bindings[srv.protocol_name]
        desc = protocol_description(binding.name, binding.methods)
        names = [m["name"] for m in desc["methods"]]
        assert names == sorted(names)

    def test_a_method_with_no_return_omits_the_result_key(self) -> None:
        """Absent and empty must not hash alike.

        A method returning nothing is not a method returning an empty struct.
        """

        class _Void(Protocol):
            protocol_name: ClassVar[str] = "demo.Void.v1"

            def fire(self, value: str) -> None:
                """Take a value and return nothing."""
                ...

        class _VoidImpl:
            def fire(self, value: str) -> None:
                return None

        srv = RpcServer(_Void, _VoidImpl())
        binding = srv.bindings[srv.protocol_name]
        entry = protocol_description(binding.name, binding.methods)["methods"][0]
        assert "result" not in entry

    def test_carries_no_server_identity(self) -> None:
        """Two processes serving one protocol must agree, so nothing per-process."""
        srv = RpcServer(_Base, _BaseImpl())
        binding = srv.bindings[srv.protocol_name]
        blob = canonical_json(protocol_description(binding.name, binding.methods)).decode()
        assert srv.server_id not in blob

    def test_carries_no_framework_version(self) -> None:
        """REQUEST_VERSION and DESCRIBE_VERSION describe the framework, not this protocol.

        Folding them in would rotate every protocol's hash on a framework
        release that changed nothing about any protocol.
        """
        srv = RpcServer(_Base, _BaseImpl())
        binding = srv.bindings[srv.protocol_name]
        desc = protocol_description(binding.name, binding.methods)
        assert set(desc) == {"protocol", "methods"}

    def test_types_appear_as_tokens_not_as_arrow_bytes(self) -> None:
        """The decoded structure is the contract; encoder output is not."""
        srv = RpcServer(_Base, _BaseImpl())
        binding = srv.bindings[srv.protocol_name]
        entry = protocol_description(binding.name, binding.methods)["methods"][0]
        assert entry["params"] == [{"name": "value", "nullable": False, "type": "utf8"}]

    def test_the_same_schema_from_a_different_encoder_hashes_the_same(self) -> None:
        """Re-encoding through IPC changes bytes, not the decoded structure.

        This is the property the byte-based definition could not offer, and
        the reason the docs used to disclaim cross-language stability.
        """
        from vgi_rpc.rpc._type_tokens import schema_tokens

        fields: list[pa.Field[Any]] = [pa.field("a", pa.int64(), nullable=False), pa.field("b", pa.string())]
        schema = pa.schema(fields)
        roundtripped = pa.ipc.read_schema(pa.py_buffer(schema.serialize()))
        assert schema_tokens(schema) == schema_tokens(roundtripped)


class TestIdentityV1IsACrossPortVector:
    """``vgi_rpc.Identity.v1``'s digests, pinned as the ports' shared vector.

    Identity is the first protocol every port implements from a written
    contract rather than by translating the reference line by line, so these
    three digests are what the six ports assert against.  Pinning them here
    keeps the reference honest: if a change to the codec or the type tokens
    moves Identity's shape, this fails in the port that defines the vector
    rather than in six ports that copied it.

    The two single-method digests are not decoration.  A method whose hook the
    deployment did not configure is *not hosted at all*, and the binding's
    method set -- and so its hash -- narrows with it.  A port that instead
    hosted a method that refuses would produce the both-methods digest for a
    server that cannot mint, which is precisely the "routed-and-refusing"
    shape the design rejects.  Only a narrowed digest distinguishes them.
    """

    #: Both methods hosted.
    BOTH = "8317f2ad8e2476bb99e8b94800ab79b19a8cf0c6bdd6d66c2d82bd62ffbe69d5"
    #: Only ``introspect_token`` -- a worker that resolves but does not mint.
    INTROSPECT_ONLY = "27b75bef22e4c70baab92a5188a473506b89055d2cb2b58cc187f6fe7a436385"
    #: Only ``issue_grant`` -- a worker that mints but does not resolve.
    GRANT_ONLY = "c71b12f453310139b6b6a445378064661c52711d03ae1e4fba29b8f7976ef4d8"

    @staticmethod
    def _methods(*only: str) -> dict[str, Any]:
        from vgi_rpc.rpc._token_identity import Identity
        from vgi_rpc.rpc._types import rpc_methods

        methods = rpc_methods(Identity)
        return {k: v for k, v in methods.items() if not only or k in only}

    def test_both_methods(self) -> None:
        """A deployment offering both methods hashes to the shared vector."""
        from vgi_rpc.rpc._token_identity import Identity

        assert compute_protocol_hash(Identity.protocol_name, self._methods()) == self.BOTH

    def test_narrowed_to_introspect_token(self) -> None:
        """A worker that resolves but cannot mint hosts one method, and says so."""
        from vgi_rpc.rpc._token_identity import Identity

        digest = compute_protocol_hash(Identity.protocol_name, self._methods("introspect_token"))
        assert digest == self.INTROSPECT_ONLY
        assert digest != self.BOTH

    def test_narrowed_to_issue_grant(self) -> None:
        """A worker that mints but cannot resolve hosts one method, and says so."""
        from vgi_rpc.rpc._token_identity import Identity

        digest = compute_protocol_hash(Identity.protocol_name, self._methods("issue_grant"))
        assert digest == self.GRANT_ONLY
        assert digest != self.BOTH

    def test_scopes_declares_a_nullable_list_item(self) -> None:
        """Arrow treats child nullability as part of the type, so this is the hash.

        Called out because it is the single most portable mistake here: a port
        that spells the item non-nullable describes a surface the reference
        does not have, and TypeScript shipped exactly that across every list
        type before the canonical preimage could show it.
        """
        from vgi_rpc.rpc._token_identity import Identity

        entry = next(
            m
            for m in protocol_description(Identity.protocol_name, self._methods())["methods"]
            if m["name"] == "issue_grant"
        )
        scopes = next(p for p in entry["params"] if p["name"] == "scopes")
        assert scopes["type"] == "list<item?:utf8>"

    def test_the_preimage_is_diffable_by_a_failing_port(self) -> None:
        """A port whose digest disagrees should be able to diff JSON, not guess."""
        from vgi_rpc.rpc._token_identity import Identity

        blob = canonical_json(protocol_description(Identity.protocol_name, self._methods()))
        assert json.loads(blob)["protocol"] == "vgi_rpc.Identity.v1"
        assert [m["name"] for m in json.loads(blob)["methods"]] == ["introspect_token", "issue_grant"]
