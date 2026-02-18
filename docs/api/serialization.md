# Serialization

Arrow serialization layer for automatic schema generation and IPC validation.

## ArrowSerializableDataclass

Use `ArrowSerializableDataclass` as a mixin for dataclasses that need Arrow serialization. The Arrow schema is generated automatically from field annotations:

```python
from dataclasses import dataclass
from typing import Annotated

import pyarrow as pa

from vgi_rpc import ArrowSerializableDataclass, ArrowType


@dataclass(frozen=True)
class Measurement(ArrowSerializableDataclass):
    timestamp: str
    value: float
    count: Annotated[int, ArrowType(pa.int32())]  # explicit Arrow type override

# Auto-generated schema
print(Measurement.ARROW_SCHEMA)
# timestamp: string
# value: double
# count: int32

# Serialize / deserialize
m = Measurement(timestamp="2024-01-01T00:00:00Z", value=42.0, count=7)
data = m.serialize_to_bytes()
m2 = Measurement.deserialize_from_bytes(data)
assert m == m2
```

These dataclasses work directly as RPC parameters and return types. They're also the base class for [`StreamState`](streaming.md) — any field you add to a stream state is automatically serialized between calls.

### Type mappings

| Python type | Arrow type |
|---|---|
| `str` | `utf8` |
| `bytes` | `binary` |
| `int` | `int64` |
| `float` | `float64` |
| `bool` | `bool_` |
| `list[T]` | `list_<T>` |
| `dict[K, V]` | `map_<K, V>` |
| `frozenset[T]` | `list_<T>` |
| `Enum` | `dictionary(int32, utf8)` |
| `Optional[T]` | nullable `T` |
| nested `ArrowSerializableDataclass` | `struct` |
| `Annotated[T, ArrowType(...)]` | explicit override |

## API Reference

### ArrowSerializableDataclass

::: vgi_rpc.utils.ArrowSerializableDataclass

### ArrowType

::: vgi_rpc.utils.ArrowType

### IpcValidation

::: vgi_rpc.utils.IpcValidation

### ValidatedReader

::: vgi_rpc.utils.ValidatedReader

### Validation

::: vgi_rpc.utils.validate_batch

### Errors

::: vgi_rpc.utils.IPCError
