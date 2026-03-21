# TypeScript: exchange_cast_compatible tests fail on pipe/subprocess

**Repo:** ~/Development/vgi-rpc-typescript
**Severity:** 4 conformance tests fail (846/850 pass)

## Problem

The `TestExchangeCastCompatible` tests fail on pipe and subprocess transports:

```
FAILED test_ts_conformance.py::TestExchangeCastCompatible::test_cast_int64_to_float64[pipe]
FAILED test_ts_conformance.py::TestExchangeCastCompatible::test_cast_int64_to_float64[subprocess]
FAILED test_ts_conformance.py::TestExchangeCastCompatible::test_cast_incompatible_column_name[pipe]
FAILED test_ts_conformance.py::TestExchangeCastCompatible::test_cast_incompatible_column_name[subprocess]
```

## Symptoms

- `test_cast_int64_to_float64`: Exchange receives int64 data when the exchange expects float64 input schema. The Python conformance suite expects this to succeed (cast-compatible schemas should be accepted). TypeScript may not be casting.
- `test_cast_incompatible_column_name`: Exchange receives a batch with a wrong column name. The Python suite expects an `RpcError` to be raised, but TypeScript does not raise one.

## Key Files

- `src/dispatch/stream.ts` — exchange dispatch, likely where input schema validation/casting should happen
- `~/Development/vgi-rpc/vgi_rpc/conformance/_pytest_suite.py` — test definitions (around line 880-910)

## Verification

```bash
cd ~/Development/vgi-rpc-typescript
PYTHON=~/Development/vgi-rpc/.venv/bin/python ~/Development/vgi-rpc/.venv/bin/python -m pytest test_ts_conformance.py -v -k "cast" --timeout=30
```
