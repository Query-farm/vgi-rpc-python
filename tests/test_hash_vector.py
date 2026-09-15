# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""The cross-port protocol-hash test vector.

Every port must produce the same ``protocol_hash`` for the conformance
service.  Asserting only the digest gives a failing port one bit of
information: *wrong*.  So the canonical-JSON preimage is checked in beside it.
A port whose digest disagrees diffs its own preimage against this file and
sees which method, which field, or which type token it spells differently --
which is the difference between an afternoon and a week.

Regenerate deliberately, never to make a red test green::

    uv run python tests/test_hash_vector.py --update

The digest changing is a real event: it means the conformance service's wire
surface moved, or the hash definition did.  Either needs the other five ports
updated in the same change.
"""

from __future__ import annotations

import pathlib
import sys

from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.rpc._protocol_hash import canonical_json, compute_protocol_hash, protocol_description
from vgi_rpc.rpc._types import _protocol_wire_name, rpc_methods

_VECTOR = pathlib.Path(__file__).parent / "golden" / "protocol_hash_vector.json"


def _render() -> str:
    """Build the vector: the digest, then the exact bytes it was taken over."""
    name = _protocol_wire_name(ConformanceService)
    methods = rpc_methods(ConformanceService)
    digest = compute_protocol_hash(name, methods)
    preimage = canonical_json(protocol_description(name, methods))
    # The digest first so a reader sees the answer, then the preimage verbatim
    # -- not re-indented, because the bytes are the artifact under test.
    return f"# sha256 = {digest}\n{preimage.decode()}\n"


def test_hash_vector_matches() -> None:
    """The conformance service's hash and preimage are unchanged.

    A diff here is never noise.  It means either the conformance protocol's
    wire surface changed, or the hash definition did, and in both cases the
    other ports need the same change before they can interoperate.
    """
    assert _VECTOR.exists(), f"missing vector; regenerate with: python {__file__} --update"
    assert _render() == _VECTOR.read_text(), (
        "The conformance protocol hash or its preimage changed. If deliberate, "
        f"regenerate with: python {__file__} --update -- and update every other port."
    )


def test_the_recorded_digest_matches_the_recorded_preimage() -> None:
    """The file is self-consistent, so a hand-edit cannot quietly desynchronise it.

    Without this, editing the preimage to match a port's output would make the
    vector agree with a digest it does not actually produce.
    """
    import hashlib

    from vgi_rpc.rpc._protocol_hash import HASH_DOMAIN

    header, _, preimage = _VECTOR.read_text().partition("\n")
    recorded = header.removeprefix("# sha256 = ").strip()
    assert hashlib.sha256(HASH_DOMAIN + preimage.rstrip("\n").encode()).hexdigest() == recorded


def test_the_preimage_carries_no_numbers() -> None:
    """RFC 8785's number rules must stay unreachable for every port."""
    import json

    _, _, preimage = _VECTOR.read_text().partition("\n")

    def walk(value: object) -> None:
        if isinstance(value, bool):
            return
        assert not isinstance(value, (int, float)), f"a number reached the preimage: {value!r}"
        if isinstance(value, dict):
            for v in value.values():
                walk(v)
        elif isinstance(value, list):
            for v in value:
                walk(v)

    walk(json.loads(preimage))


if __name__ == "__main__":
    if "--update" in sys.argv:
        _VECTOR.parent.mkdir(parents=True, exist_ok=True)
        _VECTOR.write_text(_render())
        print(f"wrote {_VECTOR}")
    else:
        print(f"usage: python {__file__} --update")
