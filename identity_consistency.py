#!/usr/bin/env python3
"""Audit ``vgi_rpc.Identity.v1`` for cross-port consistency.

``describe_diff.py`` compares what the ports put *on the wire* -- and for the
wire shape it is authoritative, because the canonical ``protocol_hash`` covers
every field name, type and nullability flag in the description.

It cannot see any of the things this file checks.  Identity's security
properties live almost entirely in code the hash does not touch: the order the
guards run in, the size of the window a rate limiter admits, whether an
allowlist can be reached by omission, whether a transient failure is
distinguishable from a definitive one.  Six ports can agree on the digest to
the last byte and still disagree about every one of those.

So this is a source-level audit, deliberately: it greps for the constants and
strings that must be identical, and reports a matrix.  A regex over source is a
weak instrument -- it proves a value is *written*, not that it is *used* -- and
it is here because the alternative is nothing.  Treat a PASS as "this port has
not obviously diverged", never as "this port is correct"; the per-port test
suites are what establish behaviour.

Usage:
    python identity_consistency.py            # matrix
    python identity_consistency.py --verbose  # + the matching line per check
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

DEV = Path.home() / "Development"

#: Where each port keeps source.  Identity files are found by content, not by
#: name, because the ports do not agree on naming and should not have to.
PORTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "python": ("vgi-rpc-python", ("vgi_rpc/**/*.py", "tests/**/*.py")),
    "go": ("vgi-rpc-go", ("vgirpc/**/*.go",)),
    "typescript": ("vgi-rpc-typescript", ("src/**/*.ts", "test/**/*.ts", "tests/**/*.ts")),
    "rust": ("vgi-rpc-rust", ("vgi-rpc/**/*.rs", "vgi-rpc-macros/**/*.rs", "tests/**/*.rs")),
    "java": ("vgi-rpc-java", ("vgirpc/src/**/*.java",)),
    "csharp": ("vgi-rpc-csharp", ("src/**/*.cs", "tests/**/*.cs", "test/**/*.cs")),
    "cpp": ("vgi-rpc-c++", ("src/**/*.cpp", "src/**/*.hpp", "include/**/*.hpp", "test/**/*.cpp", "tests/**/*.cpp")),
}

#: (label, regex, why it matters if it differs)
CHECKS: tuple[tuple[str, str, str], ...] = (
    # --- wire shape: proves the port actually verified the digests ---
    ("hash:both", r"8317f2ad8e2476bb99e8b94800ab79b19a8cf0c6bdd6d66c2d82bd62ffbe69d5",
     "the both-methods digest is not asserted anywhere, so the wire shape is unverified"),
    ("hash:introspect-only", r"27b75bef22e4c70baab92a5188a473506b89055d2cb2b58cc187f6fe7a436385",
     "method-level narrowing is not pinned: hosting a refusing method would pass"),
    ("hash:grant-only", r"c71b12f453310139b6b6a445378064661c52711d03ae1e4fba29b8f7976ef4d8",
     "method-level narrowing is not pinned in the other direction"),

    # --- constants: a different value is a different security posture ---
    ("cap value 4096", r"max[_a-z]*token[_a-z]*(chars|len|size|bytes)\D{0,40}4096|4096\D{0,40}token",
     "the cap on a credential we will attempt to resolve differs"),
    ("rate limit 20/s", r"(rate.?limit|per.?window|introspect)\D{0,60}\b20\b",
     "the introspection oracle is bounded differently"),
    ("max_auth_age 900", r"max.?auth.?age\D{0,40}900|900(\.0)?\D{0,40}auth.?age",
     "the ceiling on how stale a login may be and still mint a grant differs"),
    ("grant ttl default 300", r"ttl[_a-z]*(seconds)?\s*[:=]\s*300\b|\b300\b\D{0,30}ttl",
     "the default cache window -- and so the revocation lag -- differs"),
    ("retry_after 5", r"retry.?after\D{0,30}\b5\b",
     "a transient failure does not tell the caller how long to wait"),

    # --- the four divergences the port exercise found, none hash-visible ---
    ("cap named in BYTES", r"MAX_TOKEN_BYTES|MaxTokenBytes|kMaxTokenBytes",
     "the cap's unit is left to the reader; the ports used three different ones"),
    ("trims before shape test", r"trim\w*\s*\(|Trimmed\(|strip\(\)",
     "the shape test runs on the raw credential, so padding smuggles a JWS past it"),
    ("U+0085 pinned by test", r"0085|u\{85\}|x85|\\u0085",
     "the NEL floor is not pinned by a test; the port may cover it today and regress silently"),
    ("U+00A0 pinned by test", r"00A0|00a0|u\{a0\}|xa0|\\u00a0",
     "the NBSP floor is not pinned by a test; the port may cover it today and regress silently"),

    # --- the JWS refusal: routing one onward hands a third party a token ---
    # Matches a regex literal OR a hand-rolled matcher: C++ and Rust hand-roll
    # deliberately, because a backtracking engine on attacker-controlled input
    # is its own hazard.  Keying on the regex spelling marked those correct
    # ports as failures.
    ("JWS matcher present", r"A-Za-z0-9_-|base64url|jws.?shaped|is.?jws",
     "no JWS shape test is discoverable; a JWS-shaped subject may reach the resolver"),

    # --- error taxonomy: the only definitive-vs-transient signal a caller has ---
    ("kind:introspection_refused", r"introspection_refused", "error_kind absent or misspelled"),
    ("kind:token_unresolved", r"token_unresolved", "error_kind absent or misspelled"),
    ("kind:stale_auth", r"stale_auth", "error_kind absent or misspelled"),
    ("kind:grant_refused", r"grant_refused", "error_kind absent or misspelled"),
    ("kind:identity_unavailable", r"identity_unavailable", "error_kind absent or misspelled"),

    # --- guard messages: uniform rejection, and the one actionable exception ---
    ("uniform rejection", r'"unresolved"|\bunresolved\b',
     "rejections may distinguish unknown from expired from malformed"),
    ("auth_time required", r"auth_time",
     "a grant could mint another grant and escape the identity provider"),
    ("allowlist required", r"at least one principal|introspect_principals must",
     "the allowlist may be reachable by omission -- an open oracle"),

    # --- things that must NOT be there ---
    ("token_digest present", r"token.?digest|sha256.*token|TokenDigest",
     "no way to correlate a credential's failures without logging the credential"),
)

#: Checks whose absence is the point -- inverted.
FORBIDDEN: tuple[tuple[str, str, str], ...] = (
    ("no subject param", r"issue_grant\s*\([^)]*\bsubject\b",
     "issue_grant takes a subject: cross-subject minting is open"),
    ("no claims passthrough", r"(class|struct|record|interface)\s+TokenIdentity[\s\S]{0,400}?\bclaims\b\s*[:=<]",
     "TokenIdentity carries claims: the worker can choose its caller's policy branch"),
)


#: Comment markers common across the seven ports.  Prose is stripped before the
#: FORBIDDEN checks run, because a docstring saying "never carries claims" is
#: evidence *for* the invariant and the naive pattern read it as a violation.
_COMMENT = re.compile(r"^\s*(#|//|/\*|\*|--)")


def strip_prose(text: str) -> str:
    """Drop obvious comment lines so absence-checks see code, not commentary."""
    return "\n".join(ln for ln in text.splitlines() if not _COMMENT.match(ln))


def identity_sources(root: Path, globs: tuple[str, ...]) -> list[Path]:
    """Return files that mention Identity, by content rather than by name."""
    hits: list[Path] = []
    for g in globs:
        for p in root.glob(g):
            if not p.is_file():
                continue
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            if "Identity.v1" in text or "introspect_token" in text or "issue_grant" in text:
                hits.append(p)
    return hits


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--verbose", action="store_true", help="show the matching line for each check")
    args = ap.parse_args()

    results: dict[str, dict[str, bool]] = {}
    missing_ports: list[str] = []

    for port, (dirname, globs) in PORTS.items():
        root = DEV / dirname
        if not root.is_dir():
            missing_ports.append(port)
            continue
        files = identity_sources(root, globs)
        if not files:
            results[port] = {}
            continue
        blob = "\n".join(f.read_text(encoding="utf-8", errors="replace") for f in files)
        row: dict[str, bool] = {}
        for label, pattern, _why in CHECKS:
            row[label] = re.search(pattern, blob, re.IGNORECASE) is not None
        code = strip_prose(blob)
        for label, pattern, _why in FORBIDDEN:
            row[label] = re.search(pattern, code, re.IGNORECASE) is None  # absence is PASS
        results[port] = row
        if args.verbose:
            print(f"\n=== {port}: {len(files)} identity file(s)")
            for f in sorted(files):
                print(f"    {f.relative_to(root)}")

    ports = [p for p in PORTS if p in results]
    all_labels = [c[0] for c in CHECKS] + [c[0] for c in FORBIDDEN]
    why = {c[0]: c[2] for c in CHECKS + FORBIDDEN}

    width = max(len(lbl) for lbl in all_labels) + 2
    print()
    print(" " * width + "".join(f"{p[:6]:>8}" for p in ports))
    divergent: list[str] = []
    unimplemented = [p for p in ports if not results[p]]

    for label in all_labels:
        cells = []
        for p in ports:
            if not results[p]:
                cells.append("  --  ")
                continue
            cells.append("  ok  " if results[p][label] else "  XX  ")
        print(f"{label:<{width}}" + "".join(f"{c:>8}" for c in cells))
        vals = {results[p][label] for p in ports if results[p]}
        if len(vals) > 1:
            divergent.append(label)

    print()
    for p in unimplemented:
        print(f"NOT IMPLEMENTED: {p} -- no source mentions vgi_rpc.Identity.v1")
    for p in missing_ports:
        print(f"ABSENT: {p} -- repository not found")

    if divergent:
        print("\nDIVERGENCE across implemented ports:")
        for label in divergent:
            holds = [p for p in ports if results[p] and results[p][label]]
            lacks = [p for p in ports if results[p] and not results[p][label]]
            print(f"  {label}: present in {','.join(holds) or 'none'}; absent in {','.join(lacks)}")
            print(f"      -> {why[label]}")
        return 1

    if unimplemented:
        print("\nRESULT: implemented ports agree; some ports have no implementation yet")
        return 1

    print("\nRESULT: all ports implement vgi_rpc.Identity.v1 and agree on every checked invariant")
    print("(source-level only -- behaviour is established by each port's own suite)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
