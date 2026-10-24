"""Workflow policy check: fail when a committed workflow carries a credential
or fetches over a transport it does not pin.

The CI workflow must declare no password for the PostgreSQL service, connect
with a DSN that carries none, and publish the database port on the runner's
loopback only. The security workflow must pin every download to https and to
a TLS floor of 1.2, so neither the request nor a redirect it follows can
fall back to a weaker transport.

Every rule is a regular expression with the message that explains it. A
matching forbidden rule is a violation; a required rule that is not found is
one as well."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# (pattern, message): a match in the file is a violation.
CI_FORBIDDEN = [
    (r"^\s*POSTGRES_PASSWORD\s*:", "the PostgreSQL service declares a password"),
    (r"postgres(?:ql)?://[^/\s:@]+:[^/\s@]+@", "a PostgreSQL DSN carries a password"),
    (
        r"postgres(?:ql)?://[^\s'\"]*[?&]password=",
        "a PostgreSQL DSN carries a password in its query",
    ),
    (
        r"^\s*-\s*(?!127\.0\.0\.1:5432:5432\s*$)\S*5432:5432\s*$",
        "the database port is published beyond the loopback",
    ),
]
# (pattern, message): the pattern must be found in the file.
CI_REQUIRED = [
    (
        r"^\s*-\s*127\.0\.0\.1:5432:5432\s*$",
        "the database port is not published on the runner's loopback",
    ),
]
SECURITY_FORBIDDEN = [
    (
        r"^\s*curl\s(?![^\n]*--proto\s+'?=https'?(?:\s|$))",
        "a download does not pin the protocol to https",
    ),
    (
        r"^\s*curl\s(?![^\n]*--tlsv1\.2)",
        "a download does not pin the TLS floor",
    ),
]

FILES = [
    (ROOT / ".github" / "workflows" / "ci.yml", CI_FORBIDDEN, CI_REQUIRED),
    (ROOT / ".github" / "workflows" / "security.yml", SECURITY_FORBIDDEN, []),
]


def violations(text: str, rules: list[tuple[str, str]]) -> list[str]:
    """Return the messages of the rules whose pattern matches the text."""
    return [message for pattern, message in rules if re.search(pattern, text, re.MULTILINE)]


def main() -> int:
    """Fail on any workflow that breaks the two properties."""
    problems = []
    for path, forbidden, required in FILES:
        if not path.is_file():
            print(f"{path} not found - cannot check the workflow policy", file=sys.stderr)
            return 1
        text = path.read_text(encoding="utf-8")
        problems += [f"{path.name}: {m}" for m in violations(text, forbidden)]
        problems += [f"{path.name}: {m}" for p, m in required if not re.search(p, text, re.MULTILINE)]
    if not problems:
        print("workflow policy ok")
        return 0
    print("workflow policy violations:")
    for problem in problems:
        print(f"  - {problem}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())