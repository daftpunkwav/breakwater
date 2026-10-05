"""Dependency policy check (Go side): fail on any banned module in the module
graph. Runs `go list -m all` (direct and transitive), compares canonical
module paths against scripts/ci/dependency-policy.json. The main module
itself (first line of the listing) is skipped."""

from __future__ import annotations

import json
# The module graph is only reachable through the go toolchain.
import subprocess  # nosec B404
import sys
from pathlib import Path

POLICY = Path(__file__).resolve().parent / "dependency-policy.json"


def main() -> int:
    """Fail on any banned Go module reachable from the main module graph."""
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    banned = policy.get("go", {})
    if not banned:
        print("go dependency policy ok (empty denylist)")
        return 0

    try:
        # A fixed literal argument list: no caller-supplied value reaches the
        # command line. The toolchain is taken from PATH, which is where the
        # CI job installs it (actions/setup-go) - resolving it here would only
        # repeat the same PATH lookup.
        listing = subprocess.run(  # nosec B603 B607
            ["go", "list", "-m", "all"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except FileNotFoundError:
        print("go toolchain not found - cannot scan the module graph", file=sys.stderr)
        return 1

    modules = [line.split()[0] for line in listing.splitlines() if line.strip()]
    # The first entry is the main module; scanning it would let a rename of
    # the repo itself trip an unrelated ban.
    dependencies = modules[1:]

    violations = sorted({m for m in dependencies if m in banned})
    if violations:
        print("banned go modules present:")
        for module in violations:
            print(f"  - {module}: {banned[module]}")
        return 1
    print(f"go dependency policy ok ({len(dependencies)} modules scanned)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
