"""Workflow policy check: fail when a committed workflow carries a credential
or fetches over a transport it does not pin.

The workflows are read as YAML, so the check sees the effective values: the
environment of the PostgreSQL service, the port mapping it publishes, the DSN
the test step connects with and the commands the security workflow runs. A
credential in the service, a port published beyond the runner's loopback, a
test DSN that is not the expected one and a release download that does not
open with the pinned options are all reported. PyYAML comes with the runner
image."""

from __future__ import annotations

import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # the runner image provides PyYAML
    print("PyYAML is required to read the workflows", file=sys.stderr)
    raise SystemExit(1) from None

ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
SECURITY_WORKFLOW = ROOT / ".github" / "workflows" / "security.yml"

LOOPBACK_PORT = "127.0.0.1:5432:5432"
TEST_DSN = "postgres://breakwater@127.0.0.1:5432/breakwater?sslmode=disable"
# The artifacts the security workflow fetches.
ARTIFACTS = ("gitleaks_${V}_linux_x64.tar.gz", "gitleaks_${V}_checksums.txt")
# A download must open with the pinned options, and a transfer chain is
# refused: curl resets the local options for the transfer that follows it.
CURL = re.compile(r"(?:^|[;|&\n])\s*curl\s(?!-sSfL --proto '=https' --tlsv1\.2 )", re.MULTILINE)
NEXT = re.compile(r"--next\b")


def service_problems(service: dict) -> list[str]:
    """Report the credential and the exposure problems of the service."""
    problems = []
    for key, value in service.get("env", {}).items():
        if "password" in str(key).lower() or "password" in str(value).lower():
            problems.append(f"the PostgreSQL service declares {key}")
    ports = [str(mapping) for mapping in service.get("ports", [])]
    if ports != [LOOPBACK_PORT]:
        problems.append(f"the database port is published beyond the loopback: {ports}")
    return problems


def dsn_problems(workflow: dict) -> list[str]:
    """Report a test DSN that is not the expected one."""
    problems = []
    verify = workflow.get("jobs", {}).get("verify", {})
    for step in verify.get("steps", []):
        dsn = step.get("env", {}).get("BREAKWATER_TEST_POSTGRES_DSN")
        if dsn is not None and str(dsn) != TEST_DSN:
            problems.append(f"the test DSN is not the expected one: {dsn}")
    return problems


def download_problems(script: str) -> list[str]:
    """Report the downloads of one script that do not pin their transport."""
    problems = []
    for match in CURL.finditer(script):
        problems.append(f"a download does not open with the pinned options: {match.group(0).strip()}")
    if NEXT.search(script):
        problems.append("a download chains another transfer with --next")
    return problems


def artifact_problems(scripts: list[str]) -> list[str]:
    """Report an artifact the security workflow does not fetch."""
    fetched = "\n".join(scripts)
    return [f"the security workflow does not fetch {a}" for a in ARTIFACTS if a not in fetched]


def ci_problems(workflow: dict) -> list[str]:
    """Report the credential and the exposure problems of the CI workflow."""
    verify = workflow.get("jobs", {}).get("verify", {})
    service = verify.get("services", {}).get("postgres", {})
    return service_problems(service) + dsn_problems(workflow)


def scripts_of(workflow: dict) -> list[str]:
    """Return the shell commands the workflow runs."""
    return [
        str(step["run"])
        for job in workflow.get("jobs", {}).values()
        for step in job.get("steps", [])
        if "run" in step
    ]


def security_problems(workflow: dict) -> list[str]:
    """Report the release downloads that do not pin their transport."""
    scripts = scripts_of(workflow)
    problems = []
    for script in scripts:
        problems += download_problems(script)
    return problems + artifact_problems(scripts)


def workflow_problems(path: Path, check) -> list[str]:
    """Report every problem of one workflow file."""
    if not path.is_file():
        print(f"{path} not found - cannot check the workflow policy", file=sys.stderr)
        raise SystemExit(1)
    return check(yaml.safe_load(path.read_text(encoding="utf-8")))


def main() -> int:
    """Fail on any workflow that breaks the two properties."""
    problems = []
    for path, check in ((CI_WORKFLOW, ci_problems), (SECURITY_WORKFLOW, security_problems)):
        problems += workflow_problems(path, check)
    if not problems:
        print("workflow policy ok")
        return 0
    print("workflow policy violations:")
    for problem in problems:
        print(f"  - {problem}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())