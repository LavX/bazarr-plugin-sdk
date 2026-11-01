"""Test helpers for plugin authors: a scripted host and the conformance suite.

Nothing here needs a running Bazarr host. The host runs in a child process
with the same frame descriptors a real host passes.
"""

from __future__ import annotations

from .conformance import (
    CheckResult,
    ConformanceReport,
    run_conformance,
)
from .fake_host import Arrival, CallOutcome, FakeHost

__all__ = [
    "Arrival",
    "CallOutcome",
    "CheckResult",
    "ConformanceReport",
    "FakeHost",
    "run_conformance",
]
