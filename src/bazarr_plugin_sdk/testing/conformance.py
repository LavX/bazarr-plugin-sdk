"""Kind-agnostic conformance checks for plugin workers.

``run_conformance`` starts the plugin in a worker process under ``FakeHost``
and runs the checks that every kind shares:

- handshake agreement, and exit status 78 for a kind the worker does not serve
- hello_ack reports the folders, and TMPDIR points at the temp folder
- secrets appear only in the ``secrets`` object of a call, never in a frame or
  in stderr
- a cancel during a call ends it with the ``cancelled`` code within 5 seconds
- a call that heartbeats keeps its heartbeats within 3 intervals of each other
- an unexpected exception reaches the host as ``internal`` with a message of
  at most 500 characters, and the traceback goes to stderr
- a result larger than the result limit is refused

The checks that need a call take it from a fixture. A fixture is a mapping
with ``method`` (text), and optionally ``params`` (a mapping) and ``body``
(bytes). A check whose fixture is missing is reported as skipped. A skipped
check was not run, so the report is not ok until every check has passed.
"""

from __future__ import annotations

import base64
import json
import secrets
import tempfile
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import pairwise
from pathlib import Path
from typing import Any

from ..plugin import Plugin
from ..runtime import RESULT_LIMIT_MESSAGE
from ..wire import PROTOCOL_VERSION
from .fake_host import CallOutcome, FakeHost

__all__ = [
    "CANCEL_BUDGET_SECONDS",
    "CONFORMANCE_HEARTBEAT_INTERVAL",
    "FIXTURE_KEYS",
    "SECRET_SENTINEL",
    "CheckResult",
    "ConformanceReport",
    "run_conformance",
]

SECRET_SENTINEL = "conformance-" + secrets.token_hex(16)
_SECRET_BYTES = SECRET_SENTINEL.encode()
CONFORMANCE_HEARTBEAT_INTERVAL = 1.0
CANCEL_BUDGET_SECONDS = 5.0
STALL_INTERVALS = 3
CANCEL_DELAY_SECONDS = 0.3
MESSAGE_LIMIT = 500
UNSERVED_KIND = "conformance.unserved.v0"
FIXTURE_KEYS = ("valid", "cancel", "heartbeat", "raise", "oversize")


@dataclass(frozen=True)
class CheckResult:
    """The outcome of one check: ``pass``, ``fail`` or ``skip``."""

    name: str
    status: str
    detail: str = ""


@dataclass
class ConformanceReport:
    """All check results for one plugin.

    ``ok`` is True only when every check passed. A failed check and a skipped
    check both make the report not ok.
    """

    results: list[CheckResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(result.status == "pass" for result in self.results)

    @property
    def failures(self) -> list[CheckResult]:
        return [result for result in self.results if result.status == "fail"]

    @property
    def passed(self) -> list[CheckResult]:
        return [result for result in self.results if result.status == "pass"]

    @property
    def skipped(self) -> list[CheckResult]:
        """Checks that did not run. They are never counted as passes."""
        return [result for result in self.results if result.status == "skip"]

    def describe(self) -> str:
        lines = []
        for result in self.results:
            line = f"{result.status.upper():<5} {result.name}"
            if result.detail:
                line += f": {result.detail}"
            lines.append(line)
        lines.append(
            f"{len(self.passed)} passed, {len(self.failures)} failed, {len(self.skipped)} skipped"
        )
        if self.skipped:
            names = ", ".join(result.name for result in self.skipped)
            lines.append(f"not run, so the report is not ok: {names}")
        return "\n".join(lines)


def _normalise(fixtures: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    cases: dict[str, dict[str, Any]] = {}
    for key, value in fixtures.items():
        if key not in FIXTURE_KEYS:
            raise ValueError(f"unknown fixture {key!r}; expected one of {', '.join(FIXTURE_KEYS)}")
        method = value.get("method")
        if not isinstance(method, str) or not method:
            raise ValueError(f"fixture {key!r} needs a method name")
        params = value.get("params", {})
        if not isinstance(params, Mapping):
            raise ValueError(f"fixture {key!r} params must be a mapping")
        body = value.get("body")
        if body is not None and not isinstance(body, bytes):
            raise ValueError(f"fixture {key!r} body must be bytes")
        cases[key] = {"method": method, "params": dict(params), "body": body}
    return cases


class _Suite:
    def __init__(
        self,
        plugin_class: type[Plugin],
        kind_api: str,
        fixtures: dict[str, dict[str, Any]],
        base: Path,
        timeout: float,
    ) -> None:
        self._plugin_class = plugin_class
        self._kind_api = kind_api
        self._fixtures = fixtures
        self._base = base
        self._timeout = timeout
        self._stderr_texts: list[str] = []

    def _host(self, name: str, kind_api: str | None = None) -> FakeHost:
        return FakeHost(
            self._plugin_class,
            kind_api=kind_api or self._kind_api,
            root=self._base / name,
            heartbeat_interval=CONFORMANCE_HEARTBEAT_INTERVAL,
            timeout=self._timeout,
        )

    @contextmanager
    def _session(self, name: str, kind_api: str | None = None) -> Iterator[FakeHost]:
        host = self._host(name, kind_api)
        try:
            yield host
        finally:
            host.close()
            self._stderr_texts.append(host.stderr_text)

    def _call(
        self,
        host: FakeHost,
        key: str,
        *,
        op: str | None = None,
        secrets: Mapping[str, str] | None = None,
        after_send: Any = None,
        deadline: float | None = None,
    ) -> CallOutcome:
        fixture = self._fixtures[key]
        return host.call(
            fixture["method"],
            fixture["params"],
            body=fixture["body"],
            secrets=secrets,
            op=op or f"conformance-{key}",
            after_send=after_send,
            deadline=deadline,
        )

    def run(self) -> ConformanceReport:
        report = ConformanceReport()
        if self._fixtures:
            report.results.append(
                CheckResult("fixtures given", "pass", f"{len(self._fixtures)} fixture(s)")
            )
        else:
            report.results.append(
                CheckResult(
                    "fixtures given",
                    "fail",
                    "no fixtures were given, so every check that makes a call is skipped",
                )
            )
        checks = [
            ("handshake agreement", self.check_handshake),
            ("handshake reports folders and environment", self.check_folders),
            ("secrets only in the secrets object", self.check_secrets),
            ("cancel mid-call", self.check_cancel),
            ("heartbeat cadence", self.check_heartbeat),
            ("unexpected exception maps to internal", self.check_exception),
            ("oversize result refused", self.check_oversize),
        ]
        for name, check in checks:
            report.results.append(self._run_check(name, check))
        report.results.append(self.check_stderr_secrets())
        return report

    def _run_check(self, name: str, check: Any) -> CheckResult:
        try:
            status, detail = check()
        except Exception as exc:
            return CheckResult(name, "fail", f"{type(exc).__name__}: {exc}")
        return CheckResult(name, status, detail)

    # ----- checks ---------------------------------------------------------

    def check_handshake(self) -> tuple[str, str]:
        problems: list[str] = []
        with self._session("handshake") as host:
            ack = host.start()
        if ack is None:
            problems.append("the worker closed before answering hello")
        else:
            if ack.get("protocol") != PROTOCOL_VERSION:
                problems.append(f"hello_ack protocol is {ack.get('protocol')!r}")
            if self._kind_api not in ack.get("kind_apis", []):
                problems.append(f"hello_ack does not list {self._kind_api!r}")
        if host.exit_code != 0:
            problems.append(f"exit status {host.exit_code} after end of input, expected 0")

        with self._session("handshake-unserved", UNSERVED_KIND) as unserved:
            unserved_ack = unserved.start()
        if unserved_ack is not None:
            problems.append("the worker answered a kind it does not serve")
        if unserved.exit_code != 78:
            problems.append(f"exit status {unserved.exit_code} for an unserved kind, expected 78")
        if problems:
            return "fail", "; ".join(problems)
        return "pass", ""

    def check_folders(self) -> tuple[str, str]:
        with self._session("folders") as host:
            ack = host.start()
        if ack is None:
            return "fail", "the worker closed before answering hello"
        expected = {
            "data_dir": str(host.data_dir),
            "temp_dir": str(host.temp_dir),
            "artifacts_dir": str(host.artifacts_dir),
        }
        if ack.get("folders") != expected:
            return "fail", f"hello_ack folders {ack.get('folders')!r} differ from the hello folders"
        environment = ack.get("environment", {})
        for name in ("TMPDIR", "TMP", "TEMP"):
            if environment.get(name) != str(host.temp_dir):
                return "fail", f"{name} is {environment.get(name)!r}, expected the temp folder"
        return "pass", ""

    def check_secrets(self) -> tuple[str, str]:
        if "valid" not in self._fixtures:
            return "skip", "no 'valid' fixture was given"
        with self._session("secrets") as host:
            host.start()
            outcome = self._call(host, "valid", secrets={"conformance_api_key": SECRET_SENTINEL})
        # The secret must not appear in any frame or body the worker produced.
        call_frame = host.calls[-1]
        without_secrets = {key: value for key, value in call_frame.items() if key != "secrets"}
        if SECRET_SENTINEL in json.dumps(without_secrets):
            return "fail", "the secret appears outside the secrets object of the call"
        for frame in host.received:
            if SECRET_SENTINEL in json.dumps(frame):
                return "fail", f"the worker sent the secret in a {frame.get('t')} frame"
        # Chunk data is base64 on the wire, so the scan works on the decoded bytes.
        # Joining the chunks also catches a secret split across two chunks. Every
        # body the worker sends, a result or a checkpoint, travels as chunks, so
        # this scan covers those bodies too.
        decoded_chunks = b"".join(
            base64.b64decode(str(frame.get("data", "")), validate=True)
            for frame in host.received
            if frame.get("t") == "chunk"
        )
        if _SECRET_BYTES in decoded_chunks:
            return "fail", "the worker sent the secret in a body chunk"
        if outcome.body is not None and _SECRET_BYTES in outcome.body:
            return "fail", "the worker sent the secret in a result body"
        return "pass", ""

    def check_cancel(self) -> tuple[str, str]:
        if "cancel" not in self._fixtures:
            return self._skip_result("cancel")
        op = "conformance-cancel"
        timers: list[threading.Timer] = []

        def arm(host: FakeHost) -> None:
            timer = threading.Timer(CANCEL_DELAY_SECONDS, host.cancel, args=(op,))
            timers.append(timer)
            timer.start()

        # One absolute deadline bounds the whole call, however many frames the
        # worker sends while it winds down.
        deadline = time.monotonic() + CANCEL_DELAY_SECONDS + CANCEL_BUDGET_SECONDS
        with self._session("cancel") as host:
            host.start()
            try:
                outcome = self._call(host, "cancel", op=op, after_send=arm, deadline=deadline)
            except TimeoutError:
                return "fail", "the call did not end before the cancel deadline"
            finally:
                for timer in timers:
                    timer.cancel()
            if "valid" in self._fixtures and outcome.kind == "error":
                follow_up = host.call(
                    self._fixtures["valid"]["method"],
                    self._fixtures["valid"]["params"],
                    body=self._fixtures["valid"]["body"],
                    op="conformance-after-cancel",
                )
            else:
                follow_up = None
        if outcome.kind != "error" or outcome.code != "cancelled":
            return "fail", f"expected cancelled, got {outcome.kind} {outcome.code}"
        sent_at = host.cancel_times.get(op)
        cancel_written = any(
            frame.get("t") == "cancel" and frame.get("op") == op for frame in host.sent
        )
        if sent_at is None or not cancel_written:
            return "fail", "the cancel frame was never written to the worker"
        if outcome.finished_at < sent_at:
            return "fail", "the call ended before the cancel was sent, so it did not answer it"
        elapsed = outcome.finished_at - sent_at
        if elapsed > CANCEL_BUDGET_SECONDS:
            return "fail", f"the call ended {elapsed:.1f} seconds after the cancel"
        if follow_up is not None and follow_up.kind != "result":
            return "fail", f"the worker did not serve a call after the cancel ({follow_up.kind})"
        return "pass", ""

    def check_heartbeat(self) -> tuple[str, str]:
        if "heartbeat" not in self._fixtures:
            return self._skip_result("heartbeat")
        op = "conformance-heartbeat"
        with self._session("heartbeat") as host:
            host.start()
            outcome = self._call(host, "heartbeat", op=op)
        if outcome.kind != "result":
            return "fail", f"expected a result, got {outcome.kind} {outcome.detail}"
        beats = [
            arrival.at
            for arrival in host.arrivals
            if arrival.frame.get("t") == "heartbeat" and arrival.frame.get("op") == op
        ]
        if len(beats) < 2:
            return "fail", f"the worker sent {len(beats)} heartbeats, expected at least two"
        points = [outcome.started_at, *beats, outcome.finished_at]
        longest_silence = max(later - earlier for earlier, later in pairwise(points))
        limit = STALL_INTERVALS * CONFORMANCE_HEARTBEAT_INTERVAL
        if longest_silence >= limit:
            return "fail", f"{longest_silence:.2f} seconds without a heartbeat (limit {limit})"
        return "pass", ""

    def check_exception(self) -> tuple[str, str]:
        if "raise" not in self._fixtures:
            return self._skip_result("raise")
        with self._session("raise") as host:
            host.start()
            outcome = self._call(host, "raise")
        if outcome.kind != "error" or outcome.code != "internal":
            return "fail", f"expected internal, got {outcome.kind} {outcome.code}"
        message = str(outcome.frame.get("message", "")) if outcome.frame else ""
        if len(message) > MESSAGE_LIMIT:
            return "fail", f"the message has {len(message)} characters"
        if "Traceback" in message:
            return "fail", "the traceback appears in the error message"
        if "Traceback" not in host.stderr_text:
            return "fail", "the traceback was not written to stderr"
        return "pass", ""

    def check_oversize(self) -> tuple[str, str]:
        if "oversize" not in self._fixtures:
            return self._skip_result("oversize")
        with self._session("oversize") as host:
            host.start()
            outcome = self._call(host, "oversize")
        if outcome.kind == "result":
            return "fail", "the oversize result was delivered"
        if outcome.kind == "breach":
            return "fail", f"the worker broke the framing: {outcome.detail}"
        if outcome.kind != "error" or outcome.code != "internal":
            return "fail", f"expected internal, got {outcome.kind} {outcome.code}"
        message = str(outcome.frame.get("message", "")) if outcome.frame else ""
        if message != RESULT_LIMIT_MESSAGE:
            return "fail", f"expected the result limit refusal, got {message!r}"
        return "pass", ""

    def check_stderr_secrets(self) -> CheckResult:
        for text in self._stderr_texts:
            if SECRET_SENTINEL in text:
                return CheckResult("no secret in stderr", "fail", "a secret reached stderr")
        if "valid" not in self._fixtures:
            return CheckResult("no secret in stderr", "skip", "no 'valid' fixture was given")
        return CheckResult("no secret in stderr", "pass", "")

    def _skip_result(self, key: str) -> tuple[str, str]:
        return "skip", f"no {key!r} fixture was given"


def run_conformance(
    plugin_class: type[Plugin],
    kind_api: str,
    fixtures: Mapping[str, Mapping[str, Any]] | None = None,
    *,
    root: str | Path | None = None,
    timeout: float = 10.0,
) -> ConformanceReport:
    """Run the kind-agnostic checks against ``plugin_class`` and return the report.

    ``kind_api`` is the interface the plugin claims to serve. ``fixtures`` maps
    check names (``valid``, ``cancel``, ``heartbeat``, ``raise``, ``oversize``)
    to calls the plugin understands. ``root`` is a scratch folder; a temporary
    one is used when it is omitted.

    The report is ok only when every check passed. A check without its fixture
    is skipped, and a skipped check makes the report not ok. Give all five
    fixtures to get an ok report.

    Use it from a test::

        report = run_conformance(MyPlugin, "example.kind.v1", FIXTURES)
        assert report.ok, report.describe()
    """
    cases = _normalise(fixtures or {})
    if root is not None:
        return _Suite(plugin_class, kind_api, cases, Path(root), timeout).run()
    with tempfile.TemporaryDirectory(prefix="plugin-conformance-") as scratch:
        return _Suite(plugin_class, kind_api, cases, Path(scratch), timeout).run()
