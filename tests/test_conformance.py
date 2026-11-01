"""Tests for the conformance suite, run against the sample plugin."""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from sample_plugin import SamplePlugin

from bazarr_plugin_sdk.testing import FakeHost, run_conformance
from bazarr_plugin_sdk.testing.conformance import CheckResult, ConformanceReport

FIXTURES = {
    "valid": {"method": "echo", "params": {"text": "hi"}, "body": b"abc"},
    "cancel": {"method": "slow"},
    "heartbeat": {"method": "blocking_heartbeat"},
    "raise": {"method": "boom"},
    "oversize": {"method": "oversize"},
}


def test_sample_plugin_passes_every_check(tmp_path: Path) -> None:
    report = run_conformance(SamplePlugin, "sample.v1", FIXTURES, root=tmp_path)
    assert report.ok, report.describe()
    assert {result.status for result in report.results} == {"pass"}
    names = {result.name for result in report.results}
    assert "cancel mid-call" in names
    assert "heartbeat cadence" in names
    assert "oversize result refused" in names


def test_sample_plugin_heartbeat_check_passes(tmp_path: Path) -> None:
    report = run_conformance(SamplePlugin, "sample.v1", FIXTURES, root=tmp_path)
    result = named(report, "heartbeat cadence")
    assert result.status == "pass", result.detail


def test_a_report_with_only_skipped_checks_is_not_ok() -> None:
    report = ConformanceReport([CheckResult("cancel mid-call", "skip", "no fixture")])
    assert not report.ok
    assert report.failures == []


def test_a_report_with_only_passes_is_ok() -> None:
    report = ConformanceReport([CheckResult("handshake agreement", "pass")])
    assert report.ok


def test_a_kind_the_plugin_does_not_serve_fails_the_handshake(tmp_path: Path) -> None:
    report = run_conformance(SamplePlugin, "translator.v1", FIXTURES, root=tmp_path)
    assert not report.ok
    handshake = next(r for r in report.results if r.name == "handshake agreement")
    assert handshake.status == "fail"


def test_checks_without_a_fixture_are_skipped(tmp_path: Path) -> None:
    report = run_conformance(
        SamplePlugin,
        "sample.v1",
        {"valid": {"method": "echo", "params": {"text": "hi"}}},
        root=tmp_path,
    )
    assert not report.ok
    assert all(result.status != "fail" for result in report.results), report.describe()
    skipped = {result.name for result in report.results if result.status == "skip"}
    expected_skips = {
        "cancel mid-call",
        "heartbeat cadence",
        "unexpected exception maps to internal",
    }
    assert expected_skips <= skipped
    assert "oversize result refused" in skipped


def test_unknown_fixture_names_are_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown fixture"):
        run_conformance(SamplePlugin, "sample.v1", {"bogus": {"method": "echo"}}, root=tmp_path)


def test_fixtures_need_a_method_name(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="method name"):
        run_conformance(SamplePlugin, "sample.v1", {"valid": {}}, root=tmp_path)


def test_report_describes_each_result(tmp_path: Path) -> None:
    report = run_conformance(
        SamplePlugin,
        "sample.v1",
        {"valid": {"method": "echo", "params": {"text": "hi"}}},
        root=tmp_path,
    )
    text = report.describe()
    assert "handshake agreement" in text
    assert "SKIP" in text
    assert f"{len(report.skipped)} skipped" in text
    assert "not run, so the report is not ok: cancel mid-call" in text
    assert "heartbeat cadence" in text.split("not run, so the report is not ok:")[1]


def named(report: ConformanceReport, name: str) -> CheckResult:
    return next(result for result in report.results if result.name == name)


def test_an_empty_fixture_set_fails(tmp_path: Path) -> None:
    report = run_conformance(SamplePlugin, "sample.v1", {}, root=tmp_path)
    assert not report.ok
    assert named(report, "fixtures given").status == "fail"


def test_skipped_checks_are_never_counted_as_passes(tmp_path: Path) -> None:
    report = run_conformance(
        SamplePlugin,
        "sample.v1",
        {"valid": {"method": "echo", "params": {"text": "hi"}}},
        root=tmp_path,
    )
    passed = {result.name for result in report.passed}
    skipped = {result.name for result in report.skipped}
    assert "cancel mid-call" in skipped
    assert not passed & skipped


def test_a_secret_stored_in_a_checkpoint_fails_the_secrets_check(tmp_path: Path) -> None:
    fixtures = {**FIXTURES, "valid": {"method": "leak_checkpoint"}}
    report = run_conformance(SamplePlugin, "sample.v1", fixtures, root=tmp_path)
    result = named(report, "secrets only in the secrets object")
    assert result.status == "fail"
    assert result.detail == "the worker sent the secret in a body chunk"


def test_cancel_check_requires_the_cancel_frame_to_be_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def record_without_writing(self: FakeHost, op: str, *, abandon: bool = False) -> None:
        self.cancel_times[op] = time.monotonic()

    monkeypatch.setattr(FakeHost, "cancel", record_without_writing)
    fixtures = {**FIXTURES, "cancel": {"method": "self_cancel"}}
    report = run_conformance(SamplePlugin, "sample.v1", fixtures, root=tmp_path)
    result = named(report, "cancel mid-call")
    assert result.status == "fail"
    assert "never written" in result.detail


def test_oversize_check_requires_the_result_limit_refusal(tmp_path: Path) -> None:
    fixtures = {**FIXTURES, "oversize": {"method": "boom"}}
    report = run_conformance(SamplePlugin, "sample.v1", fixtures, root=tmp_path)
    result = named(report, "oversize result refused")
    assert result.status == "fail"
    assert "result limit" in result.detail
