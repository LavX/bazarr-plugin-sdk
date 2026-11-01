"""Tests for the conformance suite, run against the sample plugin."""

from __future__ import annotations

from pathlib import Path

import pytest
from sample_plugin import SamplePlugin

from bazarr_plugin_sdk.testing import run_conformance

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
    assert report.ok, report.describe()
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
