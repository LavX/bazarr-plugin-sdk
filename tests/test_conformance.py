import pytest

from bazarr_plugin_sdk.testing import ConformanceError, run_conformance
from bazarr_plugin_sdk.translator.v1 import TranslatorPlugin
from tests.fixtures import plugins

KIND = "bazarr.translator.v1"


def test_a_well_behaved_plugin_passes_every_check():
    passed = run_conformance(
        plugins.Spender,
        kind_api=KIND,
        samples={
            "validate": {"args": {"config": {"a": 1}}},
            "translate": {"args": {"request": {}}},
        },
    )
    assert "handshake" in passed and "request translate" in passed
    assert any(item.startswith("cancel") for item in passed)


def test_a_plugin_that_is_meant_to_fail_a_request_can_say_so():
    run_conformance(
        plugins.Echo,
        kind_api=KIND,
        samples={"validate": {"args": {"config": {"bad": 1}}, "expect_error": "invalid_config"}},
    )


def test_a_slow_translation_is_cancelled_cleanly():
    passed = run_conformance(plugins.Interruptible, kind_api=KIND, samples={"translate": {"args": {"request": {}}}})
    assert "cancel" in passed and "request translate" in passed


def test_a_plugin_of_the_wrong_class_fails_the_structure_check():
    with pytest.raises(ConformanceError) as caught:
        run_conformance(plugins.Broken, kind_api=KIND)
    assert caught.value.failures[0].startswith("structure")


def test_an_unexpected_failure_is_reported_with_its_check_name():
    with pytest.raises(ConformanceError) as caught:
        run_conformance(plugins.Echo, kind_api=KIND, samples={"validate": {"args": {"config": {"bad": 1}}}})
    assert any(f.startswith("request validate") for f in caught.value.failures)


def test_a_sample_for_a_method_the_kind_lacks_is_a_failure():
    with pytest.raises(ConformanceError):
        run_conformance(plugins.Echo, kind_api=KIND, samples={"fly": {}})


def test_a_describe_that_is_not_an_object_fails():
    class Bad(TranslatorPlugin):
        def describe(self):
            return [1]

    with pytest.raises(ConformanceError) as caught:
        run_conformance(Bad, kind_api=KIND)
    assert any(f.startswith("describe") for f in caught.value.failures)


def test_an_unserved_kind_is_refused():
    with pytest.raises(ConformanceError):
        run_conformance(plugins.Echo, kind_api="bazarr.nothing.v1")


def test_the_process_check_repeats_the_handshake_in_a_real_worker():
    from pathlib import Path

    root = str(Path(__file__).resolve().parent.parent)
    passed = run_conformance(plugins.Echo, kind_api=KIND, entry="tests.fixtures.plugins:Echo", paths=(root,))
    assert "process" in passed
