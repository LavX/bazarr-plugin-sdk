import pytest

from bazarr_plugin_sdk._protocol.worker_channel import OperationError
from bazarr_plugin_sdk.errors import ERROR_CODES, PluginError


def test_the_vocabulary_matches_the_contract():
    assert ERROR_CODES == {
        "auth_required", "invalid_config", "rate_limited", "quota_exhausted", "dependency_unavailable",
        "declined", "cancelled", "partial", "internal", "sandbox_blocked",
    }


def test_a_plugin_error_is_an_operation_error_of_the_wire_protocol():
    error = PluginError("rate_limited", "slow", retry_after=3)
    assert isinstance(error, OperationError)
    assert (error.code, error.message, error.retryable, error.retry_after) == ("rate_limited", "slow", True, 3.0)
    assert PluginError("invalid_config", "x").retryable is False


def test_an_unknown_code_is_refused_at_construction():
    with pytest.raises(ValueError):
        PluginError("whoops", "x")


def test_the_wire_form_round_trips():
    error = PluginError("invalid_config", "bad", field_errors={"a": "b"}, reason="r")
    again = PluginError.from_dict(error.to_dict())
    assert (again.code, again.message, again.field_errors, again.reason) == ("invalid_config", "bad", {"a": "b"}, "r")


def test_the_wire_form_is_the_protocols_error_object_plus_the_extras():
    data = PluginError("rate_limited", "x", retry_after=3, reason="r").to_dict()
    assert data == {"code": "rate_limited", "message": "x", "retryable": True, "retry_after": 3.0, "reason": "r"}
    assert PluginError("internal", "x").to_dict() == {"code": "internal", "message": "x", "retryable": False}


@pytest.mark.parametrize("value", [None, -1, 0, float("nan"), float("inf"), "soon", True])
def test_a_retry_after_that_is_not_a_positive_finite_number_is_dropped(value):
    assert PluginError("rate_limited", "x", retry_after=value).retry_after is None


def test_retry_after_is_capped_at_a_day():
    assert PluginError("rate_limited", "x", retry_after=10**9).retry_after == 86400.0


def test_a_code_outside_the_vocabulary_on_the_wire_becomes_internal_and_keeps_the_original_as_reason():
    error = PluginError.from_dict({"code": "unknown_op", "message": "m"})
    assert error.code == "internal" and error.reason == "unknown_op"


def test_the_traceback_is_kept_and_cut_to_its_tail():
    error = PluginError("internal", "x")
    error.detail = "a" * 20000 + "END"
    assert len(error.to_dict()["traceback"]) == 8000
    assert PluginError.from_dict(error.to_dict()).detail.endswith("END")
