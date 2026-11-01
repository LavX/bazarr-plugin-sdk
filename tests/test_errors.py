"""Tests for the error vocabulary and the exception mapping."""

from __future__ import annotations

import pytest

from bazarr_plugin_sdk import errors
from bazarr_plugin_sdk.errors import (
    CODES,
    INTERNAL,
    MESSAGE_LIMIT,
    PluginError,
    internal_error,
    truncate_message,
)


def test_there_are_exactly_the_nine_codes_with_their_wire_names() -> None:
    assert CODES == frozenset(
        {
            "auth_required",
            "invalid_config",
            "rate_limited",
            "quota_exhausted",
            "dependency_unavailable",
            "declined",
            "cancelled",
            "partial",
            "internal",
        }
    )
    assert len(CODES) == 9


def test_plain_error_carries_code_and_message_without_optional_fields() -> None:
    error = PluginError(errors.AUTH_REQUIRED, "sign in first")
    assert error.code == "auth_required"
    assert error.message == "sign in first"
    assert str(error) == "sign in first"
    assert error.field_errors is None
    assert error.retry_after is None
    assert error.reason is None
    assert error.wire_fields() == {}


def test_unknown_code_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown error code"):
        PluginError("oops", "text")


def test_invalid_config_carries_field_errors() -> None:
    error = PluginError(
        errors.INVALID_CONFIG,
        "bad settings",
        field_errors={"api_key": "required"},
    )
    assert error.field_errors == {"api_key": "required"}
    assert error.wire_fields() == {"field_errors": {"api_key": "required"}}


def test_rate_limited_carries_retry_after() -> None:
    error = PluginError(errors.RATE_LIMITED, "slow down", retry_after=30)
    assert error.retry_after == 30.0
    assert isinstance(error.retry_after, float)
    assert error.wire_fields() == {"retry_after": 30.0}


def test_declined_carries_reason() -> None:
    error = PluginError(errors.DECLINED, "not supported", reason="unsupported_language")
    assert error.reason == "unsupported_language"
    assert error.wire_fields() == {"reason": "unsupported_language"}


@pytest.mark.parametrize(
    ("code", "kwargs"),
    [
        (errors.INTERNAL, {"field_errors": {"a": "b"}}),
        (errors.DECLINED, {"retry_after": 3}),
        (errors.RATE_LIMITED, {"reason": "x"}),
        (errors.CANCELLED, {"retry_after": 1}),
    ],
)
def test_optional_fields_are_refused_for_other_codes(code: str, kwargs: dict) -> None:
    with pytest.raises(ValueError):
        PluginError(code, "text", **kwargs)


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), True, "3"])
def test_retry_after_must_be_a_finite_non_negative_number(value: object) -> None:
    with pytest.raises(ValueError):
        PluginError(errors.RATE_LIMITED, "x", retry_after=value)  # type: ignore[arg-type]


def test_field_errors_must_map_text_to_text() -> None:
    with pytest.raises(ValueError):
        PluginError(errors.INVALID_CONFIG, "x", field_errors={"a": 1})  # type: ignore[dict-item]


def test_declined_reason_must_be_text() -> None:
    with pytest.raises(ValueError):
        PluginError(errors.DECLINED, "x", reason=5)  # type: ignore[arg-type]


def test_host_gone_and_operation_abandoned_are_distinct_exceptions() -> None:
    assert issubclass(errors.HostGone, Exception)
    assert issubclass(errors.OperationAbandoned, Exception)
    assert not issubclass(errors.HostGone, errors.OperationAbandoned)
    assert not issubclass(errors.OperationAbandoned, errors.HostGone)


def test_internal_error_uses_class_name_and_text() -> None:
    error = internal_error(ValueError("bad input"))
    assert error.code == INTERNAL
    assert error.message == "ValueError: bad input"


def test_internal_error_without_text_uses_the_class_name_alone() -> None:
    error = internal_error(KeyError())
    assert error.message == "KeyError"


def test_internal_error_message_is_cut_to_500_characters() -> None:
    error = internal_error(RuntimeError("x" * 2000))
    assert len(error.message) == MESSAGE_LIMIT
    assert error.message.startswith("RuntimeError: x")


def test_internal_error_never_carries_a_traceback() -> None:
    try:
        raise OSError("disk unreadable")
    except OSError as exc:
        error = internal_error(exc)
    assert "Traceback" not in error.message
    assert error.message == "OSError: disk unreadable"


def test_truncate_message_leaves_short_text_alone() -> None:
    assert truncate_message("short") == "short"
    assert truncate_message("abcdef", limit=3) == "abc"
