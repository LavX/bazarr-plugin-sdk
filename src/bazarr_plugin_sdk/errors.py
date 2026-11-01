"""Error vocabulary shared by plugin code and the host.

A plugin reports a failure by raising ``PluginError`` with one of the nine
codes. Any other exception reaches the host as ``internal``. The host shows
the code to the user and decides what to do next, so a code should describe
the cause, not the plugin's internals.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

__all__ = [
    "AUTH_REQUIRED",
    "CANCELLED",
    "CODES",
    "DECLINED",
    "DEPENDENCY_UNAVAILABLE",
    "INTERNAL",
    "INVALID_CONFIG",
    "MESSAGE_LIMIT",
    "PARTIAL",
    "QUOTA_EXHAUSTED",
    "RATE_LIMITED",
    "HostGone",
    "OperationAbandoned",
    "PluginError",
    "internal_error",
    "truncate_message",
]

AUTH_REQUIRED = "auth_required"
INVALID_CONFIG = "invalid_config"
RATE_LIMITED = "rate_limited"
QUOTA_EXHAUSTED = "quota_exhausted"
DEPENDENCY_UNAVAILABLE = "dependency_unavailable"
DECLINED = "declined"
CANCELLED = "cancelled"
PARTIAL = "partial"
INTERNAL = "internal"

CODES: frozenset[str] = frozenset(
    {
        AUTH_REQUIRED,
        INVALID_CONFIG,
        RATE_LIMITED,
        QUOTA_EXHAUSTED,
        DEPENDENCY_UNAVAILABLE,
        DECLINED,
        CANCELLED,
        PARTIAL,
        INTERNAL,
    }
)

MESSAGE_LIMIT = 500


def truncate_message(text: str, limit: int = MESSAGE_LIMIT) -> str:
    """Return ``text`` cut to ``limit`` characters."""
    return text if len(text) <= limit else text[:limit]


class PluginError(Exception):
    """A failure the plugin reports to the host with a code from ``CODES``.

    Optional fields belong to specific codes:

    - ``field_errors`` (a mapping from field name to message) with ``invalid_config``
    - ``retry_after`` (seconds, zero or more) with ``rate_limited``
    - ``reason`` (a short token) with ``declined``

    Passing an optional field with another code raises ``ValueError``. The
    message is cut to 500 characters when it is sent to the host.
    """

    def __init__(
        self,
        code: str,
        message: str = "",
        *,
        field_errors: Mapping[str, str] | None = None,
        retry_after: float | None = None,
        reason: str | None = None,
    ) -> None:
        if code not in CODES:
            raise ValueError(f"unknown error code {code!r}")
        if field_errors is not None and code != INVALID_CONFIG:
            raise ValueError("field_errors is only valid with the invalid_config code")
        if retry_after is not None and code != RATE_LIMITED:
            raise ValueError("retry_after is only valid with the rate_limited code")
        if reason is not None and code != DECLINED:
            raise ValueError("reason is only valid with the declined code")

        self.code = code
        self.message = str(message)
        self.field_errors: dict[str, str] | None = None
        self.retry_after: float | None = None
        self.reason: str | None = None

        if field_errors is not None:
            if not all(isinstance(k, str) and isinstance(v, str) for k, v in field_errors.items()):
                raise ValueError("field_errors must map field names to messages")
            self.field_errors = dict(field_errors)
        if retry_after is not None:
            if isinstance(retry_after, bool) or not isinstance(retry_after, (int, float)):
                raise ValueError("retry_after must be a number of seconds")
            if not math.isfinite(retry_after) or retry_after < 0:
                raise ValueError("retry_after must be zero or more seconds")
            self.retry_after = float(retry_after)
        if reason is not None:
            if not isinstance(reason, str):
                raise ValueError("reason must be a string")
            self.reason = reason

        super().__init__(self.message)

    def wire_fields(self) -> dict[str, Any]:
        """Return the optional fields that are set, for the error frame."""
        fields: dict[str, Any] = {}
        if self.field_errors is not None:
            fields["field_errors"] = dict(self.field_errors)
        if self.retry_after is not None:
            fields["retry_after"] = self.retry_after
        if self.reason is not None:
            fields["reason"] = self.reason
        return fields


class HostGone(Exception):
    """The host connection is closed, or a frame could not be written to it.

    The operation ends. Plugin code should not retry after this error.
    """


class OperationAbandoned(Exception):
    """The host gave up on this operation and discards whatever it returns.

    Plugin code should stop and let the error propagate. Retrying does not help.
    """


def internal_error(exc: BaseException) -> PluginError:
    """Map an exception that is not a ``PluginError`` to an ``internal`` error.

    The message is the exception's class name, then a colon and its text when
    there is any, cut to 500 characters. The traceback is not included.
    """
    name = type(exc).__name__
    text = str(exc)
    message = f"{name}: {text}" if text else name
    return PluginError(INTERNAL, truncate_message(message))
