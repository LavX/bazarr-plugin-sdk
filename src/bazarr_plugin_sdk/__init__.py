"""Plugin SDK for Bazarr plugin workers.

The package gives a plugin author a context for progress, heartbeats, cost
control and folders, a shared error vocabulary, the worker runtime that speaks
the framed protocol, and a conformance suite that runs against a scripted host.
"""

from __future__ import annotations

from ._version import __version__
from .context import BlockingContext, Folders, PluginContext, Request, Result
from .errors import HostGone, OperationAbandoned, PluginError
from .plugin import Plugin
from .runtime import serve

__all__ = [
    "BlockingContext",
    "Folders",
    "HostGone",
    "OperationAbandoned",
    "Plugin",
    "PluginContext",
    "PluginError",
    "Request",
    "Result",
    "__version__",
    "serve",
]
