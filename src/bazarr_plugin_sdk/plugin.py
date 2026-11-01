"""Base class for plugin workers."""

from __future__ import annotations

from typing import ClassVar

__all__ = ["Plugin"]


class Plugin:
    """Base class for a plugin worker.

    A subclass sets ``kind_api`` to the interface it implements and lists its
    operation names in ``operations``. Each name is a method on the subclass.
    A method declared with ``async def`` receives a ``PluginContext`` and awaits
    its calls. A method declared with plain ``def`` runs in a worker thread and
    receives a ``BlockingContext``. Either kind receives ``(ctx, request)`` and
    returns a mapping of JSON values, a ``Result`` or None.

    The host creates one instance per worker process. The instance lives as
    long as the worker, so it can hold state that is safe to share between
    operations.
    """

    kind_api: ClassVar[str] = ""
    operations: ClassVar[tuple[str, ...]] = ()
