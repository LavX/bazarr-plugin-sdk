"""Entry point that runs a plugin class as a worker process.

The test host starts it as ``python -m bazarr_plugin_sdk.testing.worker_child
module:qualname``. It is not part of a plugin's own entry point.
"""

from __future__ import annotations

import importlib
import sys

from ..plugin import Plugin
from ..runtime import serve

__all__ = ["load_plugin", "main"]


def load_plugin(spec: str) -> type[Plugin]:
    """Import ``module:qualname`` and return the class it names."""
    module_name, separator, qualname = spec.partition(":")
    if not separator or not module_name or not qualname:
        raise SystemExit(f"plugin spec must look like module:qualname, got {spec!r}")
    target: object = importlib.import_module(module_name)
    for part in qualname.split("."):
        target = getattr(target, part)
    if not isinstance(target, type):
        raise SystemExit(f"{spec!r} does not name a class")
    return target  # type: ignore[return-value]


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        raise SystemExit("usage: worker_child module:qualname")
    return serve(load_plugin(argv[1]))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
