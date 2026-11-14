import os
import sys

# The tests run against the installed package (``pip install .``, or ``pip install -e .``
# while developing). Only the repository root goes on the path, for ``tests.fixtures``.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
