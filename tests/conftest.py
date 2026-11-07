import os
import sys

SRC = os.path.join(os.path.dirname(__file__), "..", "src")
sys.path.insert(0, os.path.abspath(SRC))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
