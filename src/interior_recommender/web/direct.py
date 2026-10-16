"""Compatibility entry point; implementation: interior_recommender.application.direct."""
from importlib import import_module
import sys

_implementation = import_module("interior_recommender.application.direct")
sys.modules[__name__] = _implementation
