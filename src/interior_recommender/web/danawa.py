"""Compatibility entry point; implementation: interior_recommender.catalog.danawa."""
from importlib import import_module
import sys

_implementation = import_module("interior_recommender.catalog.danawa")
sys.modules[__name__] = _implementation
