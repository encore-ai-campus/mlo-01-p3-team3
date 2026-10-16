"""Compatibility entry point; implementation: interior_recommender.catalog.metadata."""
from importlib import import_module
import sys

_implementation = import_module("interior_recommender.catalog.metadata")
sys.modules[__name__] = _implementation
