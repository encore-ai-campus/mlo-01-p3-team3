"""Compatibility entry point; implementation: interior_recommender.domain.recommendation."""
from importlib import import_module
import sys

_implementation = import_module("interior_recommender.domain.recommendation")
sys.modules[__name__] = _implementation
