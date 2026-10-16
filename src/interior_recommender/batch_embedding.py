"""Compatibility entry point; implementation: interior_recommender.catalog.batch_embedding."""
from importlib import import_module
import sys

_implementation = import_module("interior_recommender.catalog.batch_embedding")
sys.modules[__name__] = _implementation
