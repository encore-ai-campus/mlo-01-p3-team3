"""Compatibility entry point; implementation: interior_recommender.domain.product_links."""
from importlib import import_module
import sys

_implementation = import_module("interior_recommender.domain.product_links")
sys.modules[__name__] = _implementation
