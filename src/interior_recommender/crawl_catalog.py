"""Compatibility entry point; implementation: interior_recommender.catalog.crawl."""
from importlib import import_module
import sys

_implementation = import_module("interior_recommender.catalog.crawl")

if __name__ == "__main__":
    _implementation.main()
else:
    sys.modules[__name__] = _implementation
