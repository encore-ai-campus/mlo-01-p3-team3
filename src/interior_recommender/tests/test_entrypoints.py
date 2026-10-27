"""Regressions for public module identities and executable catalog entry points."""
from importlib import import_module
import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize("old,new", [
    ("web.direct", "application.direct"),
    ("recommendation", "domain.recommendation"),
    ("product_links", "domain.product_links"),
    ("batch_embedding", "catalog.batch_embedding"),
    ("catalog_metadata", "catalog.metadata"),
    ("crawl_catalog", "catalog.crawl"),
    ("product_catalog", "catalog.export"),
    ("web.danawa", "catalog.danawa"),
])
def test_public_import_and_patching_reach_same_implementation(old, new, monkeypatch):
    old_module = import_module("interior_recommender." + old)
    new_module = import_module("interior_recommender." + new)
    assert old_module is new_module
    marker = object()
    monkeypatch.setattr(old_module, "_compatibility_probe", marker, raising=False)
    assert new_module._compatibility_probe is marker


@pytest.mark.parametrize("module,argument", [
    ("crawl_catalog", "--work-dir"),
    ("catalog.crawl", "--work-dir"),
    ("product_catalog", "--metadata"),
    ("catalog.export", "--metadata"),
])
def test_catalog_commands_work_from_another_directory(module, argument, tmp_path):
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    source = Path(__file__).resolve().parents[2]
    environment["PYTHONPATH"] = str(source)
    command = [sys.executable, "-m", "interior_recommender." + module]
    if "crawl" in module:
        command.append("export")
    completed = subprocess.run(command + ["--help"], cwd=tmp_path, env=environment,
                               capture_output=True, text=True, timeout=30)
    assert completed.returncode == 0, completed.stderr
    assert argument in completed.stdout


def test_crawl_default_sample_location_is_preserved():
    from interior_recommender.catalog.crawl import SAMPLE
    import interior_recommender

    assert SAMPLE == Path(interior_recommender.__file__).resolve().parent / "sample"
