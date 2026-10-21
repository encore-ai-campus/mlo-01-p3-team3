"""Prepare the supplied crawl's own images, then export completed embeddings.

This module does not call AI APIs. Use embed-directory between prepare and export.
"""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import tempfile

import numpy as np

from ..assets import checksum, png_bytes, read_json, write_bytes, write_json
from ..domain.recommendation import Product, ProductIndex
from .metadata import product_metadata
from ..domain.product_links import comparison_url
from .danawa import DanawaCatalog


SAMPLE = Path(__file__).resolve().parents[1] / "sample"
CRAWL = SAMPLE / "danawa_sample/2ab52dfe-92c3-4d0c-9ec3-879462a6c657"
WORK = SAMPLE / "crawl_test/smoke"


def inside(root, name):
    path = (root / name).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("Image path escapes its source directory")
    return path


def file_hash(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def keyed_rows(path):
    rows = {}
    with Path(path).open(encoding="utf-8-sig") as stream:
        for line, text in enumerate(stream, 1):
            if not text.strip():
                continue
            row = json.loads(text)
            if row.get("source") != "danawa" or not row.get("product_id"):
                raise ValueError(f"Missing product identity at {path}:{line}")
            pid = str(row["product_id"])
            if pid in rows:
                raise ValueError(f"Duplicate product identity at {path}:{line}")
            rows[pid] = (row, line)
    return rows


def prepare(source=CRAWL, work_dir=WORK, *, limit=10, categories=("CHAIR", "SOFA", "TABLE")):
    source, work = Path(source).resolve(), Path(work_dir).resolve()
    if limit is not None and limit < 1:
        raise ValueError("Product limit must be positive")
    if source == work or work in source.parents or source in work.parents:
        raise ValueError("Work directory must be separate from the crawl snapshot")
    paths = ["products.jsonl", "product_raw.jsonl", "media/products_with_local_images.jsonl"]
    hashes = {name: file_hash(source / name) for name in paths}
    options = {"limit": limit, "categories": sorted(set(categories)) if categories is not None else None}
    if work.exists():
        saved = read_json(work / "prepared.json")
        if saved["source_checksums"] != hashes or saved["selection_options"] != options or saved["source"] != str(source):
            raise ValueError("Preparation already exists with different inputs; choose a new --work-dir")
        for image in saved["images"]:
            if file_hash(inside(work, image["input_file"])) != image["sha256"]:
                raise ValueError("Prepared image changed")
        return saved

    products = keyed_rows(source / paths[0])
    # Stream the larger raw file; retain identity/hash only, not full payloads.
    raw_hashes = {}
    with (source / paths[1]).open(encoding="utf-8-sig") as stream:
        for text in stream:
            if text.strip():
                row = json.loads(text)
                pid = str(row["product_id"])
                if row.get("source") != "danawa" or pid in raw_hashes:
                    raise ValueError("Invalid or duplicate raw product identity")
                raw_hashes[pid] = row["payload_hash"]
    media = keyed_rows(source / paths[2])
    if set(products) != set(raw_hashes) or set(products) != set(media):
        raise ValueError("Product, raw and local-image snapshots have different product IDs")

    buckets, missing = defaultdict(list), []
    for pid, (row, line) in products.items():
        image_row, _ = media[pid]
        if row.get("raw_payload_hash") != raw_hashes[pid]:
            raise ValueError("Product raw payload hash mismatch")
        if any(value != image_row.get(key) for key, value in row.items() if key != "images"):
            raise ValueError("Product metadata differs from its local-image snapshot")
        available = []
        for image in image_row.get("images") or []:
            if image.get("status") != "downloaded":
                continue
            if str(image.get("product_id")) != pid or image.get("source") != "danawa":
                raise ValueError("Image and product identity disagree")
            path = inside(source / "media", image["local_path"])
            if file_hash(path) != image["sha256"]:
                raise ValueError("Crawl image checksum mismatch")
            available.append((image, path))
        if not available:
            missing.append(pid)
            continue
        metadata = product_metadata(row, f"{source / paths[0]}:{line};sha256={hashes[paths[0]]}")
        category = metadata["category_code"]
        if categories is not None and category not in categories:
            continue
        image, path = min(available, key=lambda pair: (pair[0].get("role") != "representative", pair[0].get("order", 999), pair[0]["sha256"]))
        digest = image["sha256"]
        metadata["image_id"] = f"danawa:{pid}:{digest}"
        # Validate metadata before publishing any preparation output.
        Product.model_validate({**metadata, "image": str(path), "image_checksum": digest})
        buckets[category or "UNKNOWN"].append({"metadata": metadata, "sha256": digest,
                                                "source_image": str(path), "role": image.get("role")})
    # Round-robin categories so the smoke test is not only the first category.
    selected = []
    while any(buckets.values()) and (limit is None or len(selected) < limit):
        for category in sorted(buckets):
            if buckets[category] and (limit is None or len(selected) < limit):
                selected.append(buckets[category].pop(0))
    if not selected:
        raise ValueError("No products with validated images in the selected categories")
    unique = {row["sha256"]: row["source_image"] for row in selected}
    report = {"test_only": True, "bronze_verified": False, "source_products": len(products),
              "products_without_images": missing, "selected_products": len(selected), "unique_images": len(unique),
              "selected_categories": dict(Counter(r["metadata"]["category_code"] or "UNKNOWN" for r in selected)),
              "ai_api_calls": 0}
    images = [{"sha256": digest, "input_file": f"embedding_inputs/{digest}{Path(path).suffix.lower()}",
               "source_image": path} for digest, path in sorted(unique.items())]
    prepared = {"version": "crawl-test-v1", "source": str(source), "source_checksums": hashes,
                "selection_options": options, "products": selected, "images": images, "report": report}
    work.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".crawl-prepare-", dir=work.parent) as scratch:
        bundle = Path(scratch) / "bundle"
        for image in images:
            data = Path(image["source_image"]).read_bytes()
            if checksum(data) != image["sha256"]:
                raise ValueError("Source image changed during preparation")
            png_bytes(data, resize=True)  # Verify that the embedding preprocessing accepts it.
            write_bytes(bundle / image["input_file"], data)
        write_json(bundle / "prepared.json", prepared)
        write_json(bundle / "preparation_report.json", report)
        bundle.rename(work)
    return prepared


def export(work_dir=WORK, *, with_links=False):
    work = Path(work_dir).resolve()
    prepared_path = work / "prepared.json"
    prepared = read_json(prepared_path)
    source_products = {}
    if with_links:
        source_path = Path(prepared["source"]) / "products.jsonl"
        if file_hash(source_path) != prepared["source_checksums"]["products.jsonl"]:
            raise ValueError("Source products changed after preparation")
        source_products = keyed_rows(source_path)
    index_path = work / "embeddings/index.json"
    if not index_path.is_file():
        raise ValueError("Embeddings are not ready; run embed-directory for embedding_inputs first")
    catalog = DanawaCatalog(index_path)
    expected = {image["sha256"] for image in prepared["images"]}
    if set(catalog.vectors) != {"danawa-" + digest for digest in expected}:
        raise ValueError("Embeddings are incomplete or belong to different prepared images")
    inputs = work / "embedding_inputs"
    if Path(catalog.index["input_dir"]).resolve() != inputs.resolve():
        raise ValueError("Embedding snapshot input directory differs from this preparation")
    for image in prepared["images"]:
        if file_hash(inside(work, image["input_file"])) != image["sha256"]:
            raise ValueError("Prepared image changed after embedding")
    references = {"prepared_checksum": file_hash(prepared_path), "embedding_index_checksum": file_hash(index_path)}
    output = work / ("catalog-with-links" if with_links else "catalog")
    if output.exists():
        data = read_json(output / "products.json")
        if any(data.get(key) != value for key, value in references.items()):
            raise ValueError("Catalog snapshot changed; use a new work directory")
        checked = ProductIndex.load(output / "products.json")
        del checked
        return output / "products.json"
    templates = {t["id"]: t for t in catalog.templates}
    rows, vectors = [], []
    with tempfile.TemporaryDirectory(prefix=".crawl-export-", dir=work) as scratch:
        bundle = Path(scratch) / "bundle"
        for entry in prepared["products"]:
            key = "danawa-" + entry["sha256"]
            name = f"images/{entry['sha256']}.png"
            data = Path(templates[key]["image"]).read_bytes()
            if checksum(data) != catalog.image_checksums[key]:
                raise ValueError("Embedding image changed during export")
            if not (bundle / name).exists():
                write_bytes(bundle / name, data)
            metadata = dict(entry["metadata"])
            if with_links:
                pid = metadata["product_id"].removeprefix("danawa:")
                metadata["comparison_url"] = comparison_url(source_products[pid][0].get("product_url"), metadata["product_id"])
            rows.append({**metadata, "image": name, "image_checksum": checksum(data)})
            vectors.append(catalog.vectors[key])
        np.save(bundle / "vectors.npy", np.asarray(vectors, dtype=np.float32), allow_pickle=False)
        write_json(bundle / "products.json", {**references, "test_only": True, "bronze_verified": False,
                   "embedding_model": catalog.index["model"], "dimension": catalog.index["dimension"],
                   "preprocessing_version": catalog.index["preprocessing_version"], "products": rows,
                   "vectors_file": "vectors.npy", "vectors_checksum": file_hash(bundle / "vectors.npy")})
        checked = ProductIndex.load(bundle / "products.json")
        del checked
        bundle.rename(output)
    return output / "products.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preparation = commands.add_parser("prepare", help="Validate crawl and select representative images; no API calls")
    preparation.add_argument("--source", type=Path, default=CRAWL)
    preparation.add_argument("--work-dir", type=Path, default=WORK)
    size = preparation.add_mutually_exclusive_group()
    size.add_argument("--limit", type=int, default=10)
    size.add_argument("--all", action="store_true")
    preparation.add_argument("--categories", nargs="+", default=["CHAIR", "SOFA", "TABLE"])
    preparation.add_argument("--all-categories", action="store_true")
    conversion = commands.add_parser("export", help="Combine prepared products with completed embeddings; no API calls")
    conversion.add_argument("--work-dir", type=Path, default=WORK)
    conversion.add_argument("--with-links", action="store_true", help="Reuse embeddings and export source-validated URLs to catalog-with-links")
    args = parser.parse_args()
    if args.command == "prepare":
        data = prepare(args.source, args.work_dir, limit=None if args.all else args.limit,
                       categories=None if args.all_categories else args.categories)
        print(json.dumps(data["report"], ensure_ascii=True))
        print(Path(args.work_dir).resolve() / "embedding_inputs")
    else:
        print(export(args.work_dir, with_links=args.with_links))


if __name__ == "__main__":
    main()
