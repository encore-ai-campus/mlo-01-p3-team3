"""One-time export of saved Danawa vectors plus sourced product metadata; no API calls."""
import argparse
from pathlib import Path

import numpy as np

from ..assets import checksum, read_json, write_bytes, write_json
from ..domain.recommendation import Product, ProductIndex
from .danawa import DanawaCatalog


def export_catalog(index_path, metadata_path, output):
    catalog = DanawaCatalog(index_path)
    metadata = read_json(metadata_path)["products"] if metadata_path else []
    mapping = {row["image_id"]: row for row in metadata}
    if len(mapping) != len(metadata) or set(mapping) - set(catalog.vectors):
        raise ValueError("Duplicate or unknown metadata image_id")
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Use a new catalog directory; existing snapshots are immutable")
    # Validate all metadata before creating an export directory.
    rows = []
    for n, template in enumerate(catalog.templates):
        image_id = template["id"]
        data = Path(template["image"]).read_bytes()
        if checksum(data) != catalog.image_checksums[image_id]:
            raise ValueError("Source image changed")
        values = {"image_id": image_id, "product_id": image_id, "name": "상품 연결 미확인 이미지", **mapping.get(image_id, {}),
                  "image": f"images/{n}.png", "image_checksum": checksum(data)}
        rows.append(Product.model_validate(values).model_dump())
    matrix = np.asarray([catalog.vectors[t["id"]] for t in catalog.templates], dtype=np.float32)
    ProductIndex(rows, matrix, model=catalog.index["model"], preprocessing=catalog.index["preprocessing_version"], version="export-validation")
    output.mkdir(parents=True)
    for row, template in zip(rows, catalog.templates):
        data = Path(template["image"]).read_bytes()
        if checksum(data) != row["image_checksum"]:
            raise ValueError("Source image changed during export")
        write_bytes(output / row["image"], data)
    np.save(output / "vectors.npy", matrix, allow_pickle=False)
    import hashlib
    with (output / "vectors.npy").open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    write_json(output / "products.json", {"embedding_model": catalog.index["model"], "dimension": catalog.index["dimension"],
               "preprocessing_version": catalog.index["preprocessing_version"], "products": rows,
               "vectors_file": "vectors.npy", "vectors_checksum": digest})
    return output / "products.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--danawa-index", required=True, type=Path)
    parser.add_argument("--metadata", type=Path, help="Sourced product rows keyed by danawa image_id; missing rows stay unverified")
    parser.add_argument("--output", required=True, type=Path, help="New immutable output directory")
    args = parser.parse_args()
    print(export_catalog(args.danawa_index, args.metadata, args.output))


if __name__ == "__main__":
    main()
