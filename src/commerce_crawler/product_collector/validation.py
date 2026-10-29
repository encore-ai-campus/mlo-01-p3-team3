"""Image integrity and product linkage checks, independent of downloading."""
import hashlib
import json
import io
import re
from pathlib import Path

from PIL import Image

from .core import utc_now
from .storage import atomic_json, file_matches


# Same names and spelling as the pipeline's inspect_image; the object key uses the extension.
EXTENSIONS = {"JPEG": "jpg", "PNG": "png", "GIF": "gif", "WEBP": "webp", "BMP": "bmp", "TIFF": "tiff"}
IMAGE_META = ("sha256", "bytes", "width", "height", "format", "extension")
MAX_IMAGE_BYTES = 20 * 1024 * 1024


def verified_image(data: bytes) -> dict:
    with Image.open(io.BytesIO(data)) as image:
        width, height = image.size
        format_name = image.format
        image.verify()
    if format_name not in EXTENSIONS:
        raise ValueError(f"unsupported downloaded image format: {format_name}")
    return {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data), "width": width,
            "height": height, "format": format_name, "extension": EXTENSIONS[format_name]}


def stored_metadata(entry: dict | None) -> dict | None:
    """Metadata of a verified download that can stand for an object already in storage."""
    if not isinstance(entry, dict):
        return None
    meta = {key: entry.get(key) for key in IMAGE_META}
    if (not isinstance(meta["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", meta["sha256"])
            or any(type(meta[key]) is not int or meta[key] <= 0 for key in ("bytes", "width", "height"))
            or meta["bytes"] > MAX_IMAGE_BYTES or not isinstance(meta["format"], str) or meta["format"] not in EXTENSIONS
            or EXTENSIONS[meta["format"]] != meta["extension"]):
        return None
    return meta


def cached_valid(entry: dict, root: Path) -> bool:
    if entry.get("status") != "downloaded" or not entry.get("local_path"):
        return False
    path = (root / entry["local_path"]).resolve()
    return path.is_relative_to(root.resolve()) and file_matches(path, entry.get("sha256"))


def image_errors(product: dict, root: Path | None = None) -> list[dict]:
    errors, seen = [], set()
    # Read existing snapshots too; new output has only images[].
    images = product.get("images") or product.get("local_images") or []
    if not images:
        errors.append({"reason": "gallery_missing"})
    for image in images:
        url = image.get("url")
        if not url or url in seen:
            errors.append({"reason": "missing_or_duplicate_url", "url": url})
        seen.add(url)
        if any(image.get(key, product[key]) != product[key] for key in ("source", "product_id")):
            errors.append({"reason": "product_link_mismatch", "url": url})
        if image.get("status") == "stored":
            # Already in object storage: nothing local; ingestion verifies the object itself.
            if (stored_metadata(image) is None or image.get("local_path") or image.get("category_paths")
                    or image.get("filename")):
                errors.append({"reason": "stored_image_invalid", "url": url})
            continue
        if root is not None:
            local = image.get("local_path")
            if 'category_paths' in image or (local and local.startswith('categories/')):
                paths = image.get('category_paths') or {}
                expected = {c['category_id'] for c in product.get('categories') or []} or {'_uncategorized'}
                if set(paths) != expected or local not in paths.values():
                    errors.append({'reason': 'category_link_mismatch', 'url': url})
                for code, path in paths.items():
                    expected_path = (Path('categories') / code / product['source'] / str(product['product_id']) / (image.get('filename') or '')).as_posix()
                    if path != expected_path or not cached_valid({**image, 'local_path': path}, root):
                        errors.append({'reason': 'category_file_invalid', 'category_id': code, 'url': url})
            if image.get("filename") and not image["filename"].startswith(f"{product['source']}_{product['product_id']}_"):
                errors.append({"reason": "filename_product_mismatch", "url": url})
            if not local or Path(local).name != image.get("filename") or not cached_valid(image, root):
                errors.append({"reason": "file_missing_or_invalid", "url": url})
            else:
                try:
                    actual = verified_image((root / local).read_bytes())
                    if any(actual[key] != image.get(key) for key in actual):
                        errors.append({"reason": "file_metadata_mismatch", "url": url})
                except (ValueError, OSError) as exc:
                    errors.append({"reason": str(exc), "url": url})
    return [{"product_id": product["product_id"], **error} for error in errors]


def verify_assets(products_path: Path, media: Path) -> dict:
    """Checks the manifest one product at a time; only the errors are kept."""
    errors, products, links = [], 0, 0
    with Path(products_path).open(encoding="utf-8-sig") as stream:
        for line in stream:
            if not line.strip():
                continue
            row = json.loads(line)
            products += 1
            links += len(row.get("images") or row.get("local_images") or [])
            errors.extend(image_errors(row, media))
    report = {"checked_at": utc_now(), "products": products, "image_links": links,
              "errors": errors, "result": "completed" if products and not errors else "partial"}
    atomic_json(media / "verification_report.json", report)
    return report
