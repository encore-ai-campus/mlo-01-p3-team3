"""Image integrity and product linkage checks, independent of downloading."""
import hashlib
import io
from pathlib import Path

from PIL import Image

from .core import utc_now
from .storage import atomic_json, file_matches, read_jsonl


def verified_image(data: bytes) -> dict:
    with Image.open(io.BytesIO(data)) as image:
        width, height = image.size
        format_name = image.format
        image.verify()
    extensions = {"JPEG": "jpg", "PNG": "png", "GIF": "gif", "WEBP": "webp", "BMP": "bmp", "TIFF": "tiff"}
    if format_name not in extensions:
        raise ValueError(f"unsupported downloaded image format: {format_name}")
    return {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data), "width": width,
            "height": height, "format": format_name, "extension": extensions[format_name]}


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
        if root is not None:
            local = image.get("local_path")
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
    rows = read_jsonl(products_path)
    errors = [error for row in rows for error in image_errors(row, media)]
    report = {"checked_at": utc_now(), "products": len(rows),
              "image_links": sum(len(row.get("images") or row.get("local_images") or []) for row in rows),
              "errors": errors, "result": "completed" if rows and not errors else "partial"}
    atomic_json(media / "verification_report.json", report)
    return report
