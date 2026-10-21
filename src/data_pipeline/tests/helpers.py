import copy
import io
import json
from pathlib import Path

from PIL import Image

from product_pipeline.contracts import collector_payload_hash
from product_pipeline.files import atomic_json
from product_pipeline.snapshot import inspect_image


def png(color="red"):
    result = io.BytesIO()
    Image.new("RGB", (3, 2), color).save(result, format="PNG")
    return result.getvalue()


def write_rows(path, values):
    Path(path).write_text("\n".join(json.dumps(v, ensure_ascii=False) for v in values) + "\n", encoding="utf-8")


def fixture(root, codes=("11", "12"), media=True, unavailable=False, gallery=True):
    export = Path(root) / "export"
    export.mkdir()
    media_root = export / "media"
    media_root.mkdir()
    raw_rows, normal_rows, media_rows = [], [], []
    for code in codes:
        gallery_rows = [{"url": f"https://images.example/{code}/{order}.png", "order": order,
                         "role": "representative" if order == 0 else "gallery",
                         "origin": "detail_gallery", "selector": "#basicThumbSlideArea img"} for order in range(2)] if gallery else []
        categories = [{"category_id": "1523647", "category_name": "의자"}]
        raw = {"source": "danawa", "product_id": code, "job_id": "fixed-collection-run",
               "schema_version": "1.2", "extractor_version": "0.3", "collected_at": "2026-10-15T12:00:00+09:00",
               "categories": categories, "detail_status": "ok", "detail_error": None,
               "payload": {"listings": [{"url": "https://prod.danawa.com/list/?cate=1523647",
                    "product_url": f"https://prod.danawa.com/info/?pcode={code}",
                    "category": categories[0], "collected_at": "2026-10-15T12:00:00+09:00", "price_text": "100원"}],
                   "detail": {"url": f"https://prod.danawa.com/info/?pcode={code}",
                              "collected_at": "2026-10-15T12:01:00+09:00", "gallery_images": gallery_rows, "html": "<html>관측</html>"}}}
        raw["payload_hash"] = collector_payload_hash(raw["payload"])
        normal = {k: copy.deepcopy(raw[k]) for k in ("source", "product_id", "job_id", "schema_version", "collected_at", "categories", "detail_status")}
        normal.update(name="테스트 의자", product_url=raw["payload"]["detail"]["url"],
                      raw_payload_hash=raw["payload_hash"], images=copy.deepcopy(gallery_rows),
                      field_status={"images": "observed" if gallery else "not_found_in_extracted_content"})
        localized = copy.deepcopy(normal)
        localized["image_download_status"] = "partial" if unavailable else "completed"
        for image in localized["images"]:
            image.update(source="danawa", product_id=code)
            if unavailable:
                image.update(status="failed", error="timeout", local_path=None, filename=None, category_paths={})
            else:
                data = png()
                filename = f"danawa_{code}_{image['order']:03d}.png"
                relative = f"categories/1523647/danawa/{code}/{filename}"
                target = media_root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                image.update(inspect_image(data), status="downloaded", filename=filename,
                             local_path=relative, category_paths={"1523647": relative}, downloaded_at="2026-10-15T12:02:00+09:00")
        raw_rows.append(raw)
        normal_rows.append(normal)
        media_rows.append(localized)
    write_rows(export / "product_raw.jsonl", raw_rows)
    write_rows(export / "products.jsonl", normal_rows)
    write_rows(media_root / "products_with_local_images.jsonl", media_rows)
    atomic_json(export / "run_report.json", {"job_id": "fixed-collection-run", "execution_mode": "live", "result": "completed"})
    atomic_json(media_root / "download_report.json", {"finished_at": "2026-10-15T12:03:00+09:00", "result": "partial" if unavailable else "completed"})
    return export, media_root / "products_with_local_images.jsonl" if media else None
