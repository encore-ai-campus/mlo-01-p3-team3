"""Check saved detail evidence without HTTP or duplicating raw/product snapshots."""
import argparse
import json
from collections import Counter
from pathlib import Path

from .core import utc_now
from .danawa import DanawaAdapter, image_identity
from .reprocess import apply_detail
from .storage import atomic_json, read_jsonl
from .validation import image_errors


def audit_saved_assets(run: Path, output: Path, limit=None, report_path=None) -> dict:
    raw = read_jsonl(run / "product_raw.jsonl")
    if limit is not None and (type(limit) is not int or limit < 1):
        raise ValueError("limit must be positive")
    selected = raw[:limit] if limit is not None else raw
    report = {"checked_at": utc_now(), "network_requests": 0, "input_products": len(raw),
              "checked_products": len(selected), "details_saved": 0, "products_with_gallery": 0,
              "gallery_image_links": 0, "errors": [], "missing_details": []}
    adapter, counts, urls = DanawaAdapter(), Counter(), set()
    for record in selected:
        code = str(record["product_id"])
        path = output / "details" / f"{code}.json"
        if not path.exists():
            report["missing_details"].append(code)
            continue
        report["details_saved"] += 1
        try:
            saved = json.loads(path.read_text(encoding="utf-8"))
            expected = f"https://prod.danawa.com/info/?pcode={code}"
            if saved["product_id"] != code or saved["url"] != expected or record["payload"]["listings"][0]["product_url"] != expected:
                raise ValueError("saved detail/product ID/URL mismatch")
            apply_detail(record, adapter, saved["html"], saved["collected_at"])
            row = adapter.normalize(record)
            images = row.get("images") or []
            report["errors"].extend(image_errors(row))
            if len({image_identity(i["url"]) for i in images}) != len(images):
                raise ValueError("duplicate gallery identity")
            if [i["order"] for i in images] != list(range(len(images))):
                raise ValueError("gallery order mismatch")
            if any(i["selector"] not in {"#basicThumbSlideArea img", "#baseImage"} for i in images):
                raise ValueError("non-gallery image source")
            report["products_with_gallery"] += bool(images)
            report["gallery_image_links"] += len(images)
            counts[len(images)] += 1
            urls.update(i["url"] for i in images)
        except (ValueError, KeyError, TypeError) as exc:
            report["errors"].append({"product_id": code, "reason": str(exc)})
    report.update(unique_gallery_urls=len(urls), gallery_count_distribution=dict(counts),
                  saved_evidence_result="passed" if not report["errors"] else "failed",
                  collection_complete=report["products_with_gallery"] == len(selected))
    path = report_path or output / "audit" / "verification_report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(path, report)
    return report


def main():
    parser = argparse.ArgumentParser(description="저장된 상품 갤러리 검사: 외부 요청 없음")
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    report = audit_saved_assets(args.run, args.output or args.run / "gallery_assets", args.limit)
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["saved_evidence_result"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
