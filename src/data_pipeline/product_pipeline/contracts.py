"""Versioned identity and wire representations, independent of crawler imports."""
import hashlib
import json
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

from .errors import PipelineError

IMAGE_META = ("sha256", "bytes", "width", "height", "format", "extension")
GALLERY_FIELDS = ("url", "observed_url", "order", "role", "origin", "selector", "resolution_status")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc(value):
    try:
        date = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if date.tzinfo is None:
            raise ValueError()
        return date.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except (AttributeError, TypeError, ValueError) as exc:
        raise PipelineError("INVALID_TIMESTAMP") from exc


def collector_payload_hash(payload):
    """Exact core.payload_hash v0.3 algorithm, including its JSON separators."""
    content = {"listings": [{k: v for k, v in item.items() if k not in {"collected_at", "category"}}
                            for item in payload["listings"]],
               "detail": {k: v for k, v in (payload["detail"] or {}).items() if k != "collected_at"}}
    return hashlib.sha256(json.dumps(content, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()).hexdigest()


def image_projection(image):
    verified = image.get("acquisition_state") == "VERIFIED"
    return {**{k: image.get(k) for k in GALLERY_FIELDS},
            **{k: image.get(k) if verified else None for k in IMAGE_META},
            "acquisition_state": "VERIFIED" if verified else "UNAVAILABLE"}


def observation_key(raw, images):
    return digest({"contract_version": 1, "collection_run_id": raw["job_id"],
                   "source_record_key": f"danawa:PRODUCT:{raw['product_id']}",
                   "source_observation_hash": digest(raw),
                   "gallery_acquisition_hash": digest([image_projection(i) for i in images])})


def registration_identity(row):
    return digest({"raw": row["raw"], "images": [image_projection(i) for i in row["images"]]})


def valid_product_url(url, product_id):
    parsed = urlparse(url)
    return (parsed.scheme == "https" and parsed.hostname == "prod.danawa.com" and not parsed.username
            and not parsed.password and parsed.port in (None, 443) and parsed.path in {"/info/", "/info"}
            and parse_qs(parsed.query).get("pcode") == [str(product_id)])


def envelope(row, raw_id, snapshot_id, bucket):
    raw, images = row["raw"], row["images"]
    detail = raw["payload"].get("detail") or {}
    refs = []
    for image in images:
        slot = "representative" if image["role"] == "representative" else f"gallery:{image['order']}"
        refs.append({**image_projection(image), "source_url": image["url"], "image_slot": slot,
                     "source_version": "source-v1:" + digest({"source_url": image["url"],
                         "observed_url": image.get("observed_url"), "etag": None, "last_modified": None,
                         "raw_document_id": raw_id, "image_slot": slot, "version_rule": 1}),
                     "validator_status": "NOT_RECORDED", "asset_id": None,
                     "bucket": bucket if image.get("object_key") else None, "object_key": image.get("object_key")})
    doc = {"_id": raw_id, "source_code": "danawa", "source_product_id": str(raw["product_id"]),
           "collection_run_id": raw["job_id"], "collected_at": utc(raw["collected_at"]),
           "source_url": detail.get("url") or next((r.get("url") for r in raw["payload"]["listings"] if r.get("url")), None),
           "payload": raw["payload"], "payload_hash": raw["payload_hash"], "raw_object_key": None,
           "response_body_hash": None, "collector_version": raw.get("extractor_version"),
           "parser_version": raw.get("extractor_version"), "schema_version": "danawa-ingest-1",
           "record_type": "PRODUCT", "source_record_key": f"danawa:PRODUCT:{raw['product_id']}",
           "collector_metadata": {k: v for k, v in raw.items() if k != "payload"},
           "lineage": row["lineage"], "source_capture": {"status": "LEGACY_EXTRACTED_ONLY"},
           "hash_contract": {"pipeline": "pipeline-json-v1", "payload": "collector-v0.3-payload_hash"},
           "ingest_contract_version": 1, "observation_key": row["observation_key"],
           "source_observation_hash": digest(raw),
           "gallery_acquisition_hash": digest([image_projection(i) for i in images]),
           "input_snapshot_id": snapshot_id, "image_refs": refs}
    doc["document_hash"] = digest(doc)
    return doc


def event_for(doc, event_id, occurred_at):
    return {"event_id": event_id, "event_type": "product.raw.collected", "schema_version": 1,
            "occurred_at": occurred_at, "correlation_id": doc["collection_run_id"],
            "payload": {"raw_document_id": doc["_id"], **{k: doc[k] for k in (
                "source_code", "source_product_id", "record_type", "source_record_key", "collection_run_id",
                "collected_at", "payload_hash", "schema_version", "observation_key", "document_hash")}}}
