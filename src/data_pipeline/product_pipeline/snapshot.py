"""Seal FULL_EXPORT with external partition/sort joins and immutable image copies."""
import hashlib
import heapq
import io
from contextlib import ExitStack, closing
from itertools import zip_longest
from pathlib import Path
import re
import shutil
import tempfile

from .contracts import (GALLERY_FIELDS, IMAGE_META, canonical, collector_payload_hash,
                        digest, now, observation_key, utc, valid_product_url)
from .errors import PipelineError
from .files import atomic_bytes, atomic_json, confined, lock, read_json, rows, sha_file, sync_dir


def product_key(row):
    source, code = row.get("source"), row.get("product_id")
    if source != "danawa" or not isinstance(code, str) or not re.fullmatch(r"[0-9]+", code):
        raise PipelineError("INVALID_PRODUCT_ID")
    return source, code


def inspect_image(data):
    from PIL import Image, UnidentifiedImageError
    try:
        with Image.open(io.BytesIO(data)) as image:
            width, height, fmt = *image.size, image.format
            image.verify()
        extensions = {"JPEG": "jpg", "PNG": "png", "WEBP": "webp", "GIF": "gif", "BMP": "bmp", "TIFF": "tiff"}
        if fmt not in extensions or width <= 0 or height <= 0:
            raise ValueError()
        return {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data), "width": width,
                "height": height, "format": fmt, "extension": extensions[fmt]}
    except (ValueError, OSError, UnidentifiedImageError, Image.DecompressionBombError) as exc:
        raise PipelineError("INVALID_IMAGE") from exc


def _partition(path, directory):
    directory.mkdir()
    count = 0
    for row in rows(path):
        key = product_key(row)
        part = digest(list(key))[:2]
        with (directory / (part + ".jsonl")).open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(canonical(row) + "\n")
        count += 1
    return count


def _sorted_unique(path, work):
    if not path.exists():
        return
    chunks, batch, size = [], [], 0

    def flush():
        target = work / f"{path.stem}-{len(chunks)}.jsonl"
        with target.open("w", encoding="utf-8", newline="\n") as stream:
            for row in sorted(batch, key=product_key):
                stream.write(canonical(row) + "\n")
        chunks.append(target)
        batch.clear()

    for row in rows(path):
        batch.append(row)
        size += len(canonical(row).encode())
        if len(batch) >= 100 or size >= 32 * 1024 * 1024:
            flush()
            size = 0
    if batch:
        flush()
    with ExitStack() as stack:
        iterators = [stack.enter_context(closing(rows(p))) for p in chunks]
        previous = None
        for row in heapq.merge(*iterators, key=product_key):
            key = product_key(row)
            if key == previous:
                raise PipelineError("DUPLICATE_PRODUCT")
            previous = key
            yield row


def _gallery(normal, media, media_root, build, budget=None):
    observed, acquired = normal.get("images") or [], (media or normal).get("images") or []
    if not isinstance(observed, list) or not isinstance(acquired, list) or len(observed) != len(acquired):
        raise PipelineError("GALLERY_MISMATCH")
    seen_urls, seen_orders, representative = set(), set(), 0
    result = []
    for source, image in zip(observed, acquired):
        if any(source.get(k) != image.get(k) for k in GALLERY_FIELDS):
            raise PipelineError("GALLERY_MISMATCH")
        if any(str(image.get(k, normal[k])) != str(normal[k]) for k in ("source", "product_id")):
            raise PipelineError("IMAGE_IDENTITY_MISMATCH")
        if (not isinstance(source.get("url"), str) or not source["url"].startswith(("https://", "http://"))
                or source["url"] in seen_urls or type(source.get("order")) is not int
                or source["order"] < 0 or source["order"] in seen_orders
                or source.get("role") not in {"representative", "gallery"}):
            raise PipelineError("INVALID_GALLERY")
        seen_urls.add(source["url"])
        seen_orders.add(source["order"])
        representative += source["role"] == "representative"
        if representative > 1:
            raise PipelineError("INVALID_GALLERY")
        entry = {**{k: source.get(k) for k in GALLERY_FIELDS},
                 "acquisition_state": "UNAVAILABLE", "object_key": None}
        if image.get("status") == "downloaded":
            if media_root is None:
                raise PipelineError("MEDIA_ROOT_REQUIRED")
            path = confined(media_root, image.get("local_path"))
            if path.stat().st_size > 20 * 1024 * 1024:
                raise PipelineError("IMAGE_TOO_LARGE")
            data = path.read_bytes()
            actual = inspect_image(data)
            if any(image.get(k) != v for k, v in actual.items()):
                raise PipelineError("IMAGE_CHECKSUM_OR_METADATA_MISMATCH")
            if image.get("filename") and Path(image["local_path"]).name != image["filename"]:
                raise PipelineError("IMAGE_FILENAME_MISMATCH")
            codes = {c["category_id"] for c in normal.get("categories") or []} or {"_uncategorized"}
            if image.get("category_paths") is not None:
                if (set(image["category_paths"]) != codes or path not in
                        {confined(media_root, relative) for relative in image['category_paths'].values()}):
                    raise PipelineError("IMAGE_CATEGORY_MISMATCH")
                for code, relative in image["category_paths"].items():
                    expected = f"categories/{code}/danawa/{normal['product_id']}/{image.get('filename')}"
                    if relative != expected:
                        raise PipelineError("IMAGE_CATEGORY_MISMATCH")
                    linked = confined(media_root, relative)
                    if linked.stat().st_size != actual["bytes"] or sha_file(linked) != actual["sha256"]:
                        raise PipelineError("IMAGE_CATEGORY_FILE_MISMATCH")
            key = f"images/sha256/{actual['sha256'][:2]}/{actual['sha256']}.{actual['extension']}"
            target = confined(build, key)
            if not target.exists():
                if budget is not None:
                    budget.reserve(len(data))
                atomic_bytes(target, data)
            entry.update(actual, acquisition_state="VERIFIED", object_key=key)
        result.append(entry)
    return result


def _validate(raw, normal, media):
    if product_key(raw) != product_key(normal) or raw.get("schema_version") != "1.2":
        raise PipelineError("UNSUPPORTED_SOURCE_SCHEMA")
    utc(raw["collected_at"])
    if not isinstance(raw.get("job_id"), str) or not raw["job_id"]:
        raise PipelineError("MISSING_RUN_ID")
    if raw["payload_hash"] != collector_payload_hash(raw["payload"]):
        raise PipelineError("PAYLOAD_HASH_MISMATCH")
    if not raw["payload"]["listings"]:
        raise PipelineError("LISTING_MISSING")
    for row in (normal, media or normal):
        if (product_key(row) != product_key(raw) or row.get("job_id") != raw["job_id"]
                or row.get("raw_payload_hash") != raw["payload_hash"]
                or row.get("collected_at") != raw["collected_at"] or row.get("categories") != raw.get("categories")):
            raise PipelineError("OBSERVATION_MISMATCH")
    if normal.get("product_url") and not valid_product_url(normal["product_url"], raw["product_id"]):
        raise PipelineError("PRODUCT_URL_MISMATCH")
    if media:
        strip = lambda row: {k: v for k, v in row.items() if k not in {"images", "image_download_status"}}
        if strip(normal) != strip(media):
            raise PipelineError("MEDIA_OBSERVATION_MISMATCH")


def prepare(export, destination, media_manifest=None):
    export, destination = Path(export).resolve(), Path(destination).resolve()
    media_manifest = Path(media_manifest).resolve() if media_manifest else None
    if destination.exists() or destination.is_relative_to(export) or export.is_relative_to(destination):
        raise PipelineError("SNAPSHOT_DESTINATION_NOT_SEPARATE")
    for base in [export, *list(export.parents)[:2]]:
        writer = base / "writer.lock"
        if writer.is_file():
            with lock(writer, existing=True):
                pass  # check only; no collector lock is held while copying
            break
    if (export / "pipeline.lock").exists():
        raise PipelineError("GALLERY_WRITER_OR_STALE_LOCK")
    if media_manifest:
        report = media_manifest.parent / "download_report.json"
        if report.is_file() and not read_json(report).get("finished_at"):
            raise PipelineError("MEDIA_ACTIVE_USE_LIVE_CONTRACT")
    files = {"product_raw.jsonl": export / "product_raw.jsonl", "products.jsonl": export / "products.jsonl"}
    if media_manifest:
        files["media_manifest.jsonl"] = media_manifest
    for name in ("run_report.json", "gallery_report.json"):
        if (export / name).is_file():
            files[name] = export / name
    before = {name: sha_file(p) for name, p in files.items()}
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pipeline-prepare-", dir=destination.parent) as temp:
        work = Path(temp)
        build = work / "sealed"
        (build / "input").mkdir(parents=True)
        for name, path in files.items():
            shutil.copyfile(path, build / "input" / name)
            if sha_file(build / "input" / name) != before[name]:
                raise PipelineError("INPUT_CHANGED_DURING_COPY")
        reports = {name: read_json(build / "input" / name) for name in files if name.endswith("report.json")}
        counts = {}
        names = ["product_raw.jsonl", "products.jsonl"] + (["media_manifest.jsonl"] if media_manifest else [])
        partitions = []
        for index, name in enumerate(names):
            part_dir = work / f"part-{index}"
            counts[name] = _partition(build / "input" / name, part_dir)
            partitions.append(part_dir)
        if not counts[names[0]] or len(set(counts.values())) != 1:
            raise PipelineError("PRODUCT_SET_MISMATCH")
        run_ids, output_count, image_count = set(), 0, 0
        output = build / "observations.jsonl"
        with output.open("w", encoding="utf-8", newline="\n") as stream:
            for number in range(256):
                part = f"{number:02x}.jsonl"
                with ExitStack() as readers:
                    generators = []
                    for index, directory in enumerate(partitions):
                        sort_dir = work / f"sort-{number}-{index}"
                        sort_dir.mkdir()
                        generators.append(readers.enter_context(closing(_sorted_unique(directory / part, sort_dir))))
                    for joined in zip_longest(*generators):
                        if any(row is None for row in joined) or len({product_key(r) for r in joined}) != 1:
                            raise PipelineError("PRODUCT_SET_MISMATCH")
                        raw, normal, *rest = joined
                        media = rest[0] if rest else None
                        _validate(raw, normal, media)
                        images = _gallery(normal, media, media_manifest.parent if media_manifest else None, build)
                        run_ids.add(raw["job_id"])
                        report = reports.get("run_report.json", {})
                        lineage = {"status": "OBSERVED" if reports else "UNKNOWN",
                                   "execution_mode": report.get("execution_mode"), "original_run_id": report.get("original_run_id")}
                        row = {"raw": raw, "normalized": normal, "images": images, "lineage": lineage,
                               "observation_key": observation_key(raw, images)}
                        stream.write(canonical(row) + "\n")
                        output_count += 1
                        image_count += sum(i["acquisition_state"] == "VERIFIED" for i in images)
            stream.flush()
            import os
            os.fsync(stream.fileno())
        if len(run_ids) != 1:
            raise PipelineError("MULTIPLE_COLLECTION_RUNS")
        if any(sha_file(path) != before[name] for name, path in files.items()):
            raise PipelineError("INPUT_CHANGED_DURING_PREPARE")
        stable_files = {p.relative_to(build).as_posix(): {"sha256": sha_file(p), "bytes": p.stat().st_size}
                        for p in sorted(build.rglob("*")) if p.is_file()}
        stable = {"schema_version": 1, "kind": "FULL_EXPORT", "files": stable_files,
                  "product_count": output_count, "verified_image_relations": image_count,
                  "collection_run_id": next(iter(run_ids))}
        manifest = {**stable, "snapshot_id": digest(stable), "prepared_at": now()}
        atomic_json(build / "snapshot.json", manifest)
        build.rename(destination)
        sync_dir(destination.parent)
    return manifest


def verify(directory, record_proofs=None):
    directory = Path(directory).resolve()
    manifest = read_json(directory / "snapshot.json")
    stable = {k: v for k, v in manifest.items() if k not in {"snapshot_id", "prepared_at"}}
    if manifest.get("schema_version") != 1 or manifest.get("kind") not in {"FULL_EXPORT", "LIVE_DELTA"} or digest(stable) != manifest.get("snapshot_id"):
        raise PipelineError("INVALID_SNAPSHOT_MANIFEST")
    required = {"observations.jsonl"}
    if manifest['kind'] == 'FULL_EXPORT':
        required.update({'input/product_raw.jsonl', 'input/products.jsonl'})
    elif (manifest.get('handoff_schema_version') != 1 or manifest.get('phase') not in {'SOURCE_METADATA', 'GALLERY_ACQUISITION'}
          or manifest.get('source_format') != 'jsonl-v1'):
        raise PipelineError('LIVE_MANIFEST_CONTRACT_INVALID')
    if not required.issubset(manifest["files"]):
        raise PipelineError("SNAPSHOT_FILES_MISSING")
    for name, info in manifest["files"].items():
        path = confined(directory, name)
        if path.stat().st_size != info["bytes"] or sha_file(path) != info["sha256"]:
            raise PipelineError("SNAPSHOT_FILE_CHANGED")
    count, seen, checked_images, relation_count = 0, set(), {}, 0
    for row in rows(directory / "observations.jsonl"):
        _validate(row["raw"], row["normalized"], None)
        key = observation_key(row["raw"], row["images"])
        if key != row["observation_key"] or key in seen or row["raw"]["job_id"] != manifest["collection_run_id"]:
            raise PipelineError("SNAPSHOT_IDENTITY_MISMATCH")
        seen.add(key)
        if record_proofs is not None:
            record_proofs[key] = digest(row)
        expected_gallery = [{k: i.get(k) for k in GALLERY_FIELDS} for i in row["normalized"].get("images") or []]
        if expected_gallery != [{k: i.get(k) for k in GALLERY_FIELDS} for i in row["images"]]:
            raise PipelineError("SNAPSHOT_GALLERY_MISMATCH")
        for image in row["images"]:
            name = image.get("object_key")
            if image.get("acquisition_state") not in {"VERIFIED", "UNAVAILABLE"}:
                raise PipelineError("SNAPSHOT_ACQUISITION_STATE_INVALID")
            if image["acquisition_state"] == "UNAVAILABLE":
                if name is not None or any(image.get(k) is not None for k in IMAGE_META):
                    raise PipelineError("SNAPSHOT_UNAVAILABLE_IMAGE_HAS_BYTES")
                continue
            expected_name = f"images/sha256/{image['sha256'][:2]}/{image['sha256']}.{image['extension']}"
            if name != expected_name or name not in manifest["files"] or manifest["files"][name]["sha256"] != image["sha256"]:
                raise PipelineError("SNAPSHOT_IMAGE_MISSING_OR_UNSAFE_KEY")
            path = confined(directory, name)
            if path.stat().st_size > 20 * 1024 * 1024:
                raise PipelineError("IMAGE_TOO_LARGE")
            if name not in checked_images:
                if len(checked_images) >= 1024:
                    checked_images.clear()
                checked_images[name] = inspect_image(path.read_bytes())
            if any(image.get(k) != v for k, v in checked_images[name].items()):
                raise PipelineError("SNAPSHOT_IMAGE_METADATA_MISMATCH")
            relation_count += 1
        count += 1
    if count != manifest["product_count"]:
        raise PipelineError("SNAPSHOT_COUNT_MISMATCH")
    if relation_count != manifest["verified_image_relations"]:
        raise PipelineError("SNAPSHOT_IMAGE_COUNT_MISMATCH")
    if manifest['kind'] == 'LIVE_DELTA':
        from .live import verify_evidence
        verify_evidence(directory, manifest)
    return manifest
