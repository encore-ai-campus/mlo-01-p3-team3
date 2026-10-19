"""Copy a selected saved-export subset and rehearse twice; never connect to services."""
import argparse
from contextlib import closing
from pathlib import Path
import shutil

from product_pipeline.config import Settings
from product_pipeline.contracts import canonical
from product_pipeline.files import atomic_json, confined, read_json, rows, sha_file
from product_pipeline.local import FileLedger, FileObjects, FilePublisher
from product_pipeline.runner import ingest
from product_pipeline.snapshot import prepare


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-dir", type=Path, required=True)
    parser.add_argument("--media-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sample-size", type=int, default=3)
    args = parser.parse_args()
    original, media = args.export_dir.resolve(), args.media_manifest.resolve()
    output = args.output.resolve()
    if (args.sample_size < 1 or output.exists() or output.is_relative_to(original)
            or original.is_relative_to(output) or media.is_relative_to(output)):
        parser.error("use a positive sample size and a new output separate from the original data")
    inputs = [original / "product_raw.jsonl", original / "products.jsonl", media]
    before = {str(p): sha_file(p) for p in inputs}
    def select(path, ids=None):
        selected = []
        with closing(rows(path)) as stream:
            for row in stream:
                if ids is None or row["product_id"] in ids:
                    selected.append(row)
                    if len(selected) == args.sample_size:
                        break
        return selected
    raw = select(inputs[0])
    ids = {r["product_id"] for r in raw}
    normal, localized = select(inputs[1], ids), select(inputs[2], ids)
    if len(raw) != args.sample_size or len(normal) != len(raw) or len(localized) != len(raw):
        parser.error("not enough matching products")
    export = output / "source"
    (export / "media").mkdir(parents=True)
    for name, values in (("product_raw.jsonl", raw), ("products.jsonl", normal),
                         ("media/products_with_local_images.jsonl", localized)):
        (export / name).write_text("".join(canonical(v) + "\n" for v in values), encoding="utf-8")
    for row in localized:
        for image in row.get("images") or []:
            if image.get("status") == "downloaded":
                for relative in set((image.get("category_paths") or {}).values()) | {image["local_path"]}:
                    source, target = confined(media.parent, relative), confined(export / "media", relative)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, target)
    report = read_json(original / "run_report.json") if (original / "run_report.json").exists() else {}
    atomic_json(export / "run_report.json", {**report, "product_count": len(raw), "validation_selection": sorted(ids)})
    if (media.parent / "download_report.json").exists():
        shutil.copyfile(media.parent / "download_report.json", export / "media/download_report.json")
    sealed = output / "snapshot"
    manifest = prepare(export, sealed, export / "media/products_with_local_images.jsonl")
    settings = Settings("local-smoke", "rehearsal", "rehearsal-images")
    ledger, objects, publisher = FileLedger(output / "ledger", settings), FileObjects(output / "objects"), FilePublisher(output / "events")
    first = ingest(sealed, settings, ledger, objects, publisher, output / "audit")
    second = ingest(sealed, settings, ledger, objects, publisher, output / "audit")
    unchanged = before == {str(p): sha_file(p) for p in inputs}
    passed = (unchanged and first["result"] == second["result"] == "completed" and first["raw_created"] == len(raw)
              and first["events_sent"] == len(raw) and second["raw_created"] == second["objects_written"] == second["events_sent"] == 0)
    summary = {"passed": passed, "selected_ids": sorted(ids), "source_run": raw[0]["job_id"],
               "snapshot_id": manifest["snapshot_id"], "image_relations": manifest["verified_image_relations"],
               "first_run": first, "second_run": second, "original_inputs_unchanged": unchanged, "live_connections": 0}
    atomic_json(output / "smoke_report.json", summary)
    print(canonical({k: v for k, v in summary.items() if k not in {"first_run", "second_run"}}))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
