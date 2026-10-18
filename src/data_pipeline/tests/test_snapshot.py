import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from helpers import fixture, write_rows
from product_pipeline.contracts import collector_payload_hash, digest, observation_key
from product_pipeline.errors import PipelineError
from product_pipeline.files import atomic_json, lock, read_json, rows, sha_file
from product_pipeline.snapshot import prepare, verify


class SnapshotTests(unittest.TestCase):
    def test_valid_snapshot_verifies_and_copies_shared_bytes_once(self):
        with tempfile.TemporaryDirectory() as temp:
            source, media = fixture(temp)
            before = sha_file(source / "product_raw.jsonl")
            manifest = prepare(source, Path(temp) / "sealed", media)
            self.assertEqual((manifest["product_count"], manifest["verified_image_relations"]), (2, 4))
            self.assertEqual(verify(Path(temp) / "sealed"), manifest)
            self.assertEqual(len(list((Path(temp) / "sealed/images").rglob("*.png"))), 1)
            self.assertEqual(before, sha_file(source / "product_raw.jsonl"))

    def test_repeat_prepare_has_same_identity_despite_prepared_at(self):
        with tempfile.TemporaryDirectory() as temp:
            source, media = fixture(temp)
            a = prepare(source, Path(temp) / "one", media)
            b = prepare(source, Path(temp) / "two", media)
            self.assertEqual(a["snapshot_id"], b["snapshot_id"])

    def test_raw_hash_or_image_tampering_rejected(self):
        for mode in ("raw", "image", "media_identity", "media_field", "missing_image", "unsafe_path"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                source, media = fixture(temp, codes=("11",))
                raw = list(rows(source / "product_raw.jsonl"))
                localized = list(rows(media))
                if mode == "raw":
                    raw[0]["payload"]["detail"]["html"] += "changed"
                    write_rows(source / "product_raw.jsonl", raw)
                elif mode == "image":
                    (media.parent / localized[0]["images"][0]["local_path"]).write_bytes(b"not an image")
                elif mode == "missing_image":
                    (media.parent / localized[0]["images"][0]["local_path"]).unlink()
                else:
                    if mode == "media_identity":
                        localized[0]["images"][0]["product_id"] = "12"
                    elif mode == "media_field":
                        localized[0]["name"] = "another product"
                    else:
                        localized[0]["images"][0]["local_path"] = "../outside.png"
                    write_rows(media, localized)
                with self.assertRaises((PipelineError, FileNotFoundError)):
                    prepare(source, Path(temp) / "sealed", media)
                self.assertFalse((Path(temp) / "sealed").exists())

    def test_duplicate_and_missing_product_sets_rejected(self):
        for duplicate in (True, False):
            with self.subTest(duplicate=duplicate), tempfile.TemporaryDirectory() as temp:
                source, _ = fixture(temp)
                values = list(rows(source / "products.jsonl"))
                write_rows(source / "products.jsonl", values + [values[0]] if duplicate else values[:1])
                with self.assertRaises(PipelineError):
                    prepare(source, Path(temp) / "sealed")

    def test_active_writer_not_removed_or_modified(self):
        with tempfile.TemporaryDirectory() as temp:
            source, _ = fixture(temp)
            with lock(source / "writer.lock"):
                with self.assertRaisesRegex(PipelineError, "WRITER_ACTIVE"):
                    prepare(source, Path(temp) / "sealed")
            self.assertTrue((source / "writer.lock").exists())

    def test_active_media_and_invalid_json_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            source, media = fixture(temp)
            atomic_json(media.parent / "download_report.json", {"result": "running"})
            with self.assertRaisesRegex(PipelineError, "MEDIA_ACTIVE"):
                prepare(source, Path(temp) / "sealed", media)
            (source / "products.jsonl").write_text('{"unfinished":', encoding="utf-8")
            with self.assertRaises(PipelineError):
                prepare(source, Path(temp) / "sealed")

    def test_snapshot_changes_detected_before_ingestion(self):
        with tempfile.TemporaryDirectory() as temp:
            source, media = fixture(temp)
            sealed = Path(temp) / "sealed"
            prepare(source, sealed, media)
            with (sealed / "observations.jsonl").open("a") as stream:
                stream.write("{}\n")
            with self.assertRaisesRegex(PipelineError, "SNAPSHOT_FILE_CHANGED"):
                verify(sealed)

    def test_unavailable_images_preserve_urls_without_claiming_files(self):
        with tempfile.TemporaryDirectory() as temp:
            source, media = fixture(temp, codes=("11",), unavailable=True)
            sealed = Path(temp) / "sealed"
            manifest = prepare(source, sealed, media)
            row = next(rows(sealed / "observations.jsonl"))
            self.assertEqual(manifest["verified_image_relations"], 0)
            self.assertEqual(len(row["images"]), 2)
            self.assertTrue(all(i["acquisition_state"] == "UNAVAILABLE" and i["object_key"] is None for i in row["images"]))

    def test_observation_keys_distinguish_source_and_acquisition_but_not_transfer_paths(self):
        raw = {"job_id": "one", "product_id": "11", "detail_error": None}
        image = {"url": "https://i/a", "order": 0, "role": "representative", "acquisition_state": "VERIFIED", "sha256": "a", "bytes": 1}
        first = observation_key(raw, [image])
        self.assertEqual(first, observation_key(raw, [{**image, "local_path": "moved", "error": "timeout", "downloaded_at": "later"}]))
        self.assertNotEqual(first, observation_key({**raw, "job_id": "two"}, [image]))
        self.assertNotEqual(first, observation_key({**raw, "detail_error": "changed source wrapper"}, [image]))
        self.assertNotEqual(first, observation_key(raw, [{**image, "sha256": "b"}]))

    def test_duplicate_json_keys_and_nonfinite_rejected(self):
        from product_pipeline.files import parse
        for value in ('{"x":1,"x":2}', '{"x":NaN}'):
            with self.assertRaises(PipelineError):
                parse(value)

    def test_input_changed_during_copy_is_not_published(self):
        import product_pipeline.snapshot as module
        with tempfile.TemporaryDirectory() as temp:
            source, _ = fixture(temp)
            original = module.shutil.copyfile
            def changing(src, dst):
                result = original(src, dst)
                if Path(src).name == "product_raw.jsonl":
                    with Path(src).open("a") as stream:
                        stream.write("\n")
                return result
            with patch.object(module.shutil, "copyfile", side_effect=changing):
                with self.assertRaisesRegex(PipelineError, "INPUT_CHANGED"):
                    prepare(source, Path(temp) / "sealed")

    def test_all_claimed_category_paths_must_exist(self):
        with tempfile.TemporaryDirectory() as temp:
            source, media = fixture(temp, codes=("11",))
            raw = list(rows(source / "product_raw.jsonl"))
            normal = list(rows(source / "products.jsonl"))
            localized = list(rows(media))
            for values in (raw, normal, localized):
                values[0]["categories"].append({"category_id": "15241194", "category_name": "다른 분류"})
            for image in localized[0]["images"]:
                image["category_paths"]["15241194"] = f"categories/15241194/danawa/11/{image['filename']}"
            write_rows(source / "product_raw.jsonl", raw)
            write_rows(source / "products.jsonl", normal)
            write_rows(media, localized)
            with self.assertRaises(FileNotFoundError):
                prepare(source, Path(temp) / "sealed", media)

    def test_resealed_manifest_cannot_choose_unexpected_object_prefix_or_fake_image_metadata(self):
        for mode in ("prefix", "width"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                source, media = fixture(temp, codes=("11",))
                sealed = Path(temp) / "sealed"
                manifest = prepare(source, sealed, media)
                values = list(rows(sealed / "observations.jsonl"))
                image = values[0]["images"][0]
                if mode == "prefix":
                    import shutil
                    original = sealed / image["object_key"]
                    image["object_key"] = "unexpected-object.png"
                    shutil.copyfile(original, sealed / image["object_key"])
                    manifest["files"][image["object_key"]] = {"sha256": sha_file(original), "bytes": original.stat().st_size}
                else:
                    image["width"] = 999
                    values[0]["observation_key"] = observation_key(values[0]["raw"], values[0]["images"])
                write_rows(sealed / "observations.jsonl", values)
                path = sealed / "observations.jsonl"
                manifest["files"]["observations.jsonl"] = {"sha256": sha_file(path), "bytes": path.stat().st_size}
                manifest["snapshot_id"] = digest({k: v for k, v in manifest.items() if k not in {"snapshot_id", "prepared_at"}})
                atomic_json(sealed / "snapshot.json", manifest)
                with self.assertRaisesRegex(PipelineError, "SNAPSHOT_IMAGE"):
                    verify(sealed)


if __name__ == "__main__":
    unittest.main()
