"""LIVE recovery regressions using committed journal and downloaded-media fixtures."""
import copy
from pathlib import Path
import tempfile
import unittest

from helpers import write_rows
from test_live import append, commit, journal_event, source_fixture
from product_pipeline.contracts import canonical, digest
from product_pipeline.files import read_json, rows
from product_pipeline.live import capture
from product_pipeline.snapshot import product_key


class LiveRecoveryRegressions(unittest.TestCase):
    def capture(self, source, spool, **kwargs):
        return capture(source, spool, max_spool_bytes=100_000_000,
                       min_free_bytes=0, **kwargs)

    def replace_journal(self, source, events):
        (source / "checkpoint_events.jsonl").write_text(
            "".join(canonical(event) + "\n" for event in events), encoding="utf-8")

    def test_unmatched_first_media_page_is_retried_after_source_advances(self):
        with tempfile.TemporaryDirectory() as directory:
            source, media, pairs = source_fixture(directory)
            spool = Path(directory) / "spool"
            pairs.sort(key=lambda pair: digest(list(product_key(pair["raw"])))[:2])
            self.replace_journal(source, [journal_event(1, [pairs[1]])])
            self.capture(source, spool)
            first = self.capture(source, spool, media_manifest=media, max_records=1)
            self.assertEqual(first["media_wait"][0]["status"], "WAIT_SOURCE_MATCH")
            self.capture(source, spool, media_manifest=media, max_records=1)
            append(source, journal_event(2, [pairs[0]]))
            commit(source, 2)
            self.capture(source, spool)
            resumed = self.capture(source, spool, media_manifest=media)
            self.assertEqual(resumed["result"], "CAPTURED_WAITING_INGEST")
            linked = list(rows(Path(resumed["snapshot"]) / "observations.jsonl"))
            self.assertTrue(any(row["raw"]["product_id"] == pairs[0]["raw"]["product_id"]
                                for row in linked))

    def test_final_source_status_reclassifies_waits_without_a_new_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            source, media, _ = source_fixture(directory)
            spool = Path(directory) / "spool"
            self.replace_journal(source, [journal_event(1, [])])
            self.capture(source, spool)
            waiting = self.capture(source, spool, media_manifest=media)
            self.assertTrue(all(item["status"] == "WAIT_SOURCE_MATCH"
                                for item in waiting["media_wait"]))
            commit(source, 1, "completed")
            completed = self.capture(source, spool, media_manifest=media)
            self.assertTrue(completed["media_wait"])
            self.assertTrue(all(item["status"] == "MEDIA_SOURCE_MISMATCH"
                                for item in completed["media_wait"]))

    def test_empty_control_events_obey_capture_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            source, _, _ = source_fixture(directory)
            spool = Path(directory) / "spool"
            self.replace_journal(source, [journal_event(seq, []) for seq in range(1, 7)])
            commit(source, 6)
            first = self.capture(source, spool, max_records=2)
            events = list(rows(Path(first["snapshot"]) / "events.jsonl"))
            self.assertGreater(len(events), 0)
            self.assertLessEqual(len(events), 2)
            self.assertLess(read_json(spool / "cursor.json")["last_seq"], 6)
            for _ in range(5):
                self.capture(source, spool, max_records=2)
            self.assertEqual(read_json(spool / "cursor.json")["last_seq"], 6)

    def test_large_original_envelope_is_isolated_from_other_envelopes(self):
        with tempfile.TemporaryDirectory() as directory:
            source, _, pairs = source_fixture(directory)
            spool = Path(directory) / "spool"
            self.replace_journal(source, [journal_event(1, pairs * 20), journal_event(2, [pairs[0]])])
            commit(source, 2)
            # Selected pairs fit, but original hash evidence is larger than the batch budget.
            first = self.capture(source, spool, max_batch_bytes=100_000)
            events = list(rows(Path(first["snapshot"]) / "events.jsonl"))
            self.assertGreater((Path(first["snapshot"]) / "events.jsonl").stat().st_size, 50_000)
            # Use a lower limit than the original envelope on the next independent spool.
            smaller = self.capture(source, Path(directory) / "small-spool", max_batch_bytes=50_000)
            isolated = list(rows(Path(smaller["snapshot"]) / "events.jsonl"))
            self.assertEqual(len(isolated), 1)
            self.assertEqual(isolated[0]["event"]["seq"], 1)
            self.assertEqual(len(events), 1)

    def test_gallery_preserves_every_equivalent_source_pointer(self):
        with tempfile.TemporaryDirectory() as directory:
            source, media, pairs = source_fixture(directory)
            spool = Path(directory) / "spool"
            self.replace_journal(source, [journal_event(1, [pairs[0], pairs[0]])])
            self.capture(source, spool)
            acquired = self.capture(source, spool, media_manifest=media)
            row = next(rows(Path(acquired["snapshot"]) / "observations.jsonl"))
            self.assertEqual({(p["seq"], p["record_index"]) for p in row["lineage"]["source_pointers"]},
                             {(1, 0), (1, 1)})

    def test_download_diagnostics_and_path_changes_preserve_observation_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            source, media, _ = source_fixture(directory)
            spool = Path(directory) / "spool"
            self.capture(source, spool)
            original = self.capture(source, spool, media_manifest=media)
            original_keys = {row["observation_key"] for row in
                             rows(Path(original["snapshot"]) / "observations.jsonl")}
            changed = copy.deepcopy(list(rows(media)))
            for product in changed:
                product["image_download_status"] = "different diagnostic"
                for image in product["images"]:
                    image["error"] = "an operational diagnostic"
                    image["downloaded_at"] = "2026-10-16T13:00:00+09:00"
                    image["local_path"] = "./" + image["local_path"]
            write_rows(media, changed)
            resumed = self.capture(source, spool, media_manifest=media)
            if resumed["result"] == "CAPTURED_WAITING_INGEST":
                resumed_keys = {row["observation_key"] for row in
                                rows(Path(resumed["snapshot"]) / "observations.jsonl")}
                self.assertTrue(resumed_keys.issubset(original_keys))
            else:
                self.assertEqual(resumed["result"], "IDLE")


if __name__ == "__main__":
    unittest.main()
