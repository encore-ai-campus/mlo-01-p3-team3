import json
from pathlib import Path
import tempfile
import unittest

from helpers import fixture
from product_pipeline.config import Settings
from product_pipeline.contracts import digest
from product_pipeline.errors import Conflict, LeaseLost, PipelineError
from product_pipeline.files import atomic_json, read_json, rows
from product_pipeline.local import FileLedger, FileObjects, FilePublisher
from product_pipeline.runner import ingest
from product_pipeline.snapshot import prepare


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source, self.media = fixture(self.root, codes=("11",))
        self.sealed = self.root / "snapshot"
        prepare(self.source, self.sealed, self.media)
        self.settings = Settings("test", "test", "images")
        self.ledger = FileLedger(self.root / "ledger", self.settings)
        self.objects = FileObjects(self.root / "objects")
        self.publisher = FilePublisher(self.root / "events")
        self.row = next(rows(self.sealed / "observations.jsonl"))

    def run_ingest(self, **kwargs):
        return ingest(self.sealed, self.settings, self.ledger, self.objects, self.publisher,
                      self.root / "audit", sleep=lambda _: None, **kwargs)

    def test_repeat_has_no_new_raw_events_or_blob_writes(self):
        first = self.run_ingest()
        saved = self.ledger.get(self.row["observation_key"])
        second = self.run_ingest()
        after = self.ledger.get(self.row["observation_key"])
        self.assertEqual(first["result"], "completed")
        self.assertEqual((first["raw_created"], first["events_sent"], first["objects_written"]), (1, 1, 1))
        self.assertEqual((second["raw_created"], second["events_sent"], second["events_reused"], second["objects_written"]), (0, 0, 1, 0))
        self.assertEqual(saved["document"], after["document"])
        self.assertEqual(saved["event"], after["event"])
        self.assertEqual(saved["image_state_revision"], after["image_state_revision"])
        self.assertEqual(len(list((self.root / "ledger/raw_products").glob("*.json"))), 1)
        self.assertEqual(len(list((self.root / "events").glob("*.json"))), 1)

    def test_completed_resume_does_not_contact_successful_backends(self):
        self.run_ingest()
        def forbidden(*args):
            raise AssertionError("completed stage contacted backend")
        self.objects.ensure = self.ledger.ensure_raw = self.publisher.send = forbidden
        result = self.run_ingest()
        self.assertEqual((result["result"], result["failed"]), ("completed", 0))
        self.assertEqual((result["observations_reused"], result["stages_reused"]), (1, 3))
        self.assertEqual((result["raw_reused"], result["image_relations_reused"]), (1, 2))

    def test_partial_image_resume_only_retries_failed_slot_and_reuses_raw_event(self):
        ensure = self.objects.ensure
        calls = []
        def partial(key, source, checksum, size):
            calls.append(source)
            if len(calls) > 1:
                raise PipelineError("OBJECT_READ_FAILED")
            return ensure(key, source, checksum, size)
        self.objects.ensure = partial
        self.assertEqual(self.run_ingest()["result"], "partial")
        before = self.ledger.get(self.row["observation_key"])
        retried = []
        def recover(*args):
            retried.append(args[0])
            return ensure(*args)
        self.objects.ensure = recover
        self.ledger.ensure_raw = lambda *_: (_ for _ in ()).throw(AssertionError("raw repeated"))
        result = self.run_ingest()
        self.assertEqual((result["result"], len(retried), result["events_sent"]), ("completed", 1, 0))
        self.assertEqual(result["image_relations_reused"], 1)
        after = self.ledger.get(self.row["observation_key"])
        self.assertEqual((after["document"], after["event"]), (before["document"], before["event"]))

    def test_image_slot_checkpoint_survives_crash_before_next_slot(self):
        def crash(stage):
            if stage == "IMAGE_RESOLVED":
                raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.run_ingest(hook=crash)
        saved = self.ledger.get(self.row["observation_key"])
        self.assertEqual(sum(i["availability"] == "AVAILABLE" for i in saved["image_state_vector"]), 1)
        ensure, calls = self.objects.ensure, []
        def record(*args):
            calls.append(args[0])
            return ensure(*args)
        self.objects.ensure = record
        self.assertEqual(self.run_ingest()["result"], "completed")
        self.assertEqual(len(calls), 1)

    def test_state_is_reread_after_lease_acquisition(self):
        self.run_ingest()
        acquire = self.ledger.acquire
        def intervening_worker(key):
            value = self.ledger.get(key)
            value["event_confirmed"] = False
            value["stage"] = "EVENT_UNKNOWN"
            atomic_json(self.ledger._path(key), value)
            return acquire(key)
        self.ledger.acquire = intervening_worker
        self.assertEqual(self.run_ingest()["events_sent"], 1)

    def test_legacy_confirmed_stage_reuses_raw_without_new_marker(self):
        self.run_ingest()
        key = self.row["observation_key"]
        saved = self.ledger.get(key)
        saved.pop("raw_confirmed")
        atomic_json(self.ledger._path(key), saved)
        self.ledger.ensure_raw = lambda *_: (_ for _ in ()).throw(AssertionError("legacy raw repeated"))
        self.assertEqual(self.run_ingest()["observations_reused"], 1)

    def test_image_reuse_validates_fingerprint_and_slot_identity(self):
        self.run_ingest()
        key = self.row["observation_key"]
        original = self.ledger.get(key)
        for recompute in (False, True):
            with self.subTest(recompute=recompute):
                saved = read_json(self.ledger._path(key))
                saved["image_state_vector"][0]["checksum"] = "untrusted"
                saved["image_state_fingerprint"] = (digest(saved["image_state_vector"]) if recompute
                                                    else original["image_state_fingerprint"])
                atomic_json(self.ledger._path(key), saved)
                result = self.run_ingest()
                self.assertEqual(result["failed"], 1)
                self.assertEqual(result["observations_reused"], 0)
                self.assertEqual(result["events_sent"], 0)

    def test_new_observation_of_same_product_keeps_both_immutable_raw_documents(self):
        from helpers import write_rows
        from product_pipeline.contracts import collector_payload_hash
        self.run_ingest()
        original = self.ledger.get(self.row["observation_key"])
        raw = next(rows(self.source / "product_raw.jsonl"))
        raw["payload"]["detail"]["html"] = "a later observation"
        raw["payload_hash"] = collector_payload_hash(raw["payload"])
        write_rows(self.source / "product_raw.jsonl", [raw])
        for path in (self.source / "products.jsonl", self.media):
            normal = next(rows(path))
            normal["raw_payload_hash"] = raw["payload_hash"]
            write_rows(path, [normal])
        newer = self.root / "newer-snapshot"
        prepare(self.source, newer, self.media)
        result = ingest(newer, self.settings, self.ledger, self.objects, self.publisher,
                        self.root / "audit", sleep=lambda _: None)
        self.assertEqual((result["raw_created"], result["events_sent"]), (1, 1))
        documents = [read_json(path) for path in (self.root / "ledger/raw_products").glob("*.json")]
        self.assertEqual(len(documents), 2)
        self.assertIn(original["document"], documents)
        self.assertEqual(len({doc["observation_key"] for doc in documents}), 2)
        self.assertEqual(len({doc["source_record_key"] for doc in documents}), 1)

    def test_interrupt_at_external_boundaries_recovers_same_ids(self):
        for position in ("REGISTERED", "IMAGES_RESOLVED", "RAW_INSERTED_BEFORE_MARK", "RAW_CONFIRMED", "EVENT_SENT_BEFORE_MARK"):
            with self.subTest(position=position):
                def fail(stage):
                    if stage == position:
                        raise KeyboardInterrupt()
                with self.assertRaises(KeyboardInterrupt):
                    self.run_ingest(hook=fail, replay_events=True, repair_raw=True)
                saved = self.ledger.get(self.row["observation_key"])
                report = self.run_ingest()
                after = self.ledger.get(self.row["observation_key"])
                self.assertEqual(report["result"], "completed")
                self.assertEqual(saved["document"]["_id"], after["document"]["_id"])
                self.assertEqual(saved["event"], after["event"])
                self.assertEqual(after["stage"], "EVENT_CONFIRMED")

    def test_upload_failure_then_success_changes_revision_not_observation(self):
        original = self.objects.ensure
        self.objects.ensure = lambda *args: (_ for _ in ()).throw(PipelineError("OBJECT_READ_FAILED"))
        first = self.run_ingest()
        saved = self.ledger.get(self.row["observation_key"])
        self.assertEqual((first["result"], first["raw_confirmed"], first["events_sent"]), ("partial", 1, 1))
        self.assertTrue(all(i["availability"] == "FAILED" for i in saved["image_state_vector"]))
        self.objects.ensure = original
        second = self.run_ingest()
        after = self.ledger.get(self.row["observation_key"])
        self.assertEqual(second["result"], "completed")
        self.assertEqual(second["events_sent"], 0)
        self.assertGreater(after["image_state_revision"], saved["image_state_revision"])
        self.assertEqual(saved["document"], after["document"])
        self.assertEqual(saved["event"], after["event"])

    def test_publish_failure_raw_survives_and_only_fixed_event_is_retried(self):
        send = self.publisher.send
        self.publisher.send = lambda *args: (_ for _ in ()).throw(PipelineError("EVENT_DELIVERY_UNCONFIRMED"))
        failed = self.run_ingest()
        saved = self.ledger.get(self.row["observation_key"])
        self.assertEqual((failed["failed"], saved["stage"]), (1, "EVENT_UNKNOWN"))
        self.assertEqual(len(list((self.root / "ledger/raw_products").glob("*.json"))), 1)
        self.publisher.send = send
        self.objects.ensure = self.ledger.ensure_raw = lambda *_: (_ for _ in ()).throw(AssertionError("completed dependency repeated"))
        completed = self.run_ingest()
        self.assertEqual(completed["result"], "completed")
        published = next((self.root / "events").glob("*.json"))
        self.assertEqual(read_json(published)["event"], saved["event"])

    def test_replay_is_explicit_and_preserves_event_body(self):
        self.run_ingest()
        original = self.ledger.get(self.row["observation_key"])["event"]
        replay = self.run_ingest(replay_events=True)
        self.assertEqual(replay["events_sent"], 1)
        self.assertEqual(self.ledger.get(self.row["observation_key"])["event"], original)

    def test_raw_and_event_conflicts_are_not_overwritten(self):
        self.run_ingest()
        path = next((self.root / "ledger/raw_products").glob("*.json"))
        value = read_json(path)
        value["payload"]["detail"]["html"] = "tampered"
        atomic_json(path, value)
        result = self.run_ingest(replay_events=True, repair_raw=True)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["events_sent"], 0)
        self.assertEqual(read_json(path), value)

    def test_ledger_body_tampering_detected_before_external_writes(self):
        self.run_ingest()
        path = self.ledger._path(self.row["observation_key"])
        value = read_json(path)
        value["event"]["payload"]["document_hash"] = "different"
        atomic_json(path, value)
        before = self.publisher.calls
        result = self.run_ingest(replay_events=True)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(self.publisher.calls, before)

    def test_missing_raw_is_restored_under_original_id(self):
        self.run_ingest()
        saved = self.ledger.get(self.row["observation_key"])
        (self.root / "ledger/raw_products" / (saved["document"]["_id"] + ".json")).unlink()
        result = self.run_ingest(repair_raw=True)
        self.assertEqual(result["raw_created"], 1)
        self.assertEqual(self.ledger.get(self.row["observation_key"])["document"], saved["document"])

    def test_registry_loss_uses_archived_registration_not_new_identifiers(self):
        self.run_ingest()
        saved = self.ledger.get(self.row["observation_key"])
        self.ledger._path(self.row["observation_key"]).unlink()
        result = self.run_ingest()
        restored = self.ledger.get(self.row["observation_key"])
        self.assertEqual(result["result"], "completed")
        self.assertEqual(saved["document"], restored["document"])
        self.assertEqual(saved["event"], restored["event"])

    def test_same_observation_in_new_snapshot_reuses_original_envelope(self):
        self.run_ingest()
        original = self.ledger.get(self.row["observation_key"])
        report = read_json(self.source / "run_report.json")
        report["diagnostic"] = "a different export snapshot"
        atomic_json(self.source / "run_report.json", report)
        other = self.root / "snapshot-two"
        prepare(self.source, other, self.media)
        self.assertNotEqual(read_json(self.sealed / "snapshot.json")["snapshot_id"], read_json(other / "snapshot.json")["snapshot_id"])
        result = ingest(other, self.settings, self.ledger, self.objects, self.publisher, self.root / "audit", sleep=lambda _: None)
        self.assertEqual(result["events_sent"], 0)
        self.assertEqual(self.ledger.get(self.row["observation_key"])["document"], original["document"])

    def test_target_mismatch_and_stale_fencing_token_rejected(self):
        self.run_ingest()
        wrong = FileLedger(self.root / "ledger", Settings("other", "test", "images"))
        with self.assertRaises(Conflict):
            with wrong.exclusive():
                pass
        key = self.row["observation_key"]
        first = self.ledger.acquire(key)
        with self.assertRaisesRegex(PipelineError, "LEASE_BUSY"):
            self.ledger.acquire(key)
        self.ledger.release(key, first)
        other = FileLedger(self.root / "ledger", self.settings)
        second = other.acquire(key)
        with self.assertRaises(LeaseLost):
            self.ledger.update(key, first, stage="BAD")
        other.release(key, second)

    def test_no_media_keeps_raw_and_partial_image_status(self):
        sealed = self.root / "without-media"
        prepare(self.source, sealed)
        result = ingest(sealed, self.settings, self.ledger, self.objects, self.publisher, self.root / "audit")
        self.assertEqual((result["result"], result["raw_confirmed"], result["image_relations_unavailable"]), ("partial", 1, 2))

    def test_snapshot_mutation_after_validation_is_not_ingested(self):
        from helpers import write_rows
        original = self.ledger.snapshot
        def mutate(manifest):
            original(manifest)
            changed = dict(self.row)
            changed["normalized"] = {**changed["normalized"], "name": "changed after validation"}
            write_rows(self.sealed / "observations.jsonl", [changed])
        self.ledger.snapshot = mutate
        with self.assertRaisesRegex(PipelineError, "SNAPSHOT_CHANGED_DURING_INGEST"):
            self.run_ingest()
        self.assertFalse((self.root / "ledger/raw_products").exists())
        self.assertEqual(self.publisher.calls, 0)


if __name__ == "__main__":
    unittest.main()
