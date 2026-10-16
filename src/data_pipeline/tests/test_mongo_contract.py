"""Adapter contract tests with real BSON/PyMongo types and an in-memory transport double."""
import copy
from pathlib import Path
import tempfile
import types
import unittest

from pymongo.errors import AutoReconnect, DuplicateKeyError

from helpers import fixture
from product_pipeline.config import Settings
from product_pipeline.errors import Conflict, LeaseLost, PipelineError
from product_pipeline.files import rows
from product_pipeline.mongo import MongoLedger
from product_pipeline.snapshot import prepare


class Collection:
    def __init__(self):
        self.data, self.indexes, self.last_update = {}, [], None
        self.lost_insert_response = False
        self.matched = 1

    def create_index(self, *args, **kwargs):
        self.indexes.append((args, kwargs))

    def insert_one(self, value):
        if value["_id"] in self.data or (value.get("observation_key") and any(
                v.get("observation_key") == value["observation_key"] for v in self.data.values())):
            raise DuplicateKeyError("duplicate")
        self.data[value["_id"]] = copy.deepcopy(value)
        if self.lost_insert_response:
            self.lost_insert_response = False
            raise AutoReconnect("response deliberately lost")

    def find_one(self, where):
        return next((copy.deepcopy(v) for v in self.data.values() if all(v.get(k) == x for k, x in where.items())), None)

    def update_one(self, where, update):
        self.last_update = copy.deepcopy((where, update))
        if self.matched and isinstance(update, dict):
            for value in self.data.values():
                if value.get("observation_key") == where.get("observation_key"):
                    value.update(update.get("$set", {}))
                    for key in update.get("$unset", {}):
                        value.pop(key, None)
        return types.SimpleNamespace(matched_count=self.matched)


class Database:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, Collection())

    def __getattr__(self, name):
        return self[name]


class Client:
    def __init__(self):
        self.database = Database()
        self.admin = types.SimpleNamespace(command=lambda _: {"setName": "rs-products", "hosts": ["a", "b", "c"]})

    def get_database(self, name, **kwargs):
        self.options = kwargs
        return self.database

    def close(self):
        pass


class MongoContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        source, media = fixture(root, codes=("11",))
        self.snapshot = prepare(source, root / "snapshot", media)
        self.row = next(rows(root / "snapshot/observations.jsonl"))
        self.client = Client()
        self.ledger = MongoLedger(Settings("test", "test", "images"), "unused", client=self.client)

    def test_majority_primary_journal_and_indexes(self):
        self.assertEqual(self.client.options["read_concern"].document, {"level": "majority"})
        self.assertEqual(self.client.options["write_concern"].document["w"], "majority")
        self.assertTrue(self.client.options["write_concern"].document["j"])
        with self.ledger.exclusive():
            pass
        self.assertTrue(any(kwargs.get("unique") for args, kwargs in self.ledger.raw.indexes))

    def test_lost_registration_response_reuses_inserted_ids(self):
        self.ledger.observations.lost_insert_response = True
        with self.assertRaises(AutoReconnect):
            self.ledger.register(self.row, self.snapshot)
        original = next(iter(self.ledger.observations.data.values()))
        restored = self.ledger.register(self.row, self.snapshot)
        self.assertEqual(original["document"], restored["document"])
        self.assertEqual(original["event"], restored["event"])
        self.assertEqual(len(self.ledger.observations.data), 1)

    def test_lost_raw_insert_response_is_reconciled_without_overwrite(self):
        saved = self.ledger.register(self.row, self.snapshot)
        self.ledger.raw.lost_insert_response = True
        with self.assertRaises(AutoReconnect):
            self.ledger.ensure_raw(saved["document"])
        self.assertFalse(self.ledger.ensure_raw(saved["document"]))
        self.assertEqual(len(self.ledger.raw.data), 1)
        stored = next(iter(self.ledger.raw.data.values()))
        stored["payload"]["detail"]["html"] = "corrupt"
        with self.assertRaises(Conflict):
            self.ledger.ensure_raw(saved["document"])
        self.assertEqual(stored["payload"]["detail"]["html"], "corrupt")

    def test_missing_registry_cannot_generate_new_id_over_existing_raw(self):
        saved = self.ledger.register(self.row, self.snapshot)
        self.ledger.ensure_raw(saved["document"])
        self.ledger.observations.data.clear()
        with self.assertRaisesRegex(PipelineError, "REGISTRATION_EVIDENCE_MISSING"):
            self.ledger.register(self.row, self.snapshot)
        restored = self.ledger.register(self.row, self.snapshot, archived=saved)
        self.assertEqual(saved["document"]["_id"], restored["document"]["_id"])
        self.assertEqual(saved["event"], restored["event"])

    def test_vector_update_is_single_cas_with_token_and_revision(self):
        saved = self.ledger.register(self.row, self.snapshot)
        key = self.row["observation_key"]
        self.ledger.images(key, 3, [{"image_slot": "representative", "availability": "AVAILABLE"}])
        where, update = self.ledger.observations.last_update
        self.assertEqual(where["lease_token"], 3)
        self.assertEqual(where["image_state_revision"], 0)
        self.assertIn("$expr", where)
        self.assertEqual(update["$set"]["image_state_revision"], 1)
        self.assertIn("image_state_vector", update["$set"])
        self.ledger.observations.matched = 0
        with self.assertRaises(LeaseLost):
            self.ledger.images(key, 2, [])

    def test_raw_envelope_cannot_be_updated_through_state_api(self):
        self.ledger.register(self.row, self.snapshot)
        with self.assertRaisesRegex(PipelineError, "IMMUTABLE_FIELD_UPDATE"):
            self.ledger.update(self.row["observation_key"], 1, document={"changed": True})

    def test_same_snapshot_receipt_ignores_capture_wall_clock(self):
        self.ledger.snapshot(self.snapshot)
        self.ledger.snapshot({**self.snapshot, "prepared_at": "later"})
        self.assertEqual(len(self.ledger.db.ingestion_snapshots.data), 1)


if __name__ == "__main__":
    unittest.main()
