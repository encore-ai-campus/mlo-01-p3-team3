"""MongoBatchStore bulk-write contract against an in-memory collection (no server)."""
import unittest

import test_engine  # noqa: F401  (puts the package on sys.path like the other tests)
from pymongo.errors import BulkWriteError, DuplicateKeyError

from pipeline_v2.adapters.batch_store import MongoBatchStore
from pipeline_v2.domain.contracts import Invalid


class Collection:
    def __init__(self):
        self.rows = {}

    def insert_one(self, record):
        if record['_id'] in self.rows:
            raise DuplicateKeyError('duplicate')
        self.rows[record['_id']] = dict(record)

    def insert_many(self, records, ordered=True):
        errors = []
        for record in records:
            if record['_id'] in self.rows:
                errors.append(dict(code=11000, op=record))
            else:
                self.rows[record['_id']] = dict(record)
        if errors:
            raise BulkWriteError(dict(writeErrors=errors))

    def find_one(self, query):
        return self.rows.get(query['_id'])

    def find(self, query, projection=None):
        return [dict(self.rows[i]) for i in query['_id']['$in'] if i in self.rows]


class Database(dict):
    def __missing__(self, name):
        self[name] = Collection()
        return self[name]


class MongoBatchStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = MongoBatchStore.__new__(MongoBatchStore)
        self.store.database = Database()

    def test_repeated_composite_keys_with_same_values_are_accepted(self):
        items = [(['danawa:11', 'v1'], dict(raw='a')), (['danawa:12', 'v1'], dict(raw='b'))]
        self.store.put_many('observation_identity', items)
        # A retried prepare writes the same composite keys again.
        self.store.put_many('observation_identity', items)
        self.assertEqual(self.store.get_list('observation_identity', [k for k, _ in items]), [v for _, v in items])

    def test_changed_value_under_an_existing_composite_key_is_a_conflict(self):
        self.store.put_many('observation_identity', [(['danawa:11', 'v1'], dict(raw='a'))])
        with self.assertRaisesRegex(Invalid, 'IMMUTABLE_BATCH_RECORD_CONFLICT'):
            self.store.put_many('observation_identity', [(['danawa:11', 'v1'], dict(raw='changed'))])

    def test_new_records_in_the_same_call_are_still_stored(self):
        self.store.put_many('source', [('one', dict(n=1))])
        self.store.put_many('source', [('one', dict(n=1)), ('two', dict(n=2))])
        self.assertEqual(self.store.get_list('source', ['one', 'two']), [dict(n=1), dict(n=2)])


if __name__ == '__main__':
    unittest.main()
