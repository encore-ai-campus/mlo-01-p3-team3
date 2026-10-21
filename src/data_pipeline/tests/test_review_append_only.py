import unittest
from test_review import Ledger,approved,AT
from product_pipeline import review


class AppendOnlyTests(unittest.TestCase):
    def test_register_and_replay_need_no_update_permission_on_audit_ledger(self):
        class AppendOnlyLedger(Ledger):
            def one(self,sql,args=()):
                if 'pipeline_review_events' in sql and 'FOR UPDATE' in sql:
                    raise PermissionError('1142 audit ledger UPDATE prohibited')
                return super().one(sql,args)
        db=AppendOnlyLedger();bundle=approved(db)
        self.assertEqual(review.apply(db,bundle,at=AT)['created'],1)
        self.assertEqual(review.apply(db,bundle,at=AT)['reused'],1)
        with db.transaction():self.assertIsNotNone(review.latest(db,'POLICY','publication'))
