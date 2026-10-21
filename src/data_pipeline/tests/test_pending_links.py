import tempfile
import unittest
import copy
from test_publication_pending import pending,Database
from product_pipeline.publication.runner import publish,audit_publication
from product_pipeline.errors import Conflict


class PendingLinkTests(unittest.TestCase):
    def test_recovery_reuses_committed_staging_without_changing_products_or_approvals(self):
        from product_pipeline.publication.pending import reconcile_links
        with tempfile.TemporaryDirectory() as root:
            plan=pending(root);db=Database();publish(plan,db)
            db.rows['product_link_observations']=[]
            before=copy.deepcopy({k:v for k,v in db.rows.items() if k!='product_link_observations'})
            reconcile_links(plan,db);identities=[r['id'] for r in db.rows['product_link_observations']]
            reconcile_links(plan,db)
            self.assertEqual(identities,[r['id'] for r in db.rows['product_link_observations']])
            self.assertEqual(before,{k:v for k,v in db.rows.items() if k!='product_link_observations'})

    def test_link_observations_do_not_require_user_click_or_product_activation(self):
        with tempfile.TemporaryDirectory() as root:
            plan=pending(root);db=Database();publish(plan,db)
            links=plan['products'][0]['group']['tables']['product_links']
            self.assertTrue(links)
            self.assertEqual(len(db.rows['product_link_observations']),len(links))
            self.assertIsNone(db.rows['products'][0]['comparison_url'])
            self.assertEqual(db.rows['products'][0]['status'],'PENDING_REVIEW')
            self.assertFalse(db.rows['offers']);self.assertFalse(db.rows['offer_snapshots'])
            audit_publication(plan,db)
            db.rows['product_link_observations'][0]['source_url']='https://wrong.invalid/'
            with self.assertRaises(Conflict):audit_publication(plan,db)
