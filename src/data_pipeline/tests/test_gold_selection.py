import copy,tempfile,unittest
from unittest.mock import patch
from types import SimpleNamespace
from test_gold import group,reseal
from product_pipeline.gold.selection import resolve,published_choices
from product_pipeline.errors import Conflict
from product_pipeline.contracts import canonical


class HistorySelectionTests(unittest.TestCase):
    def test_frozen_window_discovery_needs_no_spark_query_and_does_not_skip_keys(self):
        from product_pipeline.gold.source import SilverSource
        source=SilverSource.__new__(SilverSource)
        source.settings=SimpleNamespace(max_records=2)
        source._product_keys=['danawa:1','danawa:2','danawa:3']
        self.assertEqual(source.products(),['danawa:1','danawa:2'])
        self.assertEqual(source.products('danawa:2'),['danawa:3'])
        self.assertEqual(source.products('danawa:3'),[])

    def groups(self,directory):
        first=group(directory);other=copy.deepcopy(first)
        key='silver-eval-v1:'+'f'*64
        other['receipt']['evaluation_key']=key
        for rows in other['tables'].values():
            for row in rows:row['evaluation_key']=key
        return first,reseal(other)

    def choice(self,g):
        r=g['receipt']
        return dict(product_key=r['product_key'],raw_document_id=r['raw_document_id'],
            evaluation_key=r['evaluation_key'],document_hash=r['document_hash'],evidence_ref='fixture://completed')

    def test_current_receipt_overrides_proven_previous_without_time_or_hash_order(self):
        with tempfile.TemporaryDirectory() as d:
            a,b=self.groups(d)
            chosen,missing=resolve([b,a],[self.choice(a)],[self.choice(b)])
            self.assertFalse(missing);self.assertEqual(chosen[0]['evaluation_key'],a['receipt']['evaluation_key'])

    def test_historical_ambiguity_requests_supplement_not_arbitrary_choice(self):
        with tempfile.TemporaryDirectory() as d:
            a,b=self.groups(d);chosen,missing=resolve([a,b])
            self.assertEqual(chosen,[]);self.assertEqual(missing[0]['raw_document_id'],a['receipt']['raw_document_id'])
            chosen,missing=resolve([a,b],prior=[self.choice(b)])
            self.assertFalse(missing);self.assertEqual(chosen[0]['evaluation_key'],b['receipt']['evaluation_key'])

    def test_prior_absent_from_pin_is_not_replaced_with_unique_unrelated_receipt(self):
        with tempfile.TemporaryDirectory() as d:
            a,b=self.groups(d);chosen,missing=resolve([a],prior=[self.choice(b)])
            self.assertFalse(chosen);self.assertEqual(missing[0]['reason'],'PRIOR_EVALUATION_NOT_IN_PIN')
            with self.assertRaises(Conflict):resolve([a],current=[self.choice(b)])

    def test_document_conflict_and_tampered_output_are_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            a,b=self.groups(d);choice=dict(self.choice(a),document_hash='bad')
            with self.assertRaises(Conflict):resolve([a],prior=[choice])
            a['tables']['product_observations'][0]['display_name']='tampered'
            with self.assertRaises(Conflict):resolve([a])

    def test_prior_choices_require_audited_completed_publication(self):
        catalog=dict(product_key='danawa:1',source_refs_json=canonical([dict(raw_document_id='raw',
            evaluation_key='eval',document_hash='hash')]))
        ref=canonical(dict(plan_id='plan'))
        db=SimpleNamespace(target='target',get=lambda *a,**k:dict(gold_snapshot_ref_json=ref))
        with patch('product_pipeline.publication.runner.audit_reference',return_value=dict(tables={'product_catalog':[catalog]})) as audit:
            choices=published_choices(db,['danawa:1'])
            audit.assert_called_once_with(db,ref)
            self.assertEqual(choices[0]['document_hash'],'hash')
        with patch('product_pipeline.publication.runner.audit_reference',side_effect=Conflict('NOT_COMPLETED')):
            with self.assertRaises(Conflict):published_choices(db,['danawa:1'])


if __name__=='__main__':unittest.main()
