"""Profiling must use existing bounded reads, including failed proof attempts."""
import json
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock,patch

from product_pipeline.delta_proofs import _verify_rows
from product_pipeline.errors import Conflict
from product_pipeline.gold.source import SilverSource
from product_pipeline.silver.source import BronzeSource
from product_pipeline.spark_session import SessionScope


def frame():
    value=MagicMock();value.columns=[]
    for name in ('filter','limit','select','withColumn','orderBy'):
        getattr(value,name).return_value=value
    return value


class DownstreamMetricsTests(unittest.TestCase):
    def test_proof_attempt_records_failure_without_an_extra_storage_action(self):
        for valid in (True,False):
            with self.subTest(valid=valid):
                data=frame();spark=MagicMock()
                spark.read.format.return_value.option.return_value.load.return_value=data
                sink=SimpleNamespace(spark=spark,paths={'proof':'file:///proof'},keys={'proof':'id'},
                    times=set(),decimals=set(),hash_row=lambda row:'expected')
                payload=json.dumps(dict(id='key',record_hash='expected' if valid else 'changed'))
                records=[dict(_table='proof',_version=5,payload=payload)]
                functions=MagicMock()
                with SessionScope() as runtime,patch.dict(sys.modules,{'pyspark.sql':SimpleNamespace(functions=functions)}),\
                        patch('product_pipeline.bounded_read.payload_rows',return_value=records) as reader:
                    if valid:self.assertEqual(_verify_rows(sink,[('proof',5,{'key':'expected'})]),{('proof',5):{'key':'expected'}})
                    else:
                        with self.assertRaises(Conflict):_verify_rows(sink,[('proof',5,{'key':'expected'})])
                    reader.assert_called_once();data.collect.assert_not_called();data.count.assert_not_called()
                    self.assertEqual(runtime.metrics['delta_proof_returned_rows_proof'],1)
                    self.assertEqual(runtime.metrics['delta_proof_read_validate_calls'],1)
                    self.assertGreaterEqual(runtime.metrics['delta_proof_read_validate_ms'],0)

    def test_bronze_page_profiles_the_existing_header_and_payload_actions(self):
        source=BronzeSource.__new__(BronzeSource)
        source.settings=SimpleNamespace(max_records=100,max_raw_bytes=1024)
        source.frame=MagicMock();headers=frame();payloads=frame()
        source.frame.select.return_value=headers;source.frame.filter.return_value=payloads
        headers.collect.return_value=[dict(observation_key='observation',raw_document_id='raw',bytes=7)]
        record=MagicMock();record.__getitem__.return_value='observation';record.asDict.return_value=dict(raw='document')
        payloads.collect.return_value=[record]
        with SessionScope() as runtime,patch.dict(sys.modules,{'pyspark.sql':SimpleNamespace(functions=MagicMock())}),\
                patch('product_pipeline.silver.source.document',side_effect=lambda row:row):
            self.assertEqual(source.page(None),[dict(raw='document')])
            headers.collect.assert_called_once();payloads.collect.assert_called_once()
            source.frame.count.assert_not_called()
            self.assertEqual(runtime.metrics['silver_bronze_selected_raw_bytes'],7)
            self.assertEqual(runtime.metrics['silver_bronze_header_read_calls'],1)
            self.assertEqual(runtime.metrics['silver_bronze_payload_read_calls'],1)

    def test_gold_input_counts_payloads_from_one_bounded_iterator(self):
        source=SilverSource.__new__(SilverSource)
        data=frame();source.frames={'product_observations':data}
        source.settings=SimpleNamespace(max_product_rows=100,max_product_bytes=1024)
        payload=json.dumps(dict(evaluation_key='evaluation',title='상품'),ensure_ascii=False)
        records=[dict(table='product_observations',payload=payload)]
        with SessionScope() as runtime,patch.dict(sys.modules,{'pyspark.sql':SimpleNamespace(functions=MagicMock())}),\
                patch('product_pipeline.bounded_read.payload_rows',return_value=records) as reader:
            self.assertEqual(source._tables_many(['product']),{'product_observations':[json.loads(payload)]})
            reader.assert_called_once();data.collect.assert_not_called();data.count.assert_not_called()
            self.assertEqual(runtime.metrics['gold_silver_input_rows'],1)
            self.assertEqual(runtime.metrics['gold_silver_input_payload_chars_product_observations'],len(payload))
            self.assertEqual(runtime.metrics['silver_union_read_calls'],1)
