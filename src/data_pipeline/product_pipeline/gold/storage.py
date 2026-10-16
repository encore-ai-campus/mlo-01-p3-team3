"""Gold schemas reuse the verified insert-only Delta commit implementation."""
from types import SimpleNamespace

from ..silver.delta import DeltaSink as SilverDeltaSink
from ..silver.checkpoint import Checkpoint as SilverCheckpoint
from .contracts import KEYS, semantic_hash

COMMON = 'row_key gold_evaluation_key product_key gold_policy_version processing_run_id processed_at input_fingerprint silver_versions_json record_hash'.split()
EXTRA = {
 'product_catalog':'source_code source_product_id first_collected_at field_status_json display_name manufacturer brand model width_mm depth_mm height_mm service_category_id service_category_code price_krw comparison_url catalog_ready recommendation_ready comparison_link_ready purchase_link_ready representative_image_key embedding_json attributes_json field_lineage_json source_refs_json quality_reason_codes publication_status publication_reason_codes',
 'current_offers':'offer_key condition_fingerprint price_kind amount currency observed_at raw_document_id purchase_url purchase_ready evidence_json',
 'offer_history':'offer_key snapshot_key price_effect_key amount currency observed_at raw_document_id source_evidence_key eligible condition_fingerprint evidence_json',
 'product_images':'image_relation_key raw_document_id source_version image_slot display_order source_url image_checksum bucket object_key recommendation_approved review_snapshot_hash evidence_json',
 'product_links':'link_observation_key link_kind source_url offer_key usable raw_document_id evidence_json',
 'quality_issues':'issue_code field_name evidence_json',
 'quality_daily':'metric_date service_category_id product_count observation_count evaluation_count image_relation_count unique_file_count catalog_ready_count recommendation_ready_count applicable_attribute_count confirmed_applicable_attribute_count applicability_status evidence_json',
 'pipeline_evaluations':'output_manifest_json input_refs_json status reason_codes',
}


def columns(table): return COMMON + EXTRA[table].split()


class DeltaSink(SilverDeltaSink):
    keys, schema_tables, schema_columns = KEYS, EXTRA, staticmethod(columns)
    arrays = {'quality_reason_codes','publication_reason_codes','reason_codes'}
    decimals = {'width_mm','depth_mm','height_mm','price_krw','amount'}
    longs = {'display_order','product_count','observation_count','evaluation_count','image_relation_count',
             'unique_file_count','catalog_ready_count','recommendation_ready_count','applicable_attribute_count','confirmed_applicable_attribute_count'}
    times = {'processed_at','observed_at','first_collected_at'}
    booleans = {'catalog_ready','recommendation_ready','comparison_link_ready','purchase_link_ready',
                'purchase_ready','eligible','recommendation_approved','usable'}
    hash_row = staticmethod(semantic_hash)

    def __init__(self,spark,settings,*,lease):
        # Base writer's path attribute is an internal storage parameter, not a Silver write.
        proxy=SimpleNamespace(silver_root=settings.gold_root,fingerprint=settings.fingerprint)
        class LeaseProxy:
            def check(self,expected=None): lease.check(settings)
        super().__init__(spark,proxy,lease=LeaseProxy())


class Checkpoint(SilverCheckpoint):
    stage, marker_name = 'GOLD', '_gold_target.json'
