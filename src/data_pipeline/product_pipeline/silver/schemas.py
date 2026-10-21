"""Versioned Delta columns; JSON provenance stays canonical, amounts stay exact."""
COMMON='evaluation_key raw_document_id document_hash observation_key source_code source_product_id product_key collection_run_id processing_input_fingerprint normalizer_version quality_policy_version processing_run_id schema_version processed_at observed_at time_status field_status_json evidence_json record_hash'.split()
EXTRA={
 'product_observations':'display_name normalized_name manufacturer brand model source_category_path_json collection_category_context_json service_category_id service_category_code mapping_version mapping_status registered_raw registered_status registered_year registered_month provider_make_raw provider_make_status width_mm depth_mm height_mm dimension_status raw_collected_at',
 'product_attributes':'attribute_key source_evidence_key attribute_name value_type raw_value_json value_text value_decimal value_min value_max codes unit measurement_target component option_key option_evidence_key option_status attribute_status',
 'price_observations':'price_observation_key source_evidence_key price_kind price_basis condition_status amount currency currency_evidence price_text offer_key option_key option_evidence_key condition_fingerprint condition_json numeric_status option_status',
 'product_images':'image_relation_key image_slot source_version source_url role origin display_order observed_url provider_image_version image_checksum bucket object_key asset_ref_json bytes width height format extension acquisition_status availability validation_status image_state_revision image_state_fingerprint image_state_checked_at review_snapshot_hash usage_scope_json review_status',
 'link_observations':'link_observation_key source_evidence_key source_url link_kind structure_status link_status comparison_url offer_key option_evidence_key checked_at expires_at validation_snapshot_hash validation_evidence_json',
 'quality_issues':'issue_key target_table target_key field_name issue_code severity blocking_scope raw_value_json reason',
 'normalized_offers':'normalized_offer_key offer_key source_seller_id source_offer_id option_key identity_rule_version identity_evidence_json price_observation_keys link_observation_keys',
 'pipeline_evaluations':'stage bronze_version bronze_path input_snapshot_refs_json related_hashes_json output_manifest_json status quality_reason_codes completed_at',
}
ARRAYS={'codes','price_observation_keys','link_observation_keys','quality_reason_codes'}
DECIMALS={'width_mm','depth_mm','height_mm','value_decimal','value_min','value_max','amount'}
LONGS={'registered_year','registered_month','display_order','bytes','width','height','image_state_revision','bronze_version'}
TIMES={'processed_at','observed_at','raw_collected_at','image_state_checked_at','checked_at','expires_at','completed_at'}


def columns(table):
    return COMMON+EXTRA[table].split()


def complete(table, row):
    from ..errors import PipelineError
    if set(row)-set(columns(table)):
        raise PipelineError('SILVER_UNKNOWN_OUTPUT_COLUMN')
    return {k:row.get(k,[] if k in ARRAYS else None) for k in columns(table)}
