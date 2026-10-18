"""Gold publication v2 schema; runtime publication never creates/migrates tables.

Ops tables are a proposed extension. Runtime code never creates/migrates tables.
The separate reference DDL is for isolated testing and owner review only.
"""
from ..contracts import digest
from ..errors import PipelineError

def fields(text):
    # name:type[:nullable], types deliberately contain no spaces.
    return {r.split(':')[0]:(r.split(':')[1],r.endswith(':null')) for r in text.split()}

TABLES={
 'categories':fields('id:char(32) code:varchar(100) name:varchar(255) active:tinyint(1) recognition_enabled:tinyint(1)'),
 'products':fields('id:char(32) product_key:varchar(191) name:varchar(500) category_id:char(32) source_code:varchar(32) source_product_id:varchar(100) brand:varchar(255):null model_name:varchar(255):null width_mm:decimal(20,4):null depth_mm:decimal(20,4):null height_mm:decimal(20,4):null comparison_url:text:null status:varchar(32) dedup_status:varchar(32) created_at:datetime(3) updated_at:datetime(3)'),
 'offers':fields('id:char(32) offer_key:varchar(191) product_id:char(32) source_code:varchar(32) source_product_id:varchar(100) source_seller_id:varchar(191) source_offer_id:varchar(191) source_variant_id:varchar(191) title:varchar(500):null price_amount:bigint currency:char(3) purchase_url:text:null availability:varchar(32) collected_at:datetime(3) latest_raw_document_id:char(24) eligible:tinyint(1) price_basis:varchar(32) price_conditions_json:json option_attributes_json:json created_at:datetime(3) updated_at:datetime(3)'),
 'offer_snapshots':fields('id:char(32) snapshot_key:varchar(191) offer_id:char(32) raw_document_id:char(24) price_amount:bigint currency:char(3) purchase_url:text:null availability:varchar(32) collected_at:datetime(3) pricing_terms_json:json created_at:datetime(3)'),
 'assets':fields('id:char(32) owner_id:char(32):null kind:varchar(32) bucket:varchar(100) object_key:varchar(500) content_type:varchar(100) size_bytes:bigint width_px:int height_px:int checksum:char(64) status:varchar(32) usage_policy_json:json created_at:datetime(3) updated_at:datetime(3)'),
 'product_images':fields('id:char(32) product_id:char(32) asset_id:char(32) display_order:int source_code:varchar(32) source_product_id:varchar(100) image_variant:varchar(32) source_url:text source_version:varchar(191) raw_document_id:char(24) raw_payload_hash:char(64) raw_collected_at:datetime(3) linked_at:datetime(3) active:tinyint(1) usage_review_json:json created_at:datetime(3) updated_at:datetime(3)'),
 'pipeline_publication_guards':fields('id:char(32) product_key:varchar(191) revision:bigint guard_json:json'),
 'pipeline_product_versions':fields('id:char(32) product_key:varchar(191) field_versions_json:json first_collected_at:datetime(3):null latest_as_of:datetime(3) latest_evaluation:varchar(191)'),
 'pipeline_publications':fields('plan_id:char(64) id:char(32) plan_hash:char(64) status:varchar(32) gold_versions_json:json product_count:int completed_at:datetime(3):null'),
 'pipeline_publication_staging':fields('id:char(32) plan_id:char(64) product_key:varchar(191) body_hash:char(64) body_json:json'),
}
BASE_COLUMNS = {table: dict(columns) for table, columns in TABLES.items()}
VERSION = 'document-publication-v2'
TABLES['products'].update(fields(
 'manufacturer:varchar(255):null attributes_json:json field_status_json:json field_lineage_json:json '
 'source_refs_json:json price_krw:bigint:null first_collected_at:datetime(3):null '
 'catalog_ready:tinyint(1) recommendation_ready:tinyint(1) comparison_link_ready:tinyint(1) '
 'purchase_link_ready:tinyint(1) quality_reason_codes:json publication_status:varchar(32) '
 'publication_reason_codes:json representative_image_id:char(32):null embedding_json:json gold_snapshot_ref_json:json'))
TABLES['product_images'].update(fields(
 'image_relation_key:varchar(191) gold_recommendation_approved:tinyint(1) '
 'gold_review_snapshot_hash:char(64):null source_evidence_json:json gold_snapshot_ref_json:json'))
for _table in ('offers', 'offer_snapshots'):
    TABLES[_table]['gold_snapshot_ref_json'] = ('json', False)
TABLES['product_link_observations'] = fields(
 'id:char(32) product_id:char(32) link_observation_key:varchar(191) gold_evaluation_key:varchar(191) '
 'link_kind:varchar(32) source_url:text offer_key:varchar(191):null usable:tinyint(1) '
 'raw_document_id:char(24):null evidence_json:json gold_snapshot_ref_json:json created_at:datetime(3)')
TABLES['product_embeddings'] = fields(
 'id:char(32) asset_id:char(32) image_checksum:char(64) model_version:varchar(191) '
 'preprocessing_version:varchar(191) dimension:int vector_ref_json:json status:varchar(32) created_at:datetime(3)')
TABLES['search_index_versions'] = fields(
 'id:char(32) scope:varchar(191) model_version:varchar(191) preprocessing_version:varchar(191) '
 'dimension:int manifest_json:json status:varchar(32) activated_at:datetime(3):null created_at:datetime(3)')
PRIMARY={t:['id'] for t in TABLES}
UNIQUE={'categories':[['code']],'products':[['product_key']],'offers':[['offer_key']],
        'offer_snapshots':[['snapshot_key']],'assets':[['bucket','object_key']],
        'product_images':[['product_id','source_code','source_product_id','image_variant','raw_document_id','source_version']],
        'pipeline_publication_guards':[['product_key']],'pipeline_product_versions':[['product_key']],
        'pipeline_publications':[['plan_id']],'pipeline_publication_staging':[['plan_id','product_key']]}
FKS={'products':[('category_id','categories','id')],'offers':[('product_id','products','id')],
     'offer_snapshots':[('offer_id','offers','id')],'product_images':[('product_id','products','id'),('asset_id','assets','id')],
     'pipeline_publication_staging':[('plan_id','pipeline_publications','plan_id')]}
UNIQUE['product_images'].append(['product_id','image_relation_key'])
UNIQUE['product_link_observations'] = [['product_id','link_observation_key','gold_evaluation_key']]
FKS['products'].append(('representative_image_id','product_images','id'))
FKS['product_link_observations'] = [('product_id','products','id')]
FKS['product_embeddings'] = [('asset_id','assets','id')]
CONTRACT_HASH=digest(dict(tables=TABLES,primary=PRIMARY,unique=UNIQUE,fks=FKS,version=VERSION))


def reference_ddl():
    statements=[]
    for table,columns in TABLES.items():
        parts=['`'+k+'` '+kind+(' NULL' if nullable else ' NOT NULL') for k,(kind,nullable) in columns.items()]
        parts.append('PRIMARY KEY ('+','.join('`'+k+'`' for k in PRIMARY[table])+')')
        for key in UNIQUE.get(table,[]):parts.append('UNIQUE KEY ('+','.join('`'+k+'`' for k in key)+')')
        statements.append('CREATE TABLE `'+table+'` (\n  '+',\n  '.join(parts)+'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;')
    # Products and product_images form a cycle. Add all FKs after creating tables.
    for table, keys in FKS.items():
        for col,parent,pk in keys:
            statements.append(f'ALTER TABLE `{table}` ADD CONSTRAINT `fk_{table}_{col}` '
                f'FOREIGN KEY (`{col}`) REFERENCES `{parent}` (`{pk}`) ON DELETE RESTRICT;')
    return '\n\n'.join(statements)+'\n'


def validate_schema(actual):
    for table,expected in TABLES.items():
        row=actual.get(table)
        if not row or row['engine'].lower()!='innodb':raise PipelineError('MYSQL_CONTRACT_ENGINE_MISMATCH',table)
        for name,(kind,nullable) in expected.items():
            found=row['columns'].get(name)
            if found!=(kind,nullable):raise PipelineError('MYSQL_CONTRACT_COLUMN_MISMATCH',table+'.'+name)
        if row['primary']!=PRIMARY[table]:raise PipelineError('MYSQL_CONTRACT_PK_MISMATCH',table)
        if any(key not in row['unique'] for key in UNIQUE.get(table,[])):raise PipelineError('MYSQL_CONTRACT_UNIQUE_MISMATCH',table)
        if any(tuple(key) not in row['fks'] for key in FKS.get(table,[])):raise PipelineError('MYSQL_CONTRACT_FK_MISMATCH',table)
        if any(rule not in {'RESTRICT','NO ACTION'} for rule in row.get('fk_delete_rules',[])):
            raise PipelineError('MYSQL_CONTRACT_FK_DELETE_RULE',table)
        # Extra required fields without a default would make generic INSERT unsafe.
        extras=set(row['columns'])-set(expected)
        if any(not row['columns'][k][1] and k not in row.get('defaulted',[]) for k in extras):
            raise PipelineError('MYSQL_UNMAPPED_REQUIRED_COLUMN',table)
    return CONTRACT_HASH


COMMON = 'row_key gold_evaluation_key product_key gold_policy_version processing_run_id processed_at input_fingerprint silver_versions_json record_hash'
EXTRA = {'product_catalog': 'source_code source_product_id first_collected_at field_status_json display_name '
                    'manufacturer brand model width_mm depth_mm height_mm service_category_id '
                    'service_category_code price_krw comparison_url catalog_ready recommendation_ready '
                    'comparison_link_ready purchase_link_ready representative_image_key embedding_json '
                    'attributes_json field_lineage_json source_refs_json quality_reason_codes '
                    'publication_status publication_reason_codes',
 'current_offers': 'offer_key condition_fingerprint price_kind amount currency observed_at '
                   'raw_document_id purchase_url purchase_ready evidence_json',
 'offer_history': 'offer_key snapshot_key price_effect_key amount currency observed_at raw_document_id '
                  'source_evidence_key eligible condition_fingerprint evidence_json',
 'product_images': 'image_relation_key raw_document_id source_version image_slot display_order '
                   'source_url image_checksum bucket object_key recommendation_approved '
                   'review_snapshot_hash evidence_json',
 'product_links': 'link_observation_key link_kind source_url offer_key usable raw_document_id '
                  'evidence_json',
 'quality_issues': 'issue_code field_name evidence_json',
 'quality_daily': 'metric_date service_category_id product_count observation_count evaluation_count '
                  'image_relation_count unique_file_count catalog_ready_count '
                  'recommendation_ready_count applicable_attribute_count '
                  'confirmed_applicable_attribute_count applicability_status evidence_json',
 'pipeline_evaluations': 'output_manifest_json input_refs_json status reason_codes'}

DIRECT = {
    'product_catalog': 'source_code source_product_id first_collected_at display_name manufacturer brand model width_mm depth_mm height_mm service_category_id price_krw comparison_url catalog_ready recommendation_ready comparison_link_ready purchase_link_ready attributes_json field_status_json field_lineage_json source_refs_json quality_reason_codes publication_status publication_reason_codes embedding_json',
    'current_offers': 'offer_key amount currency observed_at raw_document_id purchase_url',
    'offer_history': 'snapshot_key amount currency observed_at raw_document_id',
    'product_images': 'image_relation_key source_url source_version image_checksum bucket object_key display_order raw_document_id recommendation_approved review_snapshot_hash evidence_json',
    'product_links': 'link_observation_key link_kind source_url offer_key usable raw_document_id evidence_json',
}

ALIASES = {
    'product_catalog': dict(display_name='products.name',model='products.model_name',
        service_category_id='products.category_id',service_category_code='categories.code',
        representative_image_key='products.representative_image_id -> product_images.image_relation_key'),
    'current_offers': dict(amount='offers.price_amount',observed_at='offers.collected_at',raw_document_id='offers.latest_raw_document_id'),
    'offer_history': dict(amount='offer_snapshots.price_amount',observed_at='offer_snapshots.collected_at'),
    'product_images': dict(image_slot='product_images.image_variant',image_checksum='assets.checksum',bucket='assets.bucket',
        object_key='assets.object_key',recommendation_approved='product_images.gold_recommendation_approved',
        review_snapshot_hash='product_images.gold_review_snapshot_hash',evidence_json='product_images.source_evidence_json'),
}
DESTINATION = dict(product_catalog='products',current_offers='offers',offer_history='offer_snapshots',
                   product_images='product_images',product_links='product_link_observations')

def dispositions():
    from ..gold.storage import columns, EXTRA as actual
    if set(actual)!=set(EXTRA):raise PipelineError('PUBLICATION_UNCLASSIFIED_GOLD_TABLE')
    result={}
    for table, fields in EXTRA.items():
        expected=set((COMMON+' '+fields).split())
        if set(columns(table))!=expected:raise PipelineError('PUBLICATION_UNCLASSIFIED_GOLD_COLUMN',table)
        result[table]={}
        for field in sorted(expected):
            raw=f'pipeline_publication_staging.body_json.group.tables.{table}[].{field}'
            if table=='pipeline_evaluations':raw=f'pipeline_publication_staging.body_json.group.receipt.{field}'
            target=ALIASES.get(table,{}).get(field)
            if target is None and field in DIRECT.get(table,'').split():target=DESTINATION[table]+'.'+field
            result[table][field]=dict(service=target,immutable_source=raw,
                policy='SERVICE_PROJECTION_AND_SOURCE' if target else 'SOURCE_REFERENCE_ONLY',
                reason=None if target else 'Fixed Gold/source evidence; no separate service column in the publication contract')
    return result

def validate_columns(group):
    contract=dispositions()
    for table, rows in dict(group['tables'],pipeline_evaluations=[group['receipt']]).items():
        for row in rows:
            if set(row)!=set(contract[table]):raise PipelineError('PUBLICATION_GOLD_COLUMN_SET_MISMATCH',table)
