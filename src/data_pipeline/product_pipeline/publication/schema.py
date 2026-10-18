"""Document-derived v1 reference contract, NOT a claim about an existing service DB.

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
PRIMARY={t:['id'] for t in TABLES}
UNIQUE={'categories':[['code']],'products':[['product_key']],'offers':[['offer_key']],
        'offer_snapshots':[['snapshot_key']],'assets':[['bucket','object_key']],
        'product_images':[['product_id','source_code','source_product_id','image_variant','raw_document_id','source_version']],
        'pipeline_publication_guards':[['product_key']],'pipeline_product_versions':[['product_key']],
        'pipeline_publications':[['plan_id']],'pipeline_publication_staging':[['plan_id','product_key']]}
FKS={'products':[('category_id','categories','id')],'offers':[('product_id','products','id')],
     'offer_snapshots':[('offer_id','offers','id')],'product_images':[('product_id','products','id'),('asset_id','assets','id')],
     'pipeline_publication_staging':[('plan_id','pipeline_publications','plan_id')]}
CONTRACT_HASH=digest(dict(tables=TABLES,primary=PRIMARY,unique=UNIQUE,fks=FKS,version='document-publication-v1'))


def reference_ddl():
    statements=[]
    for table,columns in TABLES.items():
        parts=['`'+k+'` '+kind+(' NULL' if nullable else ' NOT NULL') for k,(kind,nullable) in columns.items()]
        parts.append('PRIMARY KEY ('+','.join('`'+k+'`' for k in PRIMARY[table])+')')
        for key in UNIQUE.get(table,[]):parts.append('UNIQUE KEY ('+','.join('`'+k+'`' for k in key)+')')
        for col,parent,pk in FKS.get(table,[]):parts.append(f'FOREIGN KEY (`{col}`) REFERENCES `{parent}` (`{pk}`)')
        statements.append('CREATE TABLE `'+table+'` (\n  '+',\n  '.join(parts)+'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;')
    return '\n\n'.join(statements)+'\n'


def validate_schema(actual):
    for table,expected in TABLES.items():
        row=actual.get(table)
        if not row or row['engine'].lower()!='innodb':raise PipelineError('MYSQL_CONTRACT_ENGINE_MISMATCH',table)
        for name,(kind,nullable) in expected.items():
            if row['columns'].get(name)!=(kind,nullable):raise PipelineError('MYSQL_CONTRACT_COLUMN_MISMATCH',table+'.'+name)
        if row['primary']!=PRIMARY[table]:raise PipelineError('MYSQL_CONTRACT_PK_MISMATCH',table)
        if any(key not in row['unique'] for key in UNIQUE.get(table,[])):raise PipelineError('MYSQL_CONTRACT_UNIQUE_MISMATCH',table)
        if any(tuple(key) not in row['fks'] for key in FKS.get(table,[])):raise PipelineError('MYSQL_CONTRACT_FK_MISMATCH',table)
        # Extra required fields without a default would make generic INSERT unsafe.
        extras=set(row['columns'])-set(expected)
        if any(not row['columns'][k][1] and k not in row.get('defaulted',[]) for k in extras):
            raise PipelineError('MYSQL_UNMAPPED_REQUIRED_COLUMN',table)
    return CONTRACT_HASH
