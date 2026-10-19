-- feature25: schema only; stop application/publication writes before running.
-- Existing populated v1 rows require real Gold backfill; no placeholder data is written.
-- Keep _feature25_schema_history until rollback is no longer needed.
SET SESSION autocommit=1;
SET SESSION time_zone='+00:00';
SET SESSION lock_wait_timeout=30;
SET SESSION innodb_lock_wait_timeout=30;
CREATE TEMPORARY TABLE f25_columns (t VARCHAR(64),c VARCHAR(64),ty VARCHAR(64),n VARCHAR(3),b TINYINT);
INSERT INTO f25_columns VALUES
('categories','id','char(32)','NO',1),
('categories','code','varchar(100)','NO',1),
('categories','name','varchar(255)','NO',1),
('categories','active','tinyint(1)','NO',1),
('categories','recognition_enabled','tinyint(1)','NO',1),
('products','id','char(32)','NO',1),
('products','product_key','varchar(191)','NO',1),
('products','name','varchar(500)','NO',1),
('products','category_id','char(32)','NO',1),
('products','source_code','varchar(32)','NO',1),
('products','source_product_id','varchar(100)','NO',1),
('products','brand','varchar(255)','YES',1),
('products','model_name','varchar(255)','YES',1),
('products','width_mm','decimal(20,4)','YES',1),
('products','depth_mm','decimal(20,4)','YES',1),
('products','height_mm','decimal(20,4)','YES',1),
('products','comparison_url','text','YES',1),
('products','status','varchar(32)','NO',1),
('products','dedup_status','varchar(32)','NO',1),
('products','created_at','datetime(3)','NO',1),
('products','updated_at','datetime(3)','NO',1),
('products','manufacturer','varchar(255)','YES',0),
('products','attributes_json','json','NO',0),
('products','field_status_json','json','NO',0),
('products','field_lineage_json','json','NO',0),
('products','source_refs_json','json','NO',0),
('products','price_krw','bigint','YES',0),
('products','first_collected_at','datetime(3)','YES',0),
('products','catalog_ready','tinyint(1)','NO',0),
('products','recommendation_ready','tinyint(1)','NO',0),
('products','comparison_link_ready','tinyint(1)','NO',0),
('products','purchase_link_ready','tinyint(1)','NO',0),
('products','quality_reason_codes','json','NO',0),
('products','publication_status','varchar(32)','NO',0),
('products','publication_reason_codes','json','NO',0),
('products','representative_image_id','char(32)','YES',0),
('products','embedding_json','json','NO',0),
('products','gold_snapshot_ref_json','json','NO',0),
('offers','id','char(32)','NO',1),
('offers','offer_key','varchar(191)','NO',1),
('offers','product_id','char(32)','NO',1),
('offers','source_code','varchar(32)','NO',1),
('offers','source_product_id','varchar(100)','NO',1),
('offers','source_seller_id','varchar(191)','NO',1),
('offers','source_offer_id','varchar(191)','NO',1),
('offers','source_variant_id','varchar(191)','NO',1),
('offers','title','varchar(500)','YES',1),
('offers','price_amount','bigint','NO',1),
('offers','currency','char(3)','NO',1),
('offers','purchase_url','text','YES',1),
('offers','availability','varchar(32)','NO',1),
('offers','collected_at','datetime(3)','NO',1),
('offers','latest_raw_document_id','char(24)','NO',1),
('offers','eligible','tinyint(1)','NO',1),
('offers','price_basis','varchar(32)','NO',1),
('offers','price_conditions_json','json','NO',1),
('offers','option_attributes_json','json','NO',1),
('offers','created_at','datetime(3)','NO',1),
('offers','updated_at','datetime(3)','NO',1),
('offers','gold_snapshot_ref_json','json','NO',0),
('offer_snapshots','id','char(32)','NO',1),
('offer_snapshots','snapshot_key','varchar(191)','NO',1),
('offer_snapshots','offer_id','char(32)','NO',1),
('offer_snapshots','raw_document_id','char(24)','NO',1),
('offer_snapshots','price_amount','bigint','NO',1),
('offer_snapshots','currency','char(3)','NO',1),
('offer_snapshots','purchase_url','text','YES',1),
('offer_snapshots','availability','varchar(32)','NO',1),
('offer_snapshots','collected_at','datetime(3)','NO',1),
('offer_snapshots','pricing_terms_json','json','NO',1),
('offer_snapshots','created_at','datetime(3)','NO',1),
('offer_snapshots','gold_snapshot_ref_json','json','NO',0),
('assets','id','char(32)','NO',1),
('assets','owner_id','char(32)','YES',1),
('assets','kind','varchar(32)','NO',1),
('assets','bucket','varchar(100)','NO',1),
('assets','object_key','varchar(500)','NO',1),
('assets','content_type','varchar(100)','NO',1),
('assets','size_bytes','bigint','NO',1),
('assets','width_px','int','NO',1),
('assets','height_px','int','NO',1),
('assets','checksum','char(64)','NO',1),
('assets','status','varchar(32)','NO',1),
('assets','usage_policy_json','json','NO',1),
('assets','created_at','datetime(3)','NO',1),
('assets','updated_at','datetime(3)','NO',1),
('product_images','id','char(32)','NO',1),
('product_images','product_id','char(32)','NO',1),
('product_images','asset_id','char(32)','NO',1),
('product_images','display_order','int','NO',1),
('product_images','source_code','varchar(32)','NO',1),
('product_images','source_product_id','varchar(100)','NO',1),
('product_images','image_variant','varchar(32)','NO',1),
('product_images','source_url','text','NO',1),
('product_images','source_version','varchar(191)','NO',1),
('product_images','raw_document_id','char(24)','NO',1),
('product_images','raw_payload_hash','char(64)','NO',1),
('product_images','raw_collected_at','datetime(3)','NO',1),
('product_images','linked_at','datetime(3)','NO',1),
('product_images','active','tinyint(1)','NO',1),
('product_images','usage_review_json','json','NO',1),
('product_images','created_at','datetime(3)','NO',1),
('product_images','updated_at','datetime(3)','NO',1),
('product_images','image_relation_key','varchar(191)','NO',0),
('product_images','gold_recommendation_approved','tinyint(1)','NO',0),
('product_images','gold_review_snapshot_hash','char(64)','YES',0),
('product_images','source_evidence_json','json','NO',0),
('product_images','gold_snapshot_ref_json','json','NO',0),
('pipeline_publication_guards','id','char(32)','NO',1),
('pipeline_publication_guards','product_key','varchar(191)','NO',1),
('pipeline_publication_guards','revision','bigint','NO',1),
('pipeline_publication_guards','guard_json','json','NO',1),
('pipeline_product_versions','id','char(32)','NO',1),
('pipeline_product_versions','product_key','varchar(191)','NO',1),
('pipeline_product_versions','field_versions_json','json','NO',1),
('pipeline_product_versions','first_collected_at','datetime(3)','YES',1),
('pipeline_product_versions','latest_as_of','datetime(3)','NO',1),
('pipeline_product_versions','latest_evaluation','varchar(191)','NO',1),
('pipeline_publications','plan_id','char(64)','NO',1),
('pipeline_publications','id','char(32)','NO',1),
('pipeline_publications','plan_hash','char(64)','NO',1),
('pipeline_publications','status','varchar(32)','NO',1),
('pipeline_publications','gold_versions_json','json','NO',1),
('pipeline_publications','product_count','int','NO',1),
('pipeline_publications','completed_at','datetime(3)','YES',1),
('pipeline_publication_staging','id','char(32)','NO',1),
('pipeline_publication_staging','plan_id','char(64)','NO',1),
('pipeline_publication_staging','product_key','varchar(191)','NO',1),
('pipeline_publication_staging','body_hash','char(64)','NO',1),
('pipeline_publication_staging','body_json','json','NO',1),
('product_link_observations','id','char(32)','NO',0),
('product_link_observations','product_id','char(32)','NO',0),
('product_link_observations','link_observation_key','varchar(191)','NO',0),
('product_link_observations','gold_evaluation_key','varchar(191)','NO',0),
('product_link_observations','link_kind','varchar(32)','NO',0),
('product_link_observations','source_url','text','NO',0),
('product_link_observations','offer_key','varchar(191)','YES',0),
('product_link_observations','usable','tinyint(1)','NO',0),
('product_link_observations','raw_document_id','char(24)','YES',0),
('product_link_observations','evidence_json','json','NO',0),
('product_link_observations','gold_snapshot_ref_json','json','NO',0),
('product_link_observations','created_at','datetime(3)','NO',0),
('product_embeddings','id','char(32)','NO',0),
('product_embeddings','asset_id','char(32)','NO',0),
('product_embeddings','image_checksum','char(64)','NO',0),
('product_embeddings','model_version','varchar(191)','NO',0),
('product_embeddings','preprocessing_version','varchar(191)','NO',0),
('product_embeddings','dimension','int','NO',0),
('product_embeddings','vector_ref_json','json','NO',0),
('product_embeddings','status','varchar(32)','NO',0),
('product_embeddings','created_at','datetime(3)','NO',0),
('search_index_versions','id','char(32)','NO',0),
('search_index_versions','scope','varchar(191)','NO',0),
('search_index_versions','model_version','varchar(191)','NO',0),
('search_index_versions','preprocessing_version','varchar(191)','NO',0),
('search_index_versions','dimension','int','NO',0),
('search_index_versions','manifest_json','json','NO',0),
('search_index_versions','status','varchar(32)','NO',0),
('search_index_versions','activated_at','datetime(3)','YES',0),
('search_index_versions','created_at','datetime(3)','NO',0);
CREATE TEMPORARY TABLE f25_keys (t VARCHAR(64),kind VARCHAR(10),cols VARCHAR(1000));
INSERT INTO f25_keys VALUES
('categories','PRIMARY','id'),
('products','PRIMARY','id'),
('offers','PRIMARY','id'),
('offer_snapshots','PRIMARY','id'),
('assets','PRIMARY','id'),
('product_images','PRIMARY','id'),
('pipeline_publication_guards','PRIMARY','id'),
('pipeline_product_versions','PRIMARY','id'),
('pipeline_publications','PRIMARY','id'),
('pipeline_publication_staging','PRIMARY','id'),
('product_link_observations','PRIMARY','id'),
('product_embeddings','PRIMARY','id'),
('search_index_versions','PRIMARY','id'),
('categories','UNIQUE','code'),
('products','UNIQUE','product_key'),
('offers','UNIQUE','offer_key'),
('offer_snapshots','UNIQUE','snapshot_key'),
('assets','UNIQUE','bucket,object_key'),
('product_images','UNIQUE','product_id,source_code,source_product_id,image_variant,raw_document_id,source_version'),
('product_images','UNIQUE','product_id,image_relation_key'),
('pipeline_publication_guards','UNIQUE','product_key'),
('pipeline_product_versions','UNIQUE','product_key'),
('pipeline_publications','UNIQUE','plan_id'),
('pipeline_publication_staging','UNIQUE','plan_id,product_key'),
('product_link_observations','UNIQUE','product_id,link_observation_key,gold_evaluation_key');
CREATE TEMPORARY TABLE f25_fks (t VARCHAR(64),c VARCHAR(64),p VARCHAR(64),pk VARCHAR(64));
INSERT INTO f25_fks VALUES
('products','category_id','categories','id'),
('products','representative_image_id','product_images','id'),
('offers','product_id','products','id'),
('offer_snapshots','offer_id','offers','id'),
('product_images','product_id','products','id'),
('product_images','asset_id','assets','id'),
('pipeline_publication_staging','plan_id','pipeline_publications','plan_id'),
('product_link_observations','product_id','products','id'),
('product_embeddings','asset_id','assets','id');

DROP PROCEDURE IF EXISTS `feature25_apply_b09eee8814c9`;
DELIMITER $$
CREATE PROCEDURE `feature25_apply_b09eee8814c9`()
COMMENT 'feature25-schema-helper-v1'
BEGIN
 DECLARE problems INT DEFAULT 0;
 DECLARE saved JSON;
 DECLARE current_shape JSON;
 DECLARE previous_status VARCHAR(32);
 DECLARE lock_name VARCHAR(64);
 DECLARE owns_lock BOOLEAN DEFAULT FALSE;
 DECLARE journal_started BOOLEAN DEFAULT FALSE;
 DECLARE EXIT HANDLER FOR SQLEXCEPTION
 BEGIN
  IF journal_started THEN
   SELECT JSON_OBJECT(
 'tables',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',TABLE_NAME,'engine',ENGINE,'collation',TABLE_COLLATION,'comment',TABLE_COMMENT)),JSON_ARRAY())
 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'columns',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',TABLE_NAME,'c',COLUMN_NAME,'ty',COLUMN_TYPE,'n',IS_NULLABLE,'d',COLUMN_DEFAULT,'extra',EXTRA,'cs',CHARACTER_SET_NAME,'collation',COLLATION_NAME,'pos',ORDINAL_POSITION)),JSON_ARRAY())
 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'indexes',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',TABLE_NAME,'name',INDEX_NAME,'c',COLUMN_NAME,'seq',SEQ_IN_INDEX,'nu',NON_UNIQUE,'prefix',SUB_PART,'kind',INDEX_TYPE,'direction',COLLATION,'visible',IS_VISIBLE,'expression',EXPRESSION)),JSON_ARRAY())
 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'fks',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',k.TABLE_NAME,'name',k.CONSTRAINT_NAME,'c',k.COLUMN_NAME,'seq',k.ORDINAL_POSITION,'ps',k.REFERENCED_TABLE_SCHEMA,'p',k.REFERENCED_TABLE_NAME,'pk',k.REFERENCED_COLUMN_NAME,'del',r.DELETE_RULE,'upd',r.UPDATE_RULE)),JSON_ARRAY())
 FROM information_schema.KEY_COLUMN_USAGE k JOIN information_schema.REFERENTIAL_CONSTRAINTS r
 ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA AND r.TABLE_NAME=k.TABLE_NAME AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME
 WHERE k.TABLE_SCHEMA=DATABASE() AND k.TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions') AND k.REFERENCED_TABLE_NAME IS NOT NULL),
 'triggers',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',EVENT_OBJECT_TABLE,'name',TRIGGER_NAME,'event',EVENT_MANIPULATION,'timing',ACTION_TIMING,'statement',ACTION_STATEMENT,'definer',DEFINER)),JSON_ARRAY())
 FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=DATABASE() AND EVENT_OBJECT_TABLE IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'checks',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',t.TABLE_NAME,'name',t.CONSTRAINT_NAME,'clause',c.CHECK_CLAUSE,'enforced',t.ENFORCED)),JSON_ARRAY())
 FROM information_schema.TABLE_CONSTRAINTS t JOIN information_schema.CHECK_CONSTRAINTS c
 ON c.CONSTRAINT_SCHEMA=t.CONSTRAINT_SCHEMA AND c.CONSTRAINT_NAME=t.CONSTRAINT_NAME
 WHERE t.TABLE_SCHEMA=DATABASE() AND t.TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions') AND t.CONSTRAINT_TYPE='CHECK')
) INTO current_shape;
   UPDATE `_feature25_schema_history` SET status='APPLY_FAILED',after_schema_json=current_shape,updated_at=UTC_TIMESTAMP(3)
   WHERE migration_key='feature25-publication-v2-b09eee8814c9';
  END IF;
  IF owns_lock THEN DO RELEASE_LOCK(lock_name); END IF;
  RESIGNAL;
 END;
 IF DATABASE() IS NULL OR DATABASE() IN ('mysql','sys','information_schema','performance_schema') THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='SELECT_SERVICE_DATABASE_WITH_DATABASE_OPTION'; END IF;
 SET lock_name=CONCAT('feature25:',LEFT(SHA2(DATABASE(),256),40));
 IF NOT (GET_LOCK(lock_name,10)<=>1) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='MIGRATION_ALREADY_RUNNING'; END IF;
 SET owns_lock=TRUE;
 IF EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='_feature25_schema_history') THEN
 IF NOT EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='_feature25_schema_history'
 AND ENGINE='InnoDB' AND TABLE_COMMENT='feature25-schema-journal-v1') OR
 (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='_feature25_schema_history'
 AND COLUMN_NAME IN ('migration_key','format_version','schema_hash','status','before_schema_json','after_schema_json','created_at','updated_at'))<>8 THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='JOURNAL_NAME_COLLISION_REVIEW_REQUIRED';
 END IF;
END IF;
 IF @@version NOT REGEXP '^(8|9)[.]' OR @@global.read_only OR @@global.super_read_only THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='WRITABLE_MYSQL_8_OR_9_PRIMARY_REQUIRED';
 END IF;
 SELECT COUNT(*) INTO problems FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()
 AND TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging') AND ENGINE='InnoDB';
 IF problems<>10 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='BASE_10_INNODB_TABLES_REQUIRED'; END IF;
 SELECT COUNT(*) INTO problems FROM f25_columns e JOIN information_schema.COLUMNS c
 ON c.TABLE_SCHEMA=DATABASE() AND c.TABLE_NAME=e.t AND c.COLUMN_NAME=e.c
 WHERE LOWER(c.COLUMN_TYPE)<>e.ty OR c.IS_NULLABLE<>e.n;
 IF problems<>0 THEN
  SELECT e.t,e.c,e.ty AS expected_type,e.n AS expected_nullable,c.COLUMN_TYPE,c.IS_NULLABLE
  FROM f25_columns e JOIN information_schema.COLUMNS c
  ON c.TABLE_SCHEMA=DATABASE() AND c.TABLE_NAME=e.t AND c.COLUMN_NAME=e.c
  WHERE LOWER(c.COLUMN_TYPE)<>e.ty OR c.IS_NULLABLE<>e.n;
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='EXISTING_COLUMN_DIFFERS_NO_TABLE_DDL_APPLIED';
 END IF;
 SELECT COUNT(*) INTO problems FROM f25_columns e LEFT JOIN information_schema.COLUMNS c
 ON c.TABLE_SCHEMA=DATABASE() AND c.TABLE_NAME=e.t AND c.COLUMN_NAME=e.c WHERE c.COLUMN_NAME IS NULL AND (e.b=1 OR e.t IN ('product_link_observations','product_embeddings','search_index_versions') AND EXISTS (
 SELECT 1 FROM information_schema.TABLES t WHERE t.TABLE_SCHEMA=DATABASE() AND t.TABLE_NAME=e.t));
 IF problems<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='EXISTING_TABLE_INCOMPLETE_NO_TABLE_DDL_APPLIED'; END IF;
 SELECT COUNT(*) INTO problems FROM information_schema.TABLES t
 WHERE t.TABLE_SCHEMA=DATABASE() AND t.TABLE_NAME IN (SELECT DISTINCT e.t FROM f25_columns e)
 AND (t.ENGINE IS NULL OR t.ENGINE<>'InnoDB');
 IF problems<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='INNODB_REQUIRED_NO_TABLE_DDL_APPLIED'; END IF;
 SELECT COUNT(*) INTO problems FROM information_schema.COLUMNS c LEFT JOIN f25_columns e
 ON e.t=c.TABLE_NAME AND e.c=c.COLUMN_NAME
 WHERE c.TABLE_SCHEMA=DATABASE() AND c.TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')
 AND e.c IS NULL AND c.IS_NULLABLE='NO' AND c.COLUMN_DEFAULT IS NULL
 AND c.EXTRA NOT LIKE '%GENERATED%' AND c.EXTRA NOT LIKE '%auto_increment%';
 IF problems<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='UNMAPPED_REQUIRED_COLUMN_NO_TABLE_DDL_APPLIED'; END IF;
 SELECT COUNT(*) INTO problems FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('products','product_images','assets') AND COLUMN_NAME='id'
 AND (CHARACTER_SET_NAME<>'utf8mb4' OR COLLATION_NAME<>'utf8mb4_bin');
 IF problems<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='UUID_COLLATION_DIFFERS_NO_TABLE_DDL_APPLIED'; END IF;
 IF EXISTS (SELECT 1 FROM `products` LIMIT 1) AND
 (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products'
 AND COLUMN_NAME IN ('attributes_json','field_status_json','field_lineage_json','source_refs_json','catalog_ready','recommendation_ready','comparison_link_ready','purchase_link_ready','quality_reason_codes','publication_status','publication_reason_codes','embedding_json','gold_snapshot_ref_json'))<>13 THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='BACKFILL_REQUIRED_PRODUCTS_NO_TABLE_DDL_APPLIED';
 END IF;
 IF EXISTS (SELECT 1 FROM `product_images` LIMIT 1) AND
 (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images'
 AND COLUMN_NAME IN ('image_relation_key','gold_recommendation_approved','source_evidence_json','gold_snapshot_ref_json'))<>4 THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='BACKFILL_REQUIRED_PRODUCT_IMAGES_NO_TABLE_DDL_APPLIED';
 END IF;
 IF EXISTS (SELECT 1 FROM `offers` LIMIT 1) AND
 (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offers'
 AND COLUMN_NAME IN ('gold_snapshot_ref_json'))<>1 THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='BACKFILL_REQUIRED_OFFERS_NO_TABLE_DDL_APPLIED';
 END IF;
 IF EXISTS (SELECT 1 FROM `offer_snapshots` LIMIT 1) AND
 (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offer_snapshots'
 AND COLUMN_NAME IN ('gold_snapshot_ref_json'))<>1 THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='BACKFILL_REQUIRED_OFFER_SNAPSHOTS_NO_TABLE_DDL_APPLIED';
 END IF;
 SELECT COUNT(*) INTO problems FROM f25_fks e JOIN information_schema.KEY_COLUMN_USAGE k
 ON k.TABLE_SCHEMA=DATABASE() AND k.TABLE_NAME=e.t AND k.COLUMN_NAME=e.c
 JOIN information_schema.REFERENTIAL_CONSTRAINTS r
 ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA AND r.TABLE_NAME=k.TABLE_NAME AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME
 WHERE k.REFERENCED_TABLE_NAME IS NOT NULL AND (k.REFERENCED_TABLE_SCHEMA<>DATABASE()
 OR k.REFERENCED_TABLE_NAME<>e.p OR k.REFERENCED_COLUMN_NAME<>e.pk OR r.DELETE_RULE NOT IN ('RESTRICT','NO ACTION'));
 IF problems<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='EXISTING_FK_DIFFERS_NO_TABLE_DDL_APPLIED'; END IF;
 CREATE TABLE IF NOT EXISTS `_feature25_schema_history` (
 migration_key VARCHAR(64) NOT NULL PRIMARY KEY,
 format_version INT NOT NULL,
 schema_hash CHAR(64) NOT NULL,
 status VARCHAR(32) NOT NULL,
 before_schema_json JSON NOT NULL,
 after_schema_json JSON NULL,
 created_at DATETIME(3) NOT NULL,
 updated_at DATETIME(3) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='feature25-schema-journal-v1';
 SELECT status INTO previous_status FROM `_feature25_schema_history` WHERE migration_key='feature25-publication-v2-b09eee8814c9';
 IF previous_status IS NOT NULL AND NOT EXISTS (SELECT 1 FROM `_feature25_schema_history`
 WHERE migration_key='feature25-publication-v2-b09eee8814c9' AND format_version=1 AND schema_hash='b09eee8814c9f602b55a1429f8883f6ccf11c955f4dda319384d065dab444b0a') THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='JOURNAL_VERSION_MISMATCH'; END IF;
 IF previous_status IS NULL OR previous_status='ROLLED_BACK' THEN
  SELECT JSON_OBJECT(
 'tables',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',TABLE_NAME,'engine',ENGINE,'collation',TABLE_COLLATION,'comment',TABLE_COMMENT)),JSON_ARRAY())
 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'columns',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',TABLE_NAME,'c',COLUMN_NAME,'ty',COLUMN_TYPE,'n',IS_NULLABLE,'d',COLUMN_DEFAULT,'extra',EXTRA,'cs',CHARACTER_SET_NAME,'collation',COLLATION_NAME,'pos',ORDINAL_POSITION)),JSON_ARRAY())
 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'indexes',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',TABLE_NAME,'name',INDEX_NAME,'c',COLUMN_NAME,'seq',SEQ_IN_INDEX,'nu',NON_UNIQUE,'prefix',SUB_PART,'kind',INDEX_TYPE,'direction',COLLATION,'visible',IS_VISIBLE,'expression',EXPRESSION)),JSON_ARRAY())
 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'fks',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',k.TABLE_NAME,'name',k.CONSTRAINT_NAME,'c',k.COLUMN_NAME,'seq',k.ORDINAL_POSITION,'ps',k.REFERENCED_TABLE_SCHEMA,'p',k.REFERENCED_TABLE_NAME,'pk',k.REFERENCED_COLUMN_NAME,'del',r.DELETE_RULE,'upd',r.UPDATE_RULE)),JSON_ARRAY())
 FROM information_schema.KEY_COLUMN_USAGE k JOIN information_schema.REFERENTIAL_CONSTRAINTS r
 ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA AND r.TABLE_NAME=k.TABLE_NAME AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME
 WHERE k.TABLE_SCHEMA=DATABASE() AND k.TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions') AND k.REFERENCED_TABLE_NAME IS NOT NULL),
 'triggers',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',EVENT_OBJECT_TABLE,'name',TRIGGER_NAME,'event',EVENT_MANIPULATION,'timing',ACTION_TIMING,'statement',ACTION_STATEMENT,'definer',DEFINER)),JSON_ARRAY())
 FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=DATABASE() AND EVENT_OBJECT_TABLE IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'checks',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',t.TABLE_NAME,'name',t.CONSTRAINT_NAME,'clause',c.CHECK_CLAUSE,'enforced',t.ENFORCED)),JSON_ARRAY())
 FROM information_schema.TABLE_CONSTRAINTS t JOIN information_schema.CHECK_CONSTRAINTS c
 ON c.CONSTRAINT_SCHEMA=t.CONSTRAINT_SCHEMA AND c.CONSTRAINT_NAME=t.CONSTRAINT_NAME
 WHERE t.TABLE_SCHEMA=DATABASE() AND t.TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions') AND t.CONSTRAINT_TYPE='CHECK')
) INTO current_shape;
  INSERT INTO `_feature25_schema_history` (migration_key,format_version,schema_hash,status,before_schema_json,after_schema_json,created_at,updated_at)
  VALUES ('feature25-publication-v2-b09eee8814c9',1,'b09eee8814c9f602b55a1429f8883f6ccf11c955f4dda319384d065dab444b0a','APPLYING',current_shape,current_shape,UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))
  ON DUPLICATE KEY UPDATE status='APPLYING',before_schema_json=VALUES(before_schema_json),
  after_schema_json=VALUES(after_schema_json),created_at=VALUES(created_at),updated_at=VALUES(updated_at);
 END IF;
 SET journal_started=TRUE;
 UPDATE `_feature25_schema_history` SET status='APPLYING',updated_at=UTC_TIMESTAMP(3) WHERE migration_key='feature25-publication-v2-b09eee8814c9';

 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='manufacturer') THEN
  ALTER TABLE `products` ADD COLUMN `manufacturer` VARCHAR(255) NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='attributes_json') THEN
  ALTER TABLE `products` ADD COLUMN `attributes_json` JSON NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='field_status_json') THEN
  ALTER TABLE `products` ADD COLUMN `field_status_json` JSON NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='field_lineage_json') THEN
  ALTER TABLE `products` ADD COLUMN `field_lineage_json` JSON NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='source_refs_json') THEN
  ALTER TABLE `products` ADD COLUMN `source_refs_json` JSON NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='price_krw') THEN
  ALTER TABLE `products` ADD COLUMN `price_krw` BIGINT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='first_collected_at') THEN
  ALTER TABLE `products` ADD COLUMN `first_collected_at` DATETIME(3) NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='catalog_ready') THEN
  ALTER TABLE `products` ADD COLUMN `catalog_ready` TINYINT(1) NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='recommendation_ready') THEN
  ALTER TABLE `products` ADD COLUMN `recommendation_ready` TINYINT(1) NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='comparison_link_ready') THEN
  ALTER TABLE `products` ADD COLUMN `comparison_link_ready` TINYINT(1) NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='purchase_link_ready') THEN
  ALTER TABLE `products` ADD COLUMN `purchase_link_ready` TINYINT(1) NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='quality_reason_codes') THEN
  ALTER TABLE `products` ADD COLUMN `quality_reason_codes` JSON NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='publication_status') THEN
  ALTER TABLE `products` ADD COLUMN `publication_status` VARCHAR(32) NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='publication_reason_codes') THEN
  ALTER TABLE `products` ADD COLUMN `publication_reason_codes` JSON NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='representative_image_id') THEN
  ALTER TABLE `products` ADD COLUMN `representative_image_id` CHAR(32) NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='embedding_json') THEN
  ALTER TABLE `products` ADD COLUMN `embedding_json` JSON NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
  ALTER TABLE `products` ADD COLUMN `gold_snapshot_ref_json` JSON NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offers' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
  ALTER TABLE `offers` ADD COLUMN `gold_snapshot_ref_json` JSON NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offer_snapshots' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
  ALTER TABLE `offer_snapshots` ADD COLUMN `gold_snapshot_ref_json` JSON NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='image_relation_key') THEN
  ALTER TABLE `product_images` ADD COLUMN `image_relation_key` VARCHAR(191) NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='gold_recommendation_approved') THEN
  ALTER TABLE `product_images` ADD COLUMN `gold_recommendation_approved` TINYINT(1) NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='gold_review_snapshot_hash') THEN
  ALTER TABLE `product_images` ADD COLUMN `gold_review_snapshot_hash` CHAR(64) NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='source_evidence_json') THEN
  ALTER TABLE `product_images` ADD COLUMN `source_evidence_json` JSON NOT NULL;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
  ALTER TABLE `product_images` ADD COLUMN `gold_snapshot_ref_json` JSON NOT NULL;
 END IF;
CREATE TABLE IF NOT EXISTS `product_link_observations` (
  `id` char(32) NOT NULL,
  `product_id` char(32) NOT NULL,
  `link_observation_key` varchar(191) NOT NULL,
  `gold_evaluation_key` varchar(191) NOT NULL,
  `link_kind` varchar(32) NOT NULL,
  `source_url` text NOT NULL,
  `offer_key` varchar(191) NULL,
  `usable` tinyint(1) NOT NULL,
  `raw_document_id` char(24) NULL,
  `evidence_json` json NOT NULL,
  `gold_snapshot_ref_json` json NOT NULL,
  `created_at` datetime(3) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`product_id`,`link_observation_key`,`gold_evaluation_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE IF NOT EXISTS `product_embeddings` (
  `id` char(32) NOT NULL,
  `asset_id` char(32) NOT NULL,
  `image_checksum` char(64) NOT NULL,
  `model_version` varchar(191) NOT NULL,
  `preprocessing_version` varchar(191) NOT NULL,
  `dimension` int NOT NULL,
  `vector_ref_json` json NOT NULL,
  `status` varchar(32) NOT NULL,
  `created_at` datetime(3) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
CREATE TABLE IF NOT EXISTS `search_index_versions` (
  `id` char(32) NOT NULL,
  `scope` varchar(191) NOT NULL,
  `model_version` varchar(191) NOT NULL,
  `preprocessing_version` varchar(191) NOT NULL,
  `dimension` int NOT NULL,
  `manifest_json` json NOT NULL,
  `status` varchar(32) NOT NULL,
  `activated_at` datetime(3) NULL,
  `created_at` datetime(3) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
 IF NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE()
 AND TABLE_NAME='product_images' AND NON_UNIQUE=0 GROUP BY INDEX_NAME
 HAVING GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')='product_id,image_relation_key'
 AND SUM(SUB_PART IS NOT NULL)=0) THEN
  ALTER TABLE product_images ADD UNIQUE KEY uq_product_images_relation (product_id,image_relation_key);
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='category_id'
 AND REFERENCED_TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME='categories' AND REFERENCED_COLUMN_NAME='id') THEN
  ALTER TABLE `products` ADD CONSTRAINT `fk_products_category_id`
  FOREIGN KEY (`category_id`) REFERENCES `categories` (`id`) ON DELETE RESTRICT;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='representative_image_id'
 AND REFERENCED_TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME='product_images' AND REFERENCED_COLUMN_NAME='id') THEN
  ALTER TABLE `products` ADD CONSTRAINT `fk_products_representative_image_id`
  FOREIGN KEY (`representative_image_id`) REFERENCES `product_images` (`id`) ON DELETE RESTRICT;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offers' AND COLUMN_NAME='product_id'
 AND REFERENCED_TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME='products' AND REFERENCED_COLUMN_NAME='id') THEN
  ALTER TABLE `offers` ADD CONSTRAINT `fk_offers_product_id`
  FOREIGN KEY (`product_id`) REFERENCES `products` (`id`) ON DELETE RESTRICT;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offer_snapshots' AND COLUMN_NAME='offer_id'
 AND REFERENCED_TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME='offers' AND REFERENCED_COLUMN_NAME='id') THEN
  ALTER TABLE `offer_snapshots` ADD CONSTRAINT `fk_offer_snapshots_offer_id`
  FOREIGN KEY (`offer_id`) REFERENCES `offers` (`id`) ON DELETE RESTRICT;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='product_id'
 AND REFERENCED_TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME='products' AND REFERENCED_COLUMN_NAME='id') THEN
  ALTER TABLE `product_images` ADD CONSTRAINT `fk_product_images_product_id`
  FOREIGN KEY (`product_id`) REFERENCES `products` (`id`) ON DELETE RESTRICT;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='asset_id'
 AND REFERENCED_TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME='assets' AND REFERENCED_COLUMN_NAME='id') THEN
  ALTER TABLE `product_images` ADD CONSTRAINT `fk_product_images_asset_id`
  FOREIGN KEY (`asset_id`) REFERENCES `assets` (`id`) ON DELETE RESTRICT;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='pipeline_publication_staging' AND COLUMN_NAME='plan_id'
 AND REFERENCED_TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME='pipeline_publications' AND REFERENCED_COLUMN_NAME='plan_id') THEN
  ALTER TABLE `pipeline_publication_staging` ADD CONSTRAINT `fk_pipeline_publication_staging_plan_id`
  FOREIGN KEY (`plan_id`) REFERENCES `pipeline_publications` (`plan_id`) ON DELETE RESTRICT;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_link_observations' AND COLUMN_NAME='product_id'
 AND REFERENCED_TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME='products' AND REFERENCED_COLUMN_NAME='id') THEN
  ALTER TABLE `product_link_observations` ADD CONSTRAINT `fk_product_link_observations_product_id`
  FOREIGN KEY (`product_id`) REFERENCES `products` (`id`) ON DELETE RESTRICT;
 END IF;
 IF NOT EXISTS (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE
 WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_embeddings' AND COLUMN_NAME='asset_id'
 AND REFERENCED_TABLE_SCHEMA=DATABASE() AND REFERENCED_TABLE_NAME='assets' AND REFERENCED_COLUMN_NAME='id') THEN
  ALTER TABLE `product_embeddings` ADD CONSTRAINT `fk_product_embeddings_asset_id`
  FOREIGN KEY (`asset_id`) REFERENCES `assets` (`id`) ON DELETE RESTRICT;
 END IF;
 SELECT COUNT(*) INTO problems FROM f25_columns e LEFT JOIN information_schema.COLUMNS c
 ON c.TABLE_SCHEMA=DATABASE() AND c.TABLE_NAME=e.t AND c.COLUMN_NAME=e.c WHERE c.COLUMN_NAME IS NULL;
 IF problems<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='POSTCHECK_MISSING_COLUMNS'; END IF;
 SELECT COUNT(*) INTO problems FROM f25_columns e JOIN information_schema.COLUMNS c
 ON c.TABLE_SCHEMA=DATABASE() AND c.TABLE_NAME=e.t AND c.COLUMN_NAME=e.c
 WHERE LOWER(c.COLUMN_TYPE)<>e.ty OR c.IS_NULLABLE<>e.n;
 IF problems<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='POSTCHECK_COLUMN_DIFFERS'; END IF;
 SELECT COUNT(*) INTO problems FROM f25_keys e WHERE NOT EXISTS (
 SELECT 1 FROM information_schema.STATISTICS s WHERE s.TABLE_SCHEMA=DATABASE()
 AND s.TABLE_NAME=e.t AND s.NON_UNIQUE=0 AND (e.kind<>'PRIMARY' OR s.INDEX_NAME='PRIMARY')
 GROUP BY s.INDEX_NAME HAVING GROUP_CONCAT(s.COLUMN_NAME ORDER BY s.SEQ_IN_INDEX SEPARATOR ',')=e.cols
 AND SUM(s.SUB_PART IS NOT NULL)=0);
 IF problems<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='POSTCHECK_PRIMARY_OR_UNIQUE_DIFFERS'; END IF;
 SELECT COUNT(*) INTO problems FROM f25_fks e WHERE NOT EXISTS (
 SELECT 1 FROM information_schema.KEY_COLUMN_USAGE k JOIN information_schema.REFERENTIAL_CONSTRAINTS r
 ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA AND r.TABLE_NAME=k.TABLE_NAME AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME
 WHERE k.TABLE_SCHEMA=DATABASE() AND k.TABLE_NAME=e.t AND k.COLUMN_NAME=e.c
 AND k.REFERENCED_TABLE_SCHEMA=DATABASE() AND k.REFERENCED_TABLE_NAME=e.p
 AND k.REFERENCED_COLUMN_NAME=e.pk AND r.DELETE_RULE IN ('RESTRICT','NO ACTION'));
 IF problems<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='POSTCHECK_FOREIGN_KEY_DIFFERS'; END IF;
 SELECT JSON_OBJECT(
 'tables',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',TABLE_NAME,'engine',ENGINE,'collation',TABLE_COLLATION,'comment',TABLE_COMMENT)),JSON_ARRAY())
 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'columns',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',TABLE_NAME,'c',COLUMN_NAME,'ty',COLUMN_TYPE,'n',IS_NULLABLE,'d',COLUMN_DEFAULT,'extra',EXTRA,'cs',CHARACTER_SET_NAME,'collation',COLLATION_NAME,'pos',ORDINAL_POSITION)),JSON_ARRAY())
 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'indexes',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',TABLE_NAME,'name',INDEX_NAME,'c',COLUMN_NAME,'seq',SEQ_IN_INDEX,'nu',NON_UNIQUE,'prefix',SUB_PART,'kind',INDEX_TYPE,'direction',COLLATION,'visible',IS_VISIBLE,'expression',EXPRESSION)),JSON_ARRAY())
 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'fks',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',k.TABLE_NAME,'name',k.CONSTRAINT_NAME,'c',k.COLUMN_NAME,'seq',k.ORDINAL_POSITION,'ps',k.REFERENCED_TABLE_SCHEMA,'p',k.REFERENCED_TABLE_NAME,'pk',k.REFERENCED_COLUMN_NAME,'del',r.DELETE_RULE,'upd',r.UPDATE_RULE)),JSON_ARRAY())
 FROM information_schema.KEY_COLUMN_USAGE k JOIN information_schema.REFERENTIAL_CONSTRAINTS r
 ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA AND r.TABLE_NAME=k.TABLE_NAME AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME
 WHERE k.TABLE_SCHEMA=DATABASE() AND k.TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions') AND k.REFERENCED_TABLE_NAME IS NOT NULL),
 'triggers',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',EVENT_OBJECT_TABLE,'name',TRIGGER_NAME,'event',EVENT_MANIPULATION,'timing',ACTION_TIMING,'statement',ACTION_STATEMENT,'definer',DEFINER)),JSON_ARRAY())
 FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=DATABASE() AND EVENT_OBJECT_TABLE IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions')),
 'checks',(SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('t',t.TABLE_NAME,'name',t.CONSTRAINT_NAME,'clause',c.CHECK_CLAUSE,'enforced',t.ENFORCED)),JSON_ARRAY())
 FROM information_schema.TABLE_CONSTRAINTS t JOIN information_schema.CHECK_CONSTRAINTS c
 ON c.CONSTRAINT_SCHEMA=t.CONSTRAINT_SCHEMA AND c.CONSTRAINT_NAME=t.CONSTRAINT_NAME
 WHERE t.TABLE_SCHEMA=DATABASE() AND t.TABLE_NAME IN ('categories','products','offers','offer_snapshots','assets','product_images','pipeline_publication_guards','pipeline_product_versions','pipeline_publications','pipeline_publication_staging','product_link_observations','product_embeddings','search_index_versions') AND t.CONSTRAINT_TYPE='CHECK')
) INTO current_shape;
 UPDATE `_feature25_schema_history` SET status='APPLIED',after_schema_json=current_shape,updated_at=UTC_TIMESTAMP(3) WHERE migration_key='feature25-publication-v2-b09eee8814c9';
 DO RELEASE_LOCK(lock_name);
 SET owns_lock=FALSE;
 SELECT 'MYSQL_SCHEMA_MATCHED_DATA_NOT_PUBLISHED' AS status,DATABASE() AS target_database,
 'b09eee8814c9f602b55a1429f8883f6ccf11c955f4dda319384d065dab444b0a' AS schema_contract_hash;
END$$
DELIMITER ;
CALL `feature25_apply_b09eee8814c9`();
DROP PROCEDURE `feature25_apply_b09eee8814c9`;

