-- feature25: reverse only objects added by feature25_apply.sql.
-- Refuses to discard new table rows or non-NULL values in newly added columns.
-- Existing data changes are not restored. Do not disable FOREIGN_KEY_CHECKS.
SET SESSION autocommit=1;
SET SESSION time_zone='+00:00';
SET SESSION lock_wait_timeout=30;
SET SESSION innodb_lock_wait_timeout=30;
DROP PROCEDURE IF EXISTS `feature25_rollback_b09eee8814c9`;
DELIMITER $$
CREATE PROCEDURE `feature25_rollback_b09eee8814c9`()
COMMENT 'feature25-schema-helper-v1'
rollback_body: BEGIN
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
   UPDATE `_feature25_schema_history` SET status='ROLLBACK_FAILED',after_schema_json=current_shape,updated_at=UTC_TIMESTAMP(3)
   WHERE migration_key='feature25-publication-v2-b09eee8814c9';
  END IF;
  IF owns_lock THEN DO RELEASE_LOCK(lock_name); END IF;
  RESIGNAL;
 END;
 IF @@version NOT REGEXP '^(8|9)[.]' OR @@global.read_only OR @@global.super_read_only THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='WRITABLE_MYSQL_8_OR_9_PRIMARY_REQUIRED';END IF;
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
 IF NOT EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='_feature25_schema_history') THEN
 DO RELEASE_LOCK(lock_name);SET owns_lock=FALSE;
 SELECT 'NOTHING_TO_ROLL_BACK' AS status;LEAVE rollback_body;END IF;
 IF NOT EXISTS (SELECT 1 FROM `_feature25_schema_history` WHERE migration_key='feature25-publication-v2-b09eee8814c9') THEN
 DO RELEASE_LOCK(lock_name);SET owns_lock=FALSE;
 SELECT 'NOTHING_TO_ROLL_BACK' AS status;LEAVE rollback_body;END IF;
 IF NOT EXISTS (SELECT 1 FROM `_feature25_schema_history` WHERE migration_key='feature25-publication-v2-b09eee8814c9' AND format_version=1 AND schema_hash='b09eee8814c9f602b55a1429f8883f6ccf11c955f4dda319384d065dab444b0a') THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='JOURNAL_VERSION_MISMATCH'; END IF;
 SELECT before_schema_json,after_schema_json,status INTO saved,current_shape,previous_status
 FROM `_feature25_schema_history` WHERE migration_key='feature25-publication-v2-b09eee8814c9';
 IF previous_status='ROLLED_BACK' THEN
 DO RELEASE_LOCK(lock_name);SET owns_lock=FALSE;
 SELECT 'ALREADY_ROLLED_BACK' AS status;LEAVE rollback_body;END IF;
 SET @f25_recorded_shape=current_shape;
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
 IF @f25_recorded_shape IS NULL OR NOT (JSON_CONTAINS(current_shape,@f25_recorded_shape)=1 AND JSON_CONTAINS(@f25_recorded_shape,current_shape)=1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='SCHEMA_CHANGED_AFTER_RECORDED_STEP_REVIEW_REQUIRED';
 END IF;
 IF JSON_CONTAINS(current_shape,saved)<>1 THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ORIGINAL_SCHEMA_CHANGED_REVIEW_REQUIRED';
 END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.tables'),JSON_OBJECT('t','product_link_observations'))=1) AND EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_link_observations') THEN
 IF EXISTS (SELECT 1 FROM `product_link_observations` LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_DATA_IN_PRODUCT_LINK_OBSERVATIONS'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.tables'),JSON_OBJECT('t','product_embeddings'))=1) AND EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_embeddings') THEN
 IF EXISTS (SELECT 1 FROM `product_embeddings` LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_DATA_IN_PRODUCT_EMBEDDINGS'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.tables'),JSON_OBJECT('t','search_index_versions'))=1) AND EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='search_index_versions') THEN
 IF EXISTS (SELECT 1 FROM `search_index_versions` LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_DATA_IN_SEARCH_INDEX_VERSIONS'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','manufacturer'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='manufacturer') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `manufacturer` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_MANUFACTURER'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','attributes_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='attributes_json') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `attributes_json` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_ATTRIBUTES_JSON'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','field_status_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='field_status_json') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `field_status_json` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_FIELD_STATUS_JSON'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','field_lineage_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='field_lineage_json') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `field_lineage_json` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_FIELD_LINEAGE_JSON'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','source_refs_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='source_refs_json') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `source_refs_json` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_SOURCE_REFS_JSON'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','price_krw'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='price_krw') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `price_krw` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_PRICE_KRW'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','first_collected_at'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='first_collected_at') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `first_collected_at` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_FIRST_COLLECTED_AT'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','catalog_ready'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='catalog_ready') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `catalog_ready` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_CATALOG_READY'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','recommendation_ready'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='recommendation_ready') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `recommendation_ready` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_RECOMMENDATION_READY'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','comparison_link_ready'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='comparison_link_ready') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `comparison_link_ready` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_COMPARISON_LINK_READY'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','purchase_link_ready'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='purchase_link_ready') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `purchase_link_ready` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_PURCHASE_LINK_READY'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','quality_reason_codes'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='quality_reason_codes') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `quality_reason_codes` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_QUALITY_REASON_CODES'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','publication_status'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='publication_status') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `publication_status` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_PUBLICATION_STATUS'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','publication_reason_codes'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='publication_reason_codes') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `publication_reason_codes` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_PUBLICATION_REASON_CODES'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','representative_image_id'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='representative_image_id') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `representative_image_id` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_REPRESENTATIVE_IMAGE_ID'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','embedding_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='embedding_json') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `embedding_json` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_EMBEDDING_JSON'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','gold_snapshot_ref_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
 IF EXISTS (SELECT 1 FROM `products` WHERE `gold_snapshot_ref_json` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCTS_GOLD_SNAPSHOT_REF_JSON'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','offers','c','gold_snapshot_ref_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offers' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
 IF EXISTS (SELECT 1 FROM `offers` WHERE `gold_snapshot_ref_json` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_OFFERS_GOLD_SNAPSHOT_REF_JSON'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','offer_snapshots','c','gold_snapshot_ref_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offer_snapshots' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
 IF EXISTS (SELECT 1 FROM `offer_snapshots` WHERE `gold_snapshot_ref_json` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_OFFER_SNAPSHOTS_GOLD_SNAPSHOT_REF_JSON'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','product_images','c','image_relation_key'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='image_relation_key') THEN
 IF EXISTS (SELECT 1 FROM `product_images` WHERE `image_relation_key` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCT_IMAGES_IMAGE_RELATION_KEY'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','product_images','c','gold_recommendation_approved'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='gold_recommendation_approved') THEN
 IF EXISTS (SELECT 1 FROM `product_images` WHERE `gold_recommendation_approved` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCT_IMAGES_GOLD_RECOMMENDATION_APPROVED'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','product_images','c','gold_review_snapshot_hash'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='gold_review_snapshot_hash') THEN
 IF EXISTS (SELECT 1 FROM `product_images` WHERE `gold_review_snapshot_hash` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCT_IMAGES_GOLD_REVIEW_SNAPSHOT_HASH'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','product_images','c','source_evidence_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='source_evidence_json') THEN
 IF EXISTS (SELECT 1 FROM `product_images` WHERE `source_evidence_json` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCT_IMAGES_SOURCE_EVIDENCE_JSON'; END IF;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','product_images','c','gold_snapshot_ref_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
 IF EXISTS (SELECT 1 FROM `product_images` WHERE `gold_snapshot_ref_json` IS NOT NULL LIMIT 1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_REFUSED_NON_NULL_PRODUCT_IMAGES_GOLD_SNAPSHOT_REF_JSON'; END IF;END IF;
 SET journal_started=TRUE;
 UPDATE `_feature25_schema_history` SET status='ROLLING_BACK',updated_at=UTC_TIMESTAMP(3) WHERE migration_key='feature25-publication-v2-b09eee8814c9';
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.fks'),JSON_OBJECT('t','products','name','fk_products_category_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products'
 AND CONSTRAINT_NAME='fk_products_category_id' AND REFERENCED_TABLE_NAME IS NOT NULL) THEN
 ALTER TABLE `products` DROP FOREIGN KEY `fk_products_category_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.fks'),JSON_OBJECT('t','products','name','fk_products_representative_image_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products'
 AND CONSTRAINT_NAME='fk_products_representative_image_id' AND REFERENCED_TABLE_NAME IS NOT NULL) THEN
 ALTER TABLE `products` DROP FOREIGN KEY `fk_products_representative_image_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.fks'),JSON_OBJECT('t','offers','name','fk_offers_product_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offers'
 AND CONSTRAINT_NAME='fk_offers_product_id' AND REFERENCED_TABLE_NAME IS NOT NULL) THEN
 ALTER TABLE `offers` DROP FOREIGN KEY `fk_offers_product_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.fks'),JSON_OBJECT('t','offer_snapshots','name','fk_offer_snapshots_offer_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offer_snapshots'
 AND CONSTRAINT_NAME='fk_offer_snapshots_offer_id' AND REFERENCED_TABLE_NAME IS NOT NULL) THEN
 ALTER TABLE `offer_snapshots` DROP FOREIGN KEY `fk_offer_snapshots_offer_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.fks'),JSON_OBJECT('t','product_images','name','fk_product_images_product_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images'
 AND CONSTRAINT_NAME='fk_product_images_product_id' AND REFERENCED_TABLE_NAME IS NOT NULL) THEN
 ALTER TABLE `product_images` DROP FOREIGN KEY `fk_product_images_product_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.fks'),JSON_OBJECT('t','product_images','name','fk_product_images_asset_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images'
 AND CONSTRAINT_NAME='fk_product_images_asset_id' AND REFERENCED_TABLE_NAME IS NOT NULL) THEN
 ALTER TABLE `product_images` DROP FOREIGN KEY `fk_product_images_asset_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.fks'),JSON_OBJECT('t','pipeline_publication_staging','name','fk_pipeline_publication_staging_plan_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='pipeline_publication_staging'
 AND CONSTRAINT_NAME='fk_pipeline_publication_staging_plan_id' AND REFERENCED_TABLE_NAME IS NOT NULL) THEN
 ALTER TABLE `pipeline_publication_staging` DROP FOREIGN KEY `fk_pipeline_publication_staging_plan_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.fks'),JSON_OBJECT('t','product_link_observations','name','fk_product_link_observations_product_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_link_observations'
 AND CONSTRAINT_NAME='fk_product_link_observations_product_id' AND REFERENCED_TABLE_NAME IS NOT NULL) THEN
 ALTER TABLE `product_link_observations` DROP FOREIGN KEY `fk_product_link_observations_product_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.fks'),JSON_OBJECT('t','product_embeddings','name','fk_product_embeddings_asset_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_embeddings'
 AND CONSTRAINT_NAME='fk_product_embeddings_asset_id' AND REFERENCED_TABLE_NAME IS NOT NULL) THEN
 ALTER TABLE `product_embeddings` DROP FOREIGN KEY `fk_product_embeddings_asset_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.indexes'),JSON_OBJECT('t','product_link_observations','name','fk_product_link_observations_product_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_link_observations' AND INDEX_NAME='fk_product_link_observations_product_id') THEN
 ALTER TABLE `product_link_observations` DROP INDEX `fk_product_link_observations_product_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.indexes'),JSON_OBJECT('t','product_embeddings','name','fk_product_embeddings_asset_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_embeddings' AND INDEX_NAME='fk_product_embeddings_asset_id') THEN
 ALTER TABLE `product_embeddings` DROP INDEX `fk_product_embeddings_asset_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.tables'),JSON_OBJECT('t','search_index_versions'))=1) AND EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='search_index_versions') THEN
 DROP TABLE `search_index_versions`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.tables'),JSON_OBJECT('t','product_embeddings'))=1) AND EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_embeddings') THEN
 DROP TABLE `product_embeddings`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.tables'),JSON_OBJECT('t','product_link_observations'))=1) AND EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_link_observations') THEN
 DROP TABLE `product_link_observations`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.indexes'),JSON_OBJECT('t','products','name','fk_products_category_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND INDEX_NAME='fk_products_category_id') THEN
 ALTER TABLE `products` DROP INDEX `fk_products_category_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.indexes'),JSON_OBJECT('t','products','name','fk_products_representative_image_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND INDEX_NAME='fk_products_representative_image_id') THEN
 ALTER TABLE `products` DROP INDEX `fk_products_representative_image_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.indexes'),JSON_OBJECT('t','offers','name','fk_offers_product_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offers' AND INDEX_NAME='fk_offers_product_id') THEN
 ALTER TABLE `offers` DROP INDEX `fk_offers_product_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.indexes'),JSON_OBJECT('t','offer_snapshots','name','fk_offer_snapshots_offer_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offer_snapshots' AND INDEX_NAME='fk_offer_snapshots_offer_id') THEN
 ALTER TABLE `offer_snapshots` DROP INDEX `fk_offer_snapshots_offer_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.indexes'),JSON_OBJECT('t','product_images','name','fk_product_images_product_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND INDEX_NAME='fk_product_images_product_id') THEN
 ALTER TABLE `product_images` DROP INDEX `fk_product_images_product_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.indexes'),JSON_OBJECT('t','product_images','name','fk_product_images_asset_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND INDEX_NAME='fk_product_images_asset_id') THEN
 ALTER TABLE `product_images` DROP INDEX `fk_product_images_asset_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.indexes'),JSON_OBJECT('t','pipeline_publication_staging','name','fk_pipeline_publication_staging_plan_id'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='pipeline_publication_staging' AND INDEX_NAME='fk_pipeline_publication_staging_plan_id') THEN
 ALTER TABLE `pipeline_publication_staging` DROP INDEX `fk_pipeline_publication_staging_plan_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.indexes'),JSON_OBJECT('t','product_images','name','uq_product_images_relation'))=1) AND EXISTS (
 SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND INDEX_NAME='uq_product_images_relation') THEN
 ALTER TABLE `product_images` DROP INDEX `uq_product_images_relation`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','product_images','c','gold_snapshot_ref_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
 ALTER TABLE `product_images` DROP COLUMN `gold_snapshot_ref_json`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','product_images','c','source_evidence_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='source_evidence_json') THEN
 ALTER TABLE `product_images` DROP COLUMN `source_evidence_json`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','product_images','c','gold_review_snapshot_hash'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='gold_review_snapshot_hash') THEN
 ALTER TABLE `product_images` DROP COLUMN `gold_review_snapshot_hash`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','product_images','c','gold_recommendation_approved'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='gold_recommendation_approved') THEN
 ALTER TABLE `product_images` DROP COLUMN `gold_recommendation_approved`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','product_images','c','image_relation_key'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='product_images' AND COLUMN_NAME='image_relation_key') THEN
 ALTER TABLE `product_images` DROP COLUMN `image_relation_key`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','offer_snapshots','c','gold_snapshot_ref_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offer_snapshots' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
 ALTER TABLE `offer_snapshots` DROP COLUMN `gold_snapshot_ref_json`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','offers','c','gold_snapshot_ref_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='offers' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
 ALTER TABLE `offers` DROP COLUMN `gold_snapshot_ref_json`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','gold_snapshot_ref_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='gold_snapshot_ref_json') THEN
 ALTER TABLE `products` DROP COLUMN `gold_snapshot_ref_json`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','embedding_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='embedding_json') THEN
 ALTER TABLE `products` DROP COLUMN `embedding_json`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','representative_image_id'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='representative_image_id') THEN
 ALTER TABLE `products` DROP COLUMN `representative_image_id`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','publication_reason_codes'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='publication_reason_codes') THEN
 ALTER TABLE `products` DROP COLUMN `publication_reason_codes`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','publication_status'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='publication_status') THEN
 ALTER TABLE `products` DROP COLUMN `publication_status`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','quality_reason_codes'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='quality_reason_codes') THEN
 ALTER TABLE `products` DROP COLUMN `quality_reason_codes`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','purchase_link_ready'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='purchase_link_ready') THEN
 ALTER TABLE `products` DROP COLUMN `purchase_link_ready`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','comparison_link_ready'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='comparison_link_ready') THEN
 ALTER TABLE `products` DROP COLUMN `comparison_link_ready`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','recommendation_ready'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='recommendation_ready') THEN
 ALTER TABLE `products` DROP COLUMN `recommendation_ready`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','catalog_ready'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='catalog_ready') THEN
 ALTER TABLE `products` DROP COLUMN `catalog_ready`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','first_collected_at'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='first_collected_at') THEN
 ALTER TABLE `products` DROP COLUMN `first_collected_at`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','price_krw'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='price_krw') THEN
 ALTER TABLE `products` DROP COLUMN `price_krw`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','source_refs_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='source_refs_json') THEN
 ALTER TABLE `products` DROP COLUMN `source_refs_json`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','field_lineage_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='field_lineage_json') THEN
 ALTER TABLE `products` DROP COLUMN `field_lineage_json`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','field_status_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='field_status_json') THEN
 ALTER TABLE `products` DROP COLUMN `field_status_json`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','attributes_json'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='attributes_json') THEN
 ALTER TABLE `products` DROP COLUMN `attributes_json`;END IF;
 IF NOT (JSON_CONTAINS(JSON_EXTRACT(saved,'$.columns'),JSON_OBJECT('t','products','c','manufacturer'))=1) AND EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='products' AND COLUMN_NAME='manufacturer') THEN
 ALTER TABLE `products` DROP COLUMN `manufacturer`;END IF;
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
 IF NOT (JSON_CONTAINS(current_shape,saved)=1 AND JSON_CONTAINS(saved,current_shape)=1) THEN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='ROLLBACK_SCHEMA_CHECK_FAILED';END IF;
 UPDATE `_feature25_schema_history` SET status='ROLLED_BACK',after_schema_json=current_shape,updated_at=UTC_TIMESTAMP(3) WHERE migration_key='feature25-publication-v2-b09eee8814c9';
 DO RELEASE_LOCK(lock_name);SET owns_lock=FALSE;
 SELECT 'FEATURE25_SCHEMA_ROLLED_BACK' AS status,DATABASE() AS target_database;
END$$
DELIMITER ;
CALL `feature25_rollback_b09eee8814c9`();
DROP PROCEDURE `feature25_rollback_b09eee8814c9`;

