CREATE TABLE `categories` (
  `id` char(32) NOT NULL,
  `code` varchar(100) NOT NULL,
  `name` varchar(255) NOT NULL,
  `active` tinyint(1) NOT NULL,
  `recognition_enabled` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `products` (
  `id` char(32) NOT NULL,
  `product_key` varchar(191) NOT NULL,
  `name` varchar(500) NOT NULL,
  `category_id` char(32) NOT NULL,
  `source_code` varchar(32) NOT NULL,
  `source_product_id` varchar(100) NOT NULL,
  `brand` varchar(255) NULL,
  `model_name` varchar(255) NULL,
  `width_mm` decimal(20,4) NULL,
  `depth_mm` decimal(20,4) NULL,
  `height_mm` decimal(20,4) NULL,
  `comparison_url` text NULL,
  `status` varchar(32) NOT NULL,
  `dedup_status` varchar(32) NOT NULL,
  `created_at` datetime(3) NOT NULL,
  `updated_at` datetime(3) NOT NULL,
  `manufacturer` varchar(255) NULL,
  `attributes_json` json NOT NULL,
  `field_status_json` json NOT NULL,
  `field_lineage_json` json NOT NULL,
  `source_refs_json` json NOT NULL,
  `price_krw` bigint NULL,
  `first_collected_at` datetime(3) NULL,
  `catalog_ready` tinyint(1) NOT NULL,
  `recommendation_ready` tinyint(1) NOT NULL,
  `comparison_link_ready` tinyint(1) NOT NULL,
  `purchase_link_ready` tinyint(1) NOT NULL,
  `quality_reason_codes` json NOT NULL,
  `publication_status` varchar(32) NOT NULL,
  `publication_reason_codes` json NOT NULL,
  `representative_image_id` char(32) NULL,
  `embedding_json` json NOT NULL,
  `gold_snapshot_ref_json` json NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`product_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `offers` (
  `id` char(32) NOT NULL,
  `offer_key` varchar(191) NOT NULL,
  `product_id` char(32) NOT NULL,
  `source_code` varchar(32) NOT NULL,
  `source_product_id` varchar(100) NOT NULL,
  `source_seller_id` varchar(191) NOT NULL,
  `source_offer_id` varchar(191) NOT NULL,
  `source_variant_id` varchar(191) NOT NULL,
  `title` varchar(500) NULL,
  `price_amount` bigint NOT NULL,
  `currency` char(3) NOT NULL,
  `purchase_url` text NULL,
  `availability` varchar(32) NOT NULL,
  `collected_at` datetime(3) NOT NULL,
  `latest_raw_document_id` char(24) NOT NULL,
  `eligible` tinyint(1) NOT NULL,
  `price_basis` varchar(32) NOT NULL,
  `price_conditions_json` json NOT NULL,
  `option_attributes_json` json NOT NULL,
  `created_at` datetime(3) NOT NULL,
  `updated_at` datetime(3) NOT NULL,
  `gold_snapshot_ref_json` json NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`offer_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `offer_snapshots` (
  `id` char(32) NOT NULL,
  `snapshot_key` varchar(191) NOT NULL,
  `offer_id` char(32) NOT NULL,
  `raw_document_id` char(24) NOT NULL,
  `price_amount` bigint NOT NULL,
  `currency` char(3) NOT NULL,
  `purchase_url` text NULL,
  `availability` varchar(32) NOT NULL,
  `collected_at` datetime(3) NOT NULL,
  `pricing_terms_json` json NOT NULL,
  `created_at` datetime(3) NOT NULL,
  `gold_snapshot_ref_json` json NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`snapshot_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `assets` (
  `id` char(32) NOT NULL,
  `owner_id` char(32) NULL,
  `kind` varchar(32) NOT NULL,
  `bucket` varchar(100) NOT NULL,
  `object_key` varchar(500) NOT NULL,
  `content_type` varchar(100) NOT NULL,
  `size_bytes` bigint NOT NULL,
  `width_px` int NOT NULL,
  `height_px` int NOT NULL,
  `checksum` char(64) NOT NULL,
  `status` varchar(32) NOT NULL,
  `usage_policy_json` json NOT NULL,
  `created_at` datetime(3) NOT NULL,
  `updated_at` datetime(3) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`bucket`,`object_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `product_images` (
  `id` char(32) NOT NULL,
  `product_id` char(32) NOT NULL,
  `asset_id` char(32) NOT NULL,
  `display_order` int NOT NULL,
  `source_code` varchar(32) NOT NULL,
  `source_product_id` varchar(100) NOT NULL,
  `image_variant` varchar(32) NOT NULL,
  `source_url` text NOT NULL,
  `source_version` varchar(191) NOT NULL,
  `raw_document_id` char(24) NOT NULL,
  `raw_payload_hash` char(64) NOT NULL,
  `raw_collected_at` datetime(3) NOT NULL,
  `linked_at` datetime(3) NOT NULL,
  `active` tinyint(1) NOT NULL,
  `usage_review_json` json NOT NULL,
  `created_at` datetime(3) NOT NULL,
  `updated_at` datetime(3) NOT NULL,
  `image_relation_key` varchar(191) NOT NULL,
  `gold_recommendation_approved` tinyint(1) NOT NULL,
  `gold_review_snapshot_hash` char(64) NULL,
  `source_evidence_json` json NOT NULL,
  `gold_snapshot_ref_json` json NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`product_id`,`source_code`,`source_product_id`,`image_variant`,`raw_document_id`,`source_version`),
  UNIQUE KEY (`product_id`,`image_relation_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `pipeline_publication_guards` (
  `id` char(32) NOT NULL,
  `product_key` varchar(191) NOT NULL,
  `revision` bigint NOT NULL,
  `guard_json` json NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`product_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `pipeline_product_versions` (
  `id` char(32) NOT NULL,
  `product_key` varchar(191) NOT NULL,
  `field_versions_json` json NOT NULL,
  `first_collected_at` datetime(3) NULL,
  `latest_as_of` datetime(3) NOT NULL,
  `latest_evaluation` varchar(191) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`product_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `pipeline_publications` (
  `plan_id` char(64) NOT NULL,
  `id` char(32) NOT NULL,
  `plan_hash` char(64) NOT NULL,
  `status` varchar(32) NOT NULL,
  `gold_versions_json` json NOT NULL,
  `product_count` int NOT NULL,
  `completed_at` datetime(3) NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`plan_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `pipeline_publication_staging` (
  `id` char(32) NOT NULL,
  `plan_id` char(64) NOT NULL,
  `product_key` varchar(191) NOT NULL,
  `body_hash` char(64) NOT NULL,
  `body_json` json NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY (`plan_id`,`product_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `product_link_observations` (
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

CREATE TABLE `product_embeddings` (
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

CREATE TABLE `search_index_versions` (
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

ALTER TABLE `products` ADD CONSTRAINT `fk_products_category_id` FOREIGN KEY (`category_id`) REFERENCES `categories` (`id`) ON DELETE RESTRICT;

ALTER TABLE `products` ADD CONSTRAINT `fk_products_representative_image_id` FOREIGN KEY (`representative_image_id`) REFERENCES `product_images` (`id`) ON DELETE RESTRICT;

ALTER TABLE `offers` ADD CONSTRAINT `fk_offers_product_id` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`) ON DELETE RESTRICT;

ALTER TABLE `offer_snapshots` ADD CONSTRAINT `fk_offer_snapshots_offer_id` FOREIGN KEY (`offer_id`) REFERENCES `offers` (`id`) ON DELETE RESTRICT;

ALTER TABLE `product_images` ADD CONSTRAINT `fk_product_images_product_id` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`) ON DELETE RESTRICT;

ALTER TABLE `product_images` ADD CONSTRAINT `fk_product_images_asset_id` FOREIGN KEY (`asset_id`) REFERENCES `assets` (`id`) ON DELETE RESTRICT;

ALTER TABLE `pipeline_publication_staging` ADD CONSTRAINT `fk_pipeline_publication_staging_plan_id` FOREIGN KEY (`plan_id`) REFERENCES `pipeline_publications` (`plan_id`) ON DELETE RESTRICT;

ALTER TABLE `product_link_observations` ADD CONSTRAINT `fk_product_link_observations_product_id` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`) ON DELETE RESTRICT;

ALTER TABLE `product_embeddings` ADD CONSTRAINT `fk_product_embeddings_asset_id` FOREIGN KEY (`asset_id`) REFERENCES `assets` (`id`) ON DELETE RESTRICT;
