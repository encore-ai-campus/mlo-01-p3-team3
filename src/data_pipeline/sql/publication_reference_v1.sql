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
  PRIMARY KEY (`id`),
  UNIQUE KEY (`product_key`),
  FOREIGN KEY (`category_id`) REFERENCES `categories` (`id`)
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
  PRIMARY KEY (`id`),
  UNIQUE KEY (`offer_key`),
  FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)
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
  PRIMARY KEY (`id`),
  UNIQUE KEY (`snapshot_key`),
  FOREIGN KEY (`offer_id`) REFERENCES `offers` (`id`)
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
  PRIMARY KEY (`id`),
  UNIQUE KEY (`product_id`,`source_code`,`source_product_id`,`image_variant`,`raw_document_id`,`source_version`),
  FOREIGN KEY (`product_id`) REFERENCES `products` (`id`),
  FOREIGN KEY (`asset_id`) REFERENCES `assets` (`id`)
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
  UNIQUE KEY (`plan_id`,`product_key`),
  FOREIGN KEY (`plan_id`) REFERENCES `pipeline_publications` (`plan_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
