-- Owner-controlled migration. Runtime review tools never execute DDL.
-- Inspect existing tables first; never replace existing owner data.
CREATE TABLE category_mappings (
 id CHAR(32) NOT NULL PRIMARY KEY,
 source_code VARCHAR(32) NOT NULL,
 source_category VARCHAR(500) NOT NULL,
 category_id CHAR(32) NOT NULL,
 mapping_version VARCHAR(100) NOT NULL,
 active BOOLEAN NOT NULL,
 created_at DATETIME(3) NOT NULL,
 updated_at DATETIME(3) NOT NULL,
 UNIQUE KEY uq_source_category (source_code,source_category),
 FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE pipeline_review_events (
 id CHAR(32) NOT NULL PRIMARY KEY,
 kind VARCHAR(32) NOT NULL,
 subject_key VARCHAR(191) NOT NULL,
 revision BIGINT NOT NULL,
 review_hash CHAR(64) NOT NULL,
 body_json JSON NOT NULL,
 actor_ref VARCHAR(255) NOT NULL,
 actor_db_user VARCHAR(255) NOT NULL,
 recorded_at DATETIME(3) NOT NULL,
 UNIQUE KEY uq_review_revision (kind,subject_key,revision),
 UNIQUE KEY uq_review_hash (review_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
