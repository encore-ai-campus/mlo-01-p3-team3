-- Explicit additive migration only. Runtime registration never executes DDL.
-- Do not use on an existing table; inspect and back up the target first.
CREATE TABLE pipeline_runs (
 id CHAR(32) NOT NULL PRIMARY KEY,
 type VARCHAR(32) NOT NULL,
 input_range_json JSON NOT NULL,
 code_version VARCHAR(191) NOT NULL,
 status VARCHAR(32) NOT NULL,
 started_at DATETIME(3) NULL,
 finished_at DATETIME(3) NULL,
 created_at DATETIME(3) NOT NULL,
 updated_at DATETIME(3) NOT NULL
) ENGINE=InnoDB;
