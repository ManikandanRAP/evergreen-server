-- Feedback workflow migration (V1 + V2)
-- Workbench/MySQL-safe (avoids IF NOT EXISTS syntax that may fail on older versions).

-- 1) Add status
SET @col_exists = (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = DATABASE()
    AND table_name = 'feedback'
    AND column_name = 'status'
);
SET @sql = IF(
  @col_exists = 0,
  "ALTER TABLE feedback ADD COLUMN status ENUM('Open','In Progress','Completed') NOT NULL DEFAULT 'Open'",
  "SELECT 1"
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 2) Add completed_at
SET @col_exists = (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = DATABASE()
    AND table_name = 'feedback'
    AND column_name = 'completed_at'
);
SET @sql = IF(
  @col_exists = 0,
  "ALTER TABLE feedback ADD COLUMN completed_at DATETIME NULL",
  "SELECT 1"
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 3) Add completed_by
SET @col_exists = (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = DATABASE()
    AND table_name = 'feedback'
    AND column_name = 'completed_by'
);
SET @sql = IF(
  @col_exists = 0,
  "ALTER TABLE feedback ADD COLUMN completed_by CHAR(36) NULL",
  "SELECT 1"
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 4) Add resolution_note
SET @col_exists = (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = DATABASE()
    AND table_name = 'feedback'
    AND column_name = 'resolution_note'
);
SET @sql = IF(
  @col_exists = 0,
  "ALTER TABLE feedback ADD COLUMN resolution_note TEXT NULL",
  "SELECT 1"
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 5) Add updated_at
SET @col_exists = (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = DATABASE()
    AND table_name = 'feedback'
    AND column_name = 'updated_at'
);
SET @sql = IF(
  @col_exists = 0,
  "ALTER TABLE feedback ADD COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
  "SELECT 1"
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Backfill any legacy NULL status.
UPDATE feedback SET status = 'Open' WHERE status IS NULL;

-- 6) Create index idx_feedback_status
SET @idx_exists = (
  SELECT COUNT(*)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'feedback'
    AND index_name = 'idx_feedback_status'
);
SET @sql = IF(
  @idx_exists = 0,
  "CREATE INDEX idx_feedback_status ON feedback(status)",
  "SELECT 1"
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 7) Create index idx_feedback_completed_at
SET @idx_exists = (
  SELECT COUNT(*)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'feedback'
    AND index_name = 'idx_feedback_completed_at'
);
SET @sql = IF(
  @idx_exists = 0,
  "CREATE INDEX idx_feedback_completed_at ON feedback(completed_at)",
  "SELECT 1"
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 8) Create index idx_feedback_completed_by
SET @idx_exists = (
  SELECT COUNT(*)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'feedback'
    AND index_name = 'idx_feedback_completed_by'
);
SET @sql = IF(
  @idx_exists = 0,
  "CREATE INDEX idx_feedback_completed_by ON feedback(completed_by)",
  "SELECT 1"
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 9) Add FK for completer if missing.
SET @fk_exists = (
  SELECT COUNT(*)
  FROM information_schema.table_constraints
  WHERE table_schema = DATABASE()
    AND table_name = 'feedback'
    AND constraint_name = 'fk_feedback_completed_by_user'
    AND constraint_type = 'FOREIGN KEY'
);
SET @sql = IF(
  @fk_exists = 0,
  "ALTER TABLE feedback ADD CONSTRAINT fk_feedback_completed_by_user FOREIGN KEY (completed_by) REFERENCES users(id) ON DELETE SET NULL ON UPDATE CASCADE",
  "SELECT 1"
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Verification
-- SHOW COLUMNS FROM feedback;
-- SELECT status, COUNT(*) AS total FROM feedback GROUP BY status;
