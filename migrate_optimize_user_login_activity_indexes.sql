-- Optimize indexes for high-volume cursor pagination and filtered reads.
-- Safe to run multiple times on MySQL 8.

-- 1) Primary cursor traversal index (ORDER BY occurred_at_utc DESC, id DESC)
SET @idx_exists := (
  SELECT COUNT(1)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'user_login_activity'
    AND index_name = 'idx_user_login_activity_time_id'
);
SET @ddl := IF(
  @idx_exists = 0,
  'CREATE INDEX idx_user_login_activity_time_id ON user_login_activity (occurred_at_utc DESC, id DESC)',
  'SELECT ''idx_user_login_activity_time_id exists'' '
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 2) Action + status filtered feeds
SET @idx_exists := (
  SELECT COUNT(1)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'user_login_activity'
    AND index_name = 'idx_user_login_activity_action_status_time_id'
);
SET @ddl := IF(
  @idx_exists = 0,
  'CREATE INDEX idx_user_login_activity_action_status_time_id ON user_login_activity (action, status, occurred_at_utc DESC, id DESC)',
  'SELECT ''idx_user_login_activity_action_status_time_id exists'' '
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 3) Per-user feeds (email + time)
SET @idx_exists := (
  SELECT COUNT(1)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'user_login_activity'
    AND index_name = 'idx_user_login_activity_email_time_id'
);
SET @ddl := IF(
  @idx_exists = 0,
  'CREATE INDEX idx_user_login_activity_email_time_id ON user_login_activity (user_email, occurred_at_utc DESC, id DESC)',
  'SELECT ''idx_user_login_activity_email_time_id exists'' '
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 4) Request-id lookups/troubleshooting
SET @idx_exists := (
  SELECT COUNT(1)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'user_login_activity'
    AND index_name = 'idx_user_login_activity_request_id_time'
);
SET @ddl := IF(
  @idx_exists = 0,
  'CREATE INDEX idx_user_login_activity_request_id_time ON user_login_activity (request_id, occurred_at_utc DESC)',
  'SELECT ''idx_user_login_activity_request_id_time exists'' '
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 5) Name prefix lookups for fast search
SET @idx_exists := (
  SELECT COUNT(1)
  FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'user_login_activity'
    AND index_name = 'idx_user_login_activity_name_time_id'
);
SET @ddl := IF(
  @idx_exists = 0,
  'CREATE INDEX idx_user_login_activity_name_time_id ON user_login_activity (user_name, occurred_at_utc DESC, id DESC)',
  'SELECT ''idx_user_login_activity_name_time_id exists'' '
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Optional cleanup: if older narrower indexes exist and are redundant, keep only after EXPLAIN confirms usage.
-- DROP INDEX idx_user_login_activity_time ON user_login_activity;
-- DROP INDEX idx_user_login_activity_email_time ON user_login_activity;
-- DROP INDEX idx_user_login_activity_action_time ON user_login_activity;
-- DROP INDEX idx_user_login_activity_status_time ON user_login_activity;

-- EXPLAIN validation examples (run manually):
-- EXPLAIN ANALYZE
-- SELECT id, occurred_at_utc
-- FROM user_login_activity
-- WHERE action='LOGIN' AND status='SUCCESS'
-- ORDER BY occurred_at_utc DESC, id DESC
-- LIMIT 26;
--
-- EXPLAIN ANALYZE
-- SELECT id, occurred_at_utc
-- FROM user_login_activity
-- WHERE user_email='admin@evergreen.com'
-- ORDER BY occurred_at_utc DESC, id DESC
-- LIMIT 26;
