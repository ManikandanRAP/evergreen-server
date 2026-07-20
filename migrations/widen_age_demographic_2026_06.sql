-- Widen age_demographic from VARCHAR(5) to VARCHAR(6).
-- Age brackets such as "23-60+", "18-60+", "28-60+" need 6 characters.
-- The column is free-text (not an ENUM), so widening it eliminates the
-- MySQL 1406 "Data too long for column 'age_demographic'" errors on import.

ALTER TABLE shows
  MODIFY COLUMN age_demographic VARCHAR(6) NULL DEFAULT NULL;
