-- Enforce strict age_demographic enum (5 standard brackets).
-- Run on staging first: SELECT DISTINCT age_demographic FROM shows WHERE age_demographic IS NOT NULL;

UPDATE shows
SET age_demographic = TRIM(age_demographic)
WHERE age_demographic IS NOT NULL;

UPDATE shows
SET age_demographic = '55+'
WHERE age_demographic IN ('55', '55 +');

UPDATE shows
SET age_demographic = NULL
WHERE age_demographic IS NOT NULL
  AND age_demographic NOT IN ('18-24', '25-34', '35-44', '45-54', '55+');

ALTER TABLE shows
  MODIFY COLUMN age_demographic ENUM('18-24', '25-34', '35-44', '45-54', '55+') NULL DEFAULT NULL;
