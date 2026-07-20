-- MYCO May 28: extend cadence enum (Seasonal, Inactive)
-- Run on staging first: SELECT DISTINCT cadence FROM shows WHERE cadence IS NOT NULL;

ALTER TABLE shows MODIFY COLUMN cadence ENUM(
  'Daily','Weekly','Biweekly','Monthly','Ad hoc','Seasonal','Inactive'
) DEFAULT NULL;
