-- Migration script to convert shows_per_year to cadence enum
-- Run this script after updating the database schema

-- Step 1: Add the new cadence column as ENUM
ALTER TABLE shows ADD COLUMN cadence ENUM('Daily', 'Weekly', 'Biweekly', 'Monthly', 'Ad hoc') DEFAULT NULL;

-- Step 2: Migrate existing data based on shows_per_year values
-- This is a mapping strategy - you may need to adjust based on your business logic

UPDATE shows SET cadence = 'Daily' WHERE shows_per_year >= 300;  -- ~52 weeks * 6 days
UPDATE shows SET cadence = 'Weekly' WHERE shows_per_year >= 40 AND shows_per_year < 300;  -- ~52 weeks
UPDATE shows SET cadence = 'Biweekly' WHERE shows_per_year >= 20 AND shows_per_year < 40;  -- ~26 biweeks
UPDATE shows SET cadence = 'Monthly' WHERE shows_per_year >= 10 AND shows_per_year < 20;  -- ~12 months
UPDATE shows SET cadence = 'Ad hoc' WHERE shows_per_year < 10 OR shows_per_year IS NULL;  -- Irregular

-- Step 3: Make cadence column NOT NULL if you want to enforce it
-- ALTER TABLE shows MODIFY COLUMN cadence ENUM('Daily', 'Weekly', 'Biweekly', 'Monthly', 'Ad hoc') NOT NULL;

-- Step 4: Drop the old shows_per_year column (uncomment when ready)
-- ALTER TABLE shows DROP COLUMN shows_per_year;

-- Verification queries
SELECT 
    cadence,
    COUNT(*) as count,
    GROUP_CONCAT(DISTINCT shows_per_year ORDER BY shows_per_year) as original_values
FROM shows 
GROUP BY cadence 
ORDER BY cadence;
