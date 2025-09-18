-- MySQL Migration: Add Ranking Category to Shows Table
-- This script adds a ranking_category column to the shows table

-- Step 1: Backup the shows table (recommended)
CREATE TABLE shows_backup_ranking AS SELECT * FROM shows;

-- Step 2: Add the new ranking_category column as ENUM
ALTER TABLE shows ADD COLUMN ranking_category ENUM('1','2','3','4','5') DEFAULT NULL;

-- Step 3: Verify the column was added
DESCRIBE shows;

-- Step 4: Test inserting a record with ranking category
-- INSERT INTO shows (title, ranking_category) VALUES ('Test Show with Ranking', '3');

-- Step 5: Test updating an existing record
-- UPDATE shows SET ranking_category = '2' WHERE title = 'Test Show with Ranking';

-- Step 6: Test querying by ranking category
-- SELECT id, title, ranking_category FROM shows WHERE ranking_category = '3';

-- Verification query to check the new column
SELECT 
    ranking_category,
    COUNT(*) as count
FROM shows 
GROUP BY ranking_category 
ORDER BY ranking_category;
