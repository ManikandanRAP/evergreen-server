-- Migration for dev when ALTER on existing table fails with error 168.
-- Run in: evergreen_dev on evergreen-mysql-dev.
-- Steps: create empty copy, alter the copy (empty table may succeed), copy data, swap, drop old.

-- Step 1: Empty copy of shows
CREATE TABLE shows_new LIKE shows;

-- Step 2: Extend enum on the empty table (sometimes succeeds when ALTER on full table fails)
ALTER TABLE shows_new
MODIFY COLUMN genre_name ENUM(
  'History','Human Resources','Human Interest','Fun & Nostalgia','True Crime',
  'Financial','News & Politics','Movies','Music','Religious','Health & Wellness',
  'Parenting','Lifestyle','Storytelling','Literature','Sports','Pop Culture',
  'Arts','Arts & Culture','Business','Philosophy','Self-Help','Marketing','Law'
) DEFAULT NULL;

-- Step 3: Copy all rows (same structure; genre stays as-is for now)
INSERT INTO shows_new SELECT * FROM shows;

-- Step 4: Migrate Arts -> Arts & Culture in the new table
UPDATE shows_new SET genre_name = 'Arts & Culture' WHERE genre_name = 'Arts';

-- Step 5: Swap tables (app will use new table)
RENAME TABLE shows TO shows_backup, shows_new TO shows;

-- Step 6: Drop old table (run after you confirm app works)
-- DROP TABLE shows_backup;
