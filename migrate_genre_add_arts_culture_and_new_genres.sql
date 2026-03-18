-- Migration: Add new genres and Arts & Culture
-- Run in MySQL Workbench (local) or against dev/prod DB as needed.
-- 1) Extend genre_name ENUM with new values (keeps existing 'Arts' for backward compatibility).
-- 2) Migrate existing 'Arts' to 'Arts & Culture'.
-- 3) Optional: remove 'Arts' from ENUM after migration (uncomment the last ALTER if desired).

-- Step 1: Add new enum values (Arts & Culture, Self-Help, Marketing, Law)
ALTER TABLE shows
MODIFY COLUMN genre_name ENUM(
  'History','Human Resources','Human Interest','Fun & Nostalgia','True Crime',
  'Financial','News & Politics','Movies','Music','Religious','Health & Wellness',
  'Parenting','Lifestyle','Storytelling','Literature','Sports','Pop Culture',
  'Arts','Arts & Culture','Business','Philosophy','Self-Help','Marketing','Law'
) DEFAULT NULL;

-- Step 2: Migrate existing 'Arts' to 'Arts & Culture'
UPDATE shows SET genre_name = 'Arts & Culture' WHERE genre_name = 'Arts';

-- Step 3 (optional): Remove 'Arts' from ENUM so only 'Arts & Culture' is valid.
-- Uncomment the following line only after Step 2 has been run and you no longer need 'Arts'.
-- ALTER TABLE shows
-- MODIFY COLUMN genre_name ENUM(
--   'History','Human Resources','Human Interest','Fun & Nostalgia','True Crime',
--   'Financial','News & Politics','Movies','Music','Religious','Health & Wellness',
--   'Parenting','Lifestyle','Storytelling','Literature','Sports','Pop Culture',
--   'Arts & Culture','Business','Philosophy','Self-Help','Marketing','Law'
-- ) DEFAULT NULL;
