-- MYCO Show Page schema migration
-- Run against MySQL (dev/staging/prod) after backup.
-- Does not modify qbo_show_id / qbo_show_name or their foreign keys.

-- 1) New columns
ALTER TABLE shows
  ADD COLUMN has_flightpath_access TINYINT(1) NOT NULL DEFAULT 0 AFTER has_web_mgmt_revenue,
  ADD COLUMN pre_roll_ad_slots INT NULL AFTER avg_show_length_mins,
  ADD COLUMN mid_roll_ad_slots INT NULL AFTER pre_roll_ad_slots,
  ADD COLUMN post_roll_ad_slots INT NULL AFTER mid_roll_ad_slots,
  ADD COLUMN us_listeners_pct DECIMAL(5,2) NULL AFTER post_roll_ad_slots;

-- 2) Renames (preserve prior column types / nullability)
ALTER TABLE shows
  CHANGE COLUMN start_date first_episode_date DATE NULL DEFAULT NULL,
  CHANGE COLUMN latest_cpm_usd base_cpm_usd DECIMAL(10,2) NULL DEFAULT NULL,
  CHANGE COLUMN requires_partner_access has_myco_ledger_access TINYINT(1) NULL DEFAULT NULL,
  CHANGE COLUMN evergreen_production_staff_name show_producer_contact TEXT NULL DEFAULT NULL,
  CHANGE COLUMN show_primary_contact primary_show_contact TEXT NULL;

-- 3) Genre: widen to VARCHAR first so no row keeps an invalid ENUM member during shrink.
--    (Fixes Error 1265 "Data truncated for column 'genre_name'" when old ENUM values
--    still exist or NOT NULL/default behavior leaves a non-NULL value.)
ALTER TABLE shows MODIFY COLUMN genre_name VARCHAR(64) NULL DEFAULT NULL;

-- 4) Clear every stored genre (team will re-import)
UPDATE shows SET genre_name = NULL;

-- 5) New genre ENUM (19 values) — safe once column is VARCHAR + all NULL
ALTER TABLE shows
MODIFY COLUMN genre_name ENUM(
  'Arts','Business','Comedy','Education','Fiction','Government','Health & Fitness','History',
  'Kids & Family','Leisure','Music','News','Religion & Spirituality','Science',
  'Society & Culture','Sports','Technology','True Crime','TV & Film'
) DEFAULT NULL;

-- 6) Drop deprecated columns
ALTER TABLE shows
  DROP COLUMN ad_slots,
  DROP COLUMN region,
  DROP COLUMN primary_education,
  DROP COLUMN secondary_education;
