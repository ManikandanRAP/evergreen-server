-- MYCO May 28: drop legacy combined contact TEXT columns (run after backfill verified)

ALTER TABLE shows
  DROP COLUMN show_host_contact,
  DROP COLUMN primary_show_contact,
  DROP COLUMN show_producer_contact;
