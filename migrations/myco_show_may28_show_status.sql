-- MYCO May 28: show_status replaces is_active

ALTER TABLE shows ADD COLUMN show_status ENUM(
  'Active', 'Inactive', 'No longer on network'
) NULL AFTER show_type;

UPDATE shows SET show_status = CASE
  WHEN is_active = 1 THEN 'Active'
  ELSE 'Inactive'
END;

ALTER TABLE shows DROP COLUMN is_active;
