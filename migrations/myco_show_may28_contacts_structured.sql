-- MYCO May 28: structured contact columns

ALTER TABLE shows
  ADD COLUMN host_contact_name VARCHAR(255) NULL,
  ADD COLUMN host_contact_address TEXT NULL,
  ADD COLUMN host_contact_phone VARCHAR(64) NULL,
  ADD COLUMN host_contact_email VARCHAR(255) NULL,
  ADD COLUMN primary_contact_name VARCHAR(255) NULL,
  ADD COLUMN primary_contact_address TEXT NULL,
  ADD COLUMN primary_contact_phone VARCHAR(64) NULL,
  ADD COLUMN primary_contact_email VARCHAR(255) NULL,
  ADD COLUMN producer_contact_name VARCHAR(255) NULL,
  ADD COLUMN producer_contact_address TEXT NULL,
  ADD COLUMN producer_contact_phone VARCHAR(64) NULL,
  ADD COLUMN producer_contact_email VARCHAR(255) NULL;
