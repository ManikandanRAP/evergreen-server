-- Remove denormalized single-host columns; hosts live in show_hosts.

ALTER TABLE shows
  DROP COLUMN host_contact_name,
  DROP COLUMN host_contact_address,
  DROP COLUMN host_contact_phone,
  DROP COLUMN host_contact_email;
