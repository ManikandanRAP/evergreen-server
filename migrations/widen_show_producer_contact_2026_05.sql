-- Widen show_producer_contact to match primary_show_contact / show_host_contact (TEXT).
-- CSV imports often include full name + address + phone + email in one field.

ALTER TABLE shows
  MODIFY COLUMN show_producer_contact TEXT NULL DEFAULT NULL;
