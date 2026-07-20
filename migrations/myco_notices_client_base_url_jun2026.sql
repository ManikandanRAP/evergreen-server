-- Store the Myco web app origin used when a notice was created (for email deep links on reminders).

ALTER TABLE myco_notices
  ADD COLUMN client_base_url VARCHAR(512) NULL AFTER myco_recipient_user_id;
