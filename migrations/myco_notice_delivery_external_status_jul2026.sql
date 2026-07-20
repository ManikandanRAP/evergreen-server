-- Twilio SMS carrier delivery status for Myco Notices text channel.

ALTER TABLE myco_notice_deliveries
  ADD COLUMN external_status VARCHAR(32) NULL AFTER external_id,
  ADD COLUMN external_status_at DATETIME NULL AFTER external_status;

CREATE INDEX idx_notice_deliveries_external_id ON myco_notice_deliveries (external_id);
