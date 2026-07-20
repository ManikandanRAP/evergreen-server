-- Track which notice contact (1–3) each delivery event belongs to.

ALTER TABLE myco_notice_deliveries
  ADD COLUMN contact_position TINYINT NULL AFTER recipient;

-- Best-effort backfill from current notice contacts.
UPDATE myco_notice_deliveries d
INNER JOIN myco_notice_contacts c
  ON c.notice_id = d.notice_id
SET d.contact_position = c.position
WHERE d.contact_position IS NULL
  AND d.channel = 'email'
  AND d.recipient IS NOT NULL
  AND LOWER(TRIM(d.recipient)) = LOWER(TRIM(c.contact_email));

UPDATE myco_notice_deliveries d
INNER JOIN myco_notice_contacts c
  ON c.notice_id = d.notice_id
SET d.contact_position = c.position
WHERE d.contact_position IS NULL
  AND d.channel = 'text'
  AND d.recipient IS NOT NULL
  AND REPLACE(REPLACE(REPLACE(REPLACE(TRIM(d.recipient), ' ', ''), '-', ''), '(', ''), ')', '')
    = REPLACE(REPLACE(REPLACE(REPLACE(TRIM(c.contact_phone), ' ', ''), '-', ''), '(', ''), ')', '');

UPDATE myco_notice_deliveries d
INNER JOIN myco_notice_contacts c
  ON c.notice_id = d.notice_id
SET d.contact_position = c.position
WHERE d.contact_position IS NULL
  AND d.channel = 'myco'
  AND d.recipient IS NOT NULL
  AND TRIM(d.recipient) = TRIM(c.myco_user_id);
