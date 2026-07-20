-- Repair obvious field swaps in show_hosts after legacy backfill.
-- Conservative: only moves values when the target column is empty and the source
-- clearly matches another field type (phone digits in email, email in phone, etc.).
-- Run audit first:
--   SELECT show_id, position, contact_name, contact_phone, contact_email, contact_address
--   FROM show_hosts
--   WHERE contact_phone LIKE '%@%' OR contact_email REGEXP '^[0-9+(). -]+$';

-- ---------------------------------------------------------------------------
-- 1) Swap when phone holds an email and email holds a phone number.
-- ---------------------------------------------------------------------------
UPDATE show_hosts h
INNER JOIN (
  SELECT id
  FROM show_hosts
  WHERE contact_phone IS NOT NULL
    AND TRIM(contact_phone) != ''
    AND contact_email IS NOT NULL
    AND TRIM(contact_email) != ''
    AND contact_phone LIKE '%@%'
    AND contact_email REGEXP '^[0-9+().[:space:]-]+$'
    AND LENGTH(REGEXP_REPLACE(contact_email, '[^0-9]', '')) BETWEEN 7 AND 15
) src ON h.id = src.id
SET
  h.contact_phone = TRIM(h.contact_email),
  h.contact_email = REPLACE(REPLACE(TRIM(h.contact_phone), ' and ', '; '), ' AND ', '; ');

-- ---------------------------------------------------------------------------
-- 2) Email stuck in phone when email column is empty.
-- ---------------------------------------------------------------------------
UPDATE show_hosts
SET
  contact_email = REPLACE(REPLACE(TRIM(contact_phone), ' and ', '; '), ' AND ', '; '),
  contact_phone = NULL
WHERE contact_phone IS NOT NULL
  AND TRIM(contact_phone) != ''
  AND contact_phone LIKE '%@%'
  AND (contact_email IS NULL OR TRIM(contact_email) = '');

-- ---------------------------------------------------------------------------
-- 3) Phone number stuck in email when phone column is empty.
-- ---------------------------------------------------------------------------
UPDATE show_hosts
SET
  contact_phone = TRIM(contact_email),
  contact_email = NULL
WHERE contact_email IS NOT NULL
  AND TRIM(contact_email) != ''
  AND (contact_phone IS NULL OR TRIM(contact_phone) = '')
  AND contact_email NOT LIKE '%@%'
  AND contact_email REGEXP '^[0-9+().[:space:]-Ee]+$'
  AND LENGTH(REGEXP_REPLACE(contact_email, '[^0-9]', '')) BETWEEN 7 AND 15;

-- ---------------------------------------------------------------------------
-- 4) Mailing address stuck in phone when address column is empty.
-- ---------------------------------------------------------------------------
UPDATE show_hosts
SET
  contact_address = TRIM(contact_phone),
  contact_phone = NULL
WHERE contact_phone IS NOT NULL
  AND TRIM(contact_phone) != ''
  AND contact_phone NOT LIKE '%@%'
  AND (contact_address IS NULL OR TRIM(contact_address) = '')
  AND (
    contact_phone REGEXP '(Drive|Avenue|Ave\\.?|Street|St\\.?|Road|Rd\\.?|Boulevard|Blvd\\.?|Cir(cle)?|Lane|Ln\\.?|Way|Court|Ct\\.?|United States|USA|France|Germany|Montmorillon)'
    OR (
      CHAR_LENGTH(TRIM(contact_phone)) > 45
      AND contact_phone REGEXP '[0-9]{5}'
    )
  );

-- ---------------------------------------------------------------------------
-- 5) Person name stuck in phone when name column is empty.
-- ---------------------------------------------------------------------------
UPDATE show_hosts
SET
  contact_name = TRIM(contact_phone),
  contact_phone = NULL
WHERE contact_phone IS NOT NULL
  AND TRIM(contact_phone) != ''
  AND contact_phone NOT LIKE '%@%'
  AND (contact_name IS NULL OR TRIM(contact_name) = '')
  AND CHAR_LENGTH(TRIM(contact_phone)) BETWEEN 3 AND 50
  AND LENGTH(REGEXP_REPLACE(contact_phone, '[^0-9]', '')) < 7
  AND contact_phone REGEXP '^[A-Za-z][A-Za-z .''\\-]+$'
  AND contact_phone NOT REGEXP '(Drive|Avenue|Street|Boulevard|United States|France|Germany)';

-- ---------------------------------------------------------------------------
-- 6) Trim whitespace on all contact fields.
-- ---------------------------------------------------------------------------
UPDATE show_hosts
SET
  contact_name = NULLIF(TRIM(contact_name), ''),
  contact_address = NULLIF(TRIM(contact_address), ''),
  contact_phone = NULLIF(TRIM(contact_phone), ''),
  contact_email = NULLIF(TRIM(contact_email), '')
WHERE contact_name IS NOT NULL
   OR contact_address IS NOT NULL
   OR contact_phone IS NOT NULL
   OR contact_email IS NOT NULL;
