-- Backfill show_hosts position 1 from legacy single-host columns on shows.

INSERT INTO show_hosts (id, show_id, position, contact_name, contact_address, contact_phone, contact_email)
SELECT
  UUID(),
  s.id,
  1,
  s.host_contact_name,
  s.host_contact_address,
  s.host_contact_phone,
  s.host_contact_email
FROM shows s
WHERE (
  s.host_contact_name IS NOT NULL AND TRIM(s.host_contact_name) != ''
  OR s.host_contact_address IS NOT NULL AND TRIM(s.host_contact_address) != ''
  OR s.host_contact_phone IS NOT NULL AND TRIM(s.host_contact_phone) != ''
  OR s.host_contact_email IS NOT NULL AND TRIM(s.host_contact_email) != ''
)
AND NOT EXISTS (
  SELECT 1 FROM show_hosts h WHERE h.show_id = s.id AND h.position = 1
);
