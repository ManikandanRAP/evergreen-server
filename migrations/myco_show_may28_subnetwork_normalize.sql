-- MYCO May 28: normalize subnetwork_id to allowed list (NULL invalid values)
-- Audit before running:
-- SELECT id, title, subnetwork_id FROM shows
-- WHERE subnetwork_id IS NOT NULL AND TRIM(subnetwork_id) != ''
--   AND subnetwork_id NOT IN (
--     'CONmunity','Crowd Network','Evergreen','Next Chapter','Osiris Media','Sound Talent Media'
--   );

UPDATE shows
SET subnetwork_id = NULL
WHERE subnetwork_id IS NULL
   OR TRIM(subnetwork_id) = ''
   OR subnetwork_id IN ('none', 'None', 'NONE')
   OR subnetwork_id NOT IN (
     'CONmunity','Crowd Network','Evergreen','Next Chapter','Osiris Media','Sound Talent Media'
   );
