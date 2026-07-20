-- Per-contact communication channels for Myco Notices (idempotent)

DROP PROCEDURE IF EXISTS migrate_myco_notice_contact_channels_jul2026;

DELIMITER $$
CREATE PROCEDURE migrate_myco_notice_contact_channels_jul2026()
BEGIN
    DECLARE v_has_channel_email INT DEFAULT 0;

    SELECT COUNT(*) INTO v_has_channel_email
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'myco_notice_contacts'
      AND column_name = 'channel_email';

    IF v_has_channel_email = 0 THEN
        ALTER TABLE myco_notice_contacts
            ADD COLUMN channel_email BOOLEAN NOT NULL DEFAULT FALSE AFTER myco_user_id,
            ADD COLUMN channel_text BOOLEAN NOT NULL DEFAULT FALSE AFTER channel_email,
            ADD COLUMN channel_myco BOOLEAN NOT NULL DEFAULT FALSE AFTER channel_text;

        UPDATE myco_notice_contacts c
        JOIN myco_notices n ON n.id = c.notice_id
        SET
            c.channel_email = CASE
                WHEN n.channel_email = TRUE
                 AND c.contact_email IS NOT NULL
                 AND TRIM(c.contact_email) <> ''
                THEN TRUE ELSE FALSE END,
            c.channel_text = CASE
                WHEN n.channel_text = TRUE
                 AND c.contact_phone IS NOT NULL
                 AND TRIM(c.contact_phone) <> ''
                THEN TRUE ELSE FALSE END,
            c.channel_myco = CASE
                WHEN n.channel_myco = TRUE
                 AND c.myco_user_id IS NOT NULL
                THEN TRUE ELSE FALSE END;
    END IF;
END$$
DELIMITER ;

CALL migrate_myco_notice_contact_channels_jul2026();
DROP PROCEDURE IF EXISTS migrate_myco_notice_contact_channels_jul2026;
