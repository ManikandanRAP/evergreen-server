-- MYCO Notices: normalize contacts into child table (1–3 per notice)
-- Idempotent: safe if contacts table already exists or legacy columns already dropped.

CREATE TABLE IF NOT EXISTS myco_notice_contacts (
    id CHAR(36) PRIMARY KEY,
    notice_id CHAR(36) NOT NULL,
    position TINYINT NOT NULL,
    contact_name VARCHAR(255) NOT NULL,
    contact_email VARCHAR(255) NULL,
    contact_phone VARCHAR(64) NULL,
    contact_source ENUM('auto_primary', 'manual') NOT NULL DEFAULT 'manual',
    myco_user_id CHAR(36) NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE KEY uq_myco_notice_contacts_notice_position (notice_id, position),
    INDEX idx_myco_notice_contacts_notice (notice_id),
    INDEX idx_myco_notice_contacts_email (contact_email),
    INDEX idx_myco_notice_contacts_myco_user (myco_user_id),
    CONSTRAINT fk_myco_notice_contacts_notice
        FOREIGN KEY (notice_id) REFERENCES myco_notices(id) ON DELETE CASCADE,
    CONSTRAINT fk_myco_notice_contacts_myco_user
        FOREIGN KEY (myco_user_id) REFERENCES users(id) ON DELETE SET NULL,
    CONSTRAINT chk_myco_notice_contacts_position
        CHECK (position BETWEEN 1 AND 3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP PROCEDURE IF EXISTS migrate_myco_notice_contacts_jul2026;

DELIMITER $$
CREATE PROCEDURE migrate_myco_notice_contacts_jul2026()
BEGIN
    DECLARE v_has_contact_name INT DEFAULT 0;
    DECLARE v_fk_name VARCHAR(255) DEFAULT NULL;

    SELECT COUNT(*) INTO v_has_contact_name
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'myco_notices'
      AND column_name = 'contact_name';

    IF v_has_contact_name > 0 THEN
        INSERT INTO myco_notice_contacts (
            id, notice_id, position, contact_name, contact_email, contact_phone,
            contact_source, myco_user_id, created_at, updated_at
        )
        SELECT
            UUID(),
            n.id,
            1,
            n.contact_name,
            n.contact_email,
            n.contact_phone,
            COALESCE(n.contact_source, 'manual'),
            n.myco_recipient_user_id,
            COALESCE(n.created_at, UTC_TIMESTAMP()),
            COALESCE(n.updated_at, UTC_TIMESTAMP())
        FROM myco_notices n
        WHERE n.contact_name IS NOT NULL
          AND TRIM(n.contact_name) <> ''
          AND NOT EXISTS (
              SELECT 1 FROM myco_notice_contacts c WHERE c.notice_id = n.id
          );

        SELECT CONSTRAINT_NAME INTO v_fk_name
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'myco_notices'
          AND COLUMN_NAME = 'myco_recipient_user_id'
          AND REFERENCED_TABLE_NAME IS NOT NULL
        LIMIT 1;

        IF v_fk_name IS NOT NULL THEN
            SET @drop_fk_sql = CONCAT('ALTER TABLE myco_notices DROP FOREIGN KEY `', v_fk_name, '`');
            PREPARE stmt FROM @drop_fk_sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;

        ALTER TABLE myco_notices
            DROP COLUMN contact_name,
            DROP COLUMN contact_email,
            DROP COLUMN contact_phone,
            DROP COLUMN contact_source,
            DROP COLUMN myco_recipient_user_id;
    END IF;
END$$
DELIMITER ;

CALL migrate_myco_notice_contacts_jul2026();
DROP PROCEDURE IF EXISTS migrate_myco_notice_contacts_jul2026;
