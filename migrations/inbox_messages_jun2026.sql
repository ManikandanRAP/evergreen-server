-- Inbox service: rename user_notifications → inbox_messages, metadata, system settings.

RENAME TABLE user_notifications TO inbox_messages;

ALTER TABLE inbox_messages
    ADD COLUMN metadata JSON NULL AFTER notice_id;

ALTER TABLE inbox_messages
    RENAME INDEX idx_user_notifications_user TO idx_inbox_messages_user,
    RENAME INDEX idx_user_notifications_created TO idx_inbox_messages_created,
    RENAME INDEX idx_user_notifications_user_created TO idx_inbox_messages_user_created;

CREATE TABLE IF NOT EXISTS system_settings (
    setting_key VARCHAR(64) NOT NULL PRIMARY KEY,
    setting_value VARCHAR(255) NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO system_settings (setting_key, setting_value, updated_at)
VALUES ('inbox_retention_days', '365', UTC_TIMESTAMP());
