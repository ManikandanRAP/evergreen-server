-- Inbox: pin messages to top of list.

ALTER TABLE inbox_messages
    ADD COLUMN pinned_at DATETIME NULL AFTER read_at;

CREATE INDEX idx_inbox_messages_user_pinned ON inbox_messages (user_id, pinned_at);
