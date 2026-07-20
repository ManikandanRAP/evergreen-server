-- Performance indexes for notices, feedbacks, notifications, shows, and admin lists.
-- Safe to run manually; the API also applies these on startup via ensure_performance_indexes().
-- Existing indexes are skipped when run through the application.

-- Feedbacks (GET /feedbacks)
CREATE INDEX idx_feedback_created_at ON feedback (created_at);
CREATE INDEX idx_feedback_created_by ON feedback (created_by);
CREATE INDEX idx_feedback_type ON feedback (type);
CREATE INDEX idx_feedback_status ON feedback (status);
CREATE INDEX idx_feedback_completed_at ON feedback (completed_at);
CREATE INDEX idx_feedback_completed_by ON feedback (completed_by);
CREATE INDEX idx_feedback_status_created_at ON feedback (status, created_at);
CREATE INDEX idx_feedback_status_completed_at ON feedback (status, completed_at);

-- Myco notices (GET /notices, scheduler)
CREATE INDEX idx_myco_notices_created_at ON myco_notices (created_at);
CREATE INDEX idx_myco_notices_status_created_at ON myco_notices (status, created_at);
CREATE INDEX idx_myco_notices_notice_type ON myco_notices (notice_type);
CREATE INDEX idx_myco_notices_created_by ON myco_notices (created_by);

-- Notice delivery history
CREATE INDEX idx_notice_deliveries_notice_sent ON myco_notice_deliveries (notice_id, sent_at);

-- Inbox messages (GET /inbox/messages, unread-count)
CREATE INDEX idx_inbox_messages_user_created ON inbox_messages (user_id, created_at);

-- Shows archive views
CREATE INDEX idx_shows_archived ON shows (is_archived);
CREATE INDEX idx_shows_archived_at ON shows (is_archived, archived_at);

-- Split history detail lookups
CREATE INDEX idx_split_history_show_vendor_eff ON split_history (show_qbo_id, vendor_qbo_id, effective_date);

-- User login activity admin page (MySQL 8+ descending indexes)
CREATE INDEX idx_user_login_activity_time_id ON user_login_activity (occurred_at_utc DESC, id DESC);
CREATE INDEX idx_user_login_activity_action_status_time_id ON user_login_activity (action, status, occurred_at_utc DESC, id DESC);
CREATE INDEX idx_user_login_activity_email_time_id ON user_login_activity (user_email, occurred_at_utc DESC, id DESC);
CREATE INDEX idx_user_login_activity_request_id_time ON user_login_activity (request_id, occurred_at_utc DESC);
CREATE INDEX idx_user_login_activity_name_time_id ON user_login_activity (user_name, occurred_at_utc DESC, id DESC);
