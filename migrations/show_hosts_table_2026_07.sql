-- Multi-host contact model: up to 3 show hosts per show (position 1–3).

CREATE TABLE IF NOT EXISTS show_hosts (
  id CHAR(36) NOT NULL,
  show_id CHAR(36) NOT NULL,
  position TINYINT UNSIGNED NOT NULL COMMENT '1=primary host, 2=co-host, 3=co-host',
  contact_name VARCHAR(255) NULL,
  contact_address TEXT NULL,
  contact_phone VARCHAR(64) NULL,
  contact_email VARCHAR(255) NULL,
  created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_show_hosts_show_position (show_id, position),
  KEY idx_show_hosts_show_id (show_id),
  CONSTRAINT fk_show_hosts_show FOREIGN KEY (show_id) REFERENCES shows (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
