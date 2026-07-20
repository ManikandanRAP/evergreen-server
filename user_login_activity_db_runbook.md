# User Login Activity DB Runbook

Use this runbook to create the permanent `user_login_activity` table in local, dev, and production environments.

## 1) Local MySQL Workbench

1. Open your local MySQL connection.
2. Select the target schema (`evergreen` by default in local docker compose).
3. Run:

```sql
SOURCE migrate_add_user_login_activity.sql;
```

Or paste the full SQL from `migrate_add_user_login_activity.sql` directly.

## 2) Local Docker (from docker-compose.yml)

```bash
docker exec -i evergreen-mysql mysql -uroot -prootpassword evergreen < migrate_add_user_login_activity.sql
```

## 3) Dev Docker (from docker-compose-dev.yml conventions)

```bash
docker exec -i evergreen-mysql-dev mysql -uroot -prootpassword evergreen_dev < migrate_add_user_login_activity.sql
```

If your dev container/db name differs, replace:
- `evergreen-mysql-dev` (container)
- `evergreen_dev` (database)

## 4) Production Docker (template)

```bash
docker exec -i <prod_mysql_container> mysql -u<prod_db_user> -p<prod_db_password> <prod_db_name> < migrate_add_user_login_activity.sql
```

## 5) Verify Table + Indexes

```bash
docker exec -i evergreen-mysql mysql -uroot -prootpassword evergreen -e "SHOW CREATE TABLE user_login_activity\G"
```

Expected indexes:
- `uq_user_login_activity_event_uuid`
- `idx_user_login_activity_time`
- `idx_user_login_activity_email_time`
- `idx_user_login_activity_action_time`
- `idx_user_login_activity_status_time`
