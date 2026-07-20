# Login Activity Automation Cron

Use this to automate routine checks with minimal manual maintenance.

## 1) One-time setup

- Ensure scripts are executable:

```bash
chmod +x evergreen-server/scripts/login_activity_health_check.py
chmod +x evergreen-server/scripts/login_activity_index_audit.py
```

- Ensure DB env vars are available to cron (or inline them in cron commands):
  - `DB_HOST`
  - `DB_PORT`
  - `DB_USER`
  - `DB_PASSWORD`
  - `DB_NAME`

## 2) Recommended cron schedule

Edit crontab:

```bash
crontab -e
```

Add:

```cron
# Daily health check at 03:15
15 3 * * * cd /d/RAP/Git/evergreen-shows-ledger && DB_HOST=127.0.0.1 DB_PORT=3306 DB_USER=root DB_PASSWORD=rootpassword DB_NAME=evergreen python evergreen-server/scripts/login_activity_health_check.py >> evergreen-server/logs/login_activity_health_check.log 2>&1

# Weekly index audit every Monday at 03:30
30 3 * * 1 cd /d/RAP/Git/evergreen-shows-ledger && DB_HOST=127.0.0.1 DB_PORT=3306 DB_USER=root DB_PASSWORD=rootpassword DB_NAME=evergreen python evergreen-server/scripts/login_activity_index_audit.py >> evergreen-server/logs/login_activity_index_audit.log 2>&1
```

## 3) Alerting integration

If you use CI/monitoring:

- trigger alert when script exits non-zero.
- parse log for:
  - `LOGIN_ACTIVITY_HEALTH_CHECK_FAILED`
  - `LOGIN_ACTIVITY_INDEX_AUDIT_FAILED`

## 4) Optional threshold tuning

`login_activity_health_check.py` supports:

- `LOGIN_ACTIVITY_READ_BUDGET_MS` (default `300`)
- `LOGIN_ACTIVITY_COUNT_BUDGET_MS` (default `1500`)
- `LOGIN_ACTIVITY_PROBE_LIMIT` (default `25`)

Example:

```bash
LOGIN_ACTIVITY_READ_BUDGET_MS=250 LOGIN_ACTIVITY_COUNT_BUDGET_MS=1200 python evergreen-server/scripts/login_activity_health_check.py
```
