# MYCO Notice Reminder Cron

Runs due notice deliveries and expires notices more than 7 days after the due date.

Reminders continue from creation through the due date, then for up to 7 additional days after the due date if the notice is still active.

## Setup

```bash
chmod +x evergreen-server/scripts/notice_reminder_cron.py
```

Ensure DB and delivery env vars are available (see `.env.example`).

## Recommended schedule (hourly)

```cron
0 * * * * cd /path/to/evergreen-shows-ledger && DB_HOST=127.0.0.1 DB_PORT=3306 DB_USER=root DB_PASSWORD=... DB_NAME=evergreen python evergreen-server/scripts/notice_reminder_cron.py >> evergreen-server/logs/notice_reminder_cron.log 2>&1
```

## Manual run

```bash
python evergreen-server/scripts/notice_reminder_cron.py
```
