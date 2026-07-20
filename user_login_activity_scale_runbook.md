# User Login Activity Scale Runbook

This runbook defines operational standards for scaling `user_login_activity` to multi-million rows.

## 1. Capacity Thresholds

- `0 - 5M` rows: cursor pagination + optimized indexes only.
- `5M - 50M` rows: enforce bounded default date range in UI (example last 90 days), add archive cadence.
- `50M+` rows: move to monthly partitioning and hot/cold data tiers.

## 2. Hot Data Policy

- Keep recent data in primary OLTP database:
  - recommended hot window: `12-24 months`.
- Admin UI defaults to hot window; explicit archive query is separate flow.

## 3. Cold Data Policy

- Export old partitions or date ranges to compressed archive dumps.
- Store in versioned object storage path, for example:
  - `s3://<bucket>/audit/user_login_activity/YYYY/MM/`
- Keep restore tooling tested quarterly.

## 4. Partitioning Strategy (MySQL 8)

Use range partitioning by month on `occurred_at_utc` when row count and write rates justify it.

Example approach:

1. Create partitioned replacement table with same schema/indexes.
2. Backfill in date chunks.
3. Swap tables during maintenance window.
4. Add scheduled task to create next monthly partition ahead of time.

Notes:

- Always test partition pruning with `EXPLAIN`.
- Keep primary key/order compatibility for cursor reads.

## 5. Archive Job Cadence

- Run archive job monthly.
- Candidate rows: older than hot window boundary.
- Steps:
  1. export range
  2. checksum/count verification
  3. mark archive manifest
  4. purge from hot table

## 6. Safety Checks Before Purge

- Verify exported row count matches source row count.
- Validate random sample re-import in non-prod.
- Store immutable manifest:
  - range
  - row count
  - checksum
  - export timestamp

## 7. Rollback Strategy

If purge or schema migration causes issue:

1. stop archive job
2. restore last verified archive into temporary table
3. validate counts/checksums
4. merge back into `user_login_activity`
5. re-enable traffic

## 8. Backup/Restore Verification

- Ensure database export/import procedures include `user_login_activity`.
- After every restore:
  - verify table exists
  - verify index set
  - run smoke query on latest and archived ranges

## 9. Monitoring and SLO Targets

- Read API (`/admin/user-login-activity`) p95 target: <= 300ms for common filters.
- Write path (`/login`, `/logout` activity insert) p95 target: <= 100ms incremental overhead.
- Alert on:
  - API p95 breaches
  - MySQL slow query spikes
  - archive job failures
