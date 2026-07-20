# User Login Activity Cursor API Contract

This document defines the scalable read contract for `GET /admin/user-login-activity`.

## Goals

- Keep response latency stable at high row counts.
- Avoid `OFFSET` pagination cost for deep pages.
- Keep existing filters compatible.
- Support gradual migration from page-based UI.

## Request Parameters

- `page_size` (optional, integer, default `25`, max `200`)
- `cursor` (optional, opaque string)
- `action` (`LOGIN` or `LOGOUT`)
- `status_value` (`SUCCESS` or `FAILED`)
- `user_email`
- `from_utc`
- `to_utc`
- `query`

Legacy compatibility:

- `page` is accepted but ignored when cursor mode is used.
- If both `page` and `cursor` are supplied, `cursor` wins.

## Cursor Format

Cursor encodes a stable boundary:

- `occurred_at_utc` timestamp
- `id`

Encoded as URL-safe base64 JSON:

```json
{ "occurred_at_utc": "2026-04-24T16:05:23.123456", "id": 12345 }
```

Server treats cursor as opaque; clients should not parse it.

## Sorting and Stability

Rows are always ordered:

1. `occurred_at_utc DESC`
2. `id DESC`

Next-page condition:

- `(occurred_at_utc < cursor.occurred_at_utc) OR (occurred_at_utc = cursor.occurred_at_utc AND id < cursor.id)`

This guarantees stable forward traversal under concurrent inserts.

## Response Shape

```json
{
  "items": [],
  "page_size": 25,
  "next_cursor": "opaque-or-null",
  "has_more": true,
  "total": null,
  "total_is_estimate": false
}
```

Notes:

- `total` is nullable by design (avoids expensive full count on hot path).
- `has_more` indicates whether another page is available.
- `next_cursor` is `null` when no more rows.

## Backward Compatibility

- Existing clients expecting `total/page/page_size` can continue during migration.
- New clients should use `next_cursor + has_more`.
- Once migration completes, page-number controls should be removed from this screen.

## Operational Guidance

- Keep default query path index-backed.
- Treat broad `%LIKE%` as best-effort and potentially slower.
- The `query` parameter matches **user name and email only** (not request id).
