# Postgres design for `ledger.Store`

Table `ledger_entries(member_id, payment_id, kind, points, month, created_at)` with
`UNIQUE (payment_id, kind)` and an index on `(member_id, month)`.

- **Award**: one transaction. Take `pg_advisory_xact_lock(hashtextextended(member_id, 0))`,
  sum the member's awards for `month`, then insert `max(min(points, cap - used), 0)` with
  `ON CONFLICT (payment_id, kind) DO NOTHING RETURNING points`. No row back means `dup`.
  Zero-point awards are still inserted, so a republish cannot earn later.
- **Reverse**: same transaction and lock. Read the award row (none: `ErrNoAward`), insert
  `-points` in the award's month with `ON CONFLICT DO NOTHING`.
- **Balance**: `SELECT COALESCE(SUM(points), 0) WHERE member_id = $1`; no lock.

## Why it stays correct with several replicas

- The lock serializes cap checks per member, so two replicas cannot read the same `used`
  and overshoot. Different members take different locks and never wait on each other.
- Concurrent redelivery of one payment: the second transaction waits, then hits the unique
  index and writes nothing. The index also covers a payment_id reused under another member.
- Reverse takes the same lock, so it never sees a half-committed award and wrongly acks.
- Lost connections and deadlocks map to `ErrUnavailable`, so the handler nacks. Every
  statement is idempotent, so redelivery after an unknown commit is safe.
