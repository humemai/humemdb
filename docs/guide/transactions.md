# Transactions

Transaction control is explicit and currently applies to the canonical SQLite write
path.

Public writes already target SQLite, so `db.transaction()` is the normal transaction
surface. The public API does not expose a DuckDB transaction route override.

## Context manager

```python
with db.transaction():
    db.query(
        "INSERT INTO users (name) VALUES ($name)",
        params={"name": "Alice"},
    )
```

On normal exit, the transaction commits. If an exception escapes the block, HumemDB
rolls the SQLite transaction back before the exception continues.

## Manual control

```python
db.begin()
try:
    db.query("UPDATE users SET active = $active", params={"active": 1})
    db.commit()
except Exception:
    db.rollback()
    raise
```

## Batch writes

`executemany(...)` is intentionally limited to SQLite for now. That keeps the initial
bulk-write story simple and aligned with the current source-of-truth model. That means
the common pattern is to combine `executemany(...)` with `db.transaction()` when one
batch should commit atomically.
