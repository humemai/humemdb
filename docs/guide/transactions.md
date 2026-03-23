# Transactions

Transaction control is explicit and route-bound.

## Context manager

```python
with db.transaction():
    db.query(
        "INSERT INTO users (name) VALUES ($name)",
        params={"name": "Alice"},
    )
```

On normal exit, the transaction commits. If an exception escapes the block, HumemDB
rolls the selected route back before the exception continues.

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
bulk-write story simple and aligned with the current source-of-truth model.