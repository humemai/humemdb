# Transactions

Transaction control is explicit and route-scoped.

## Context manager

```python
with db.transaction(route="sqlite"):
    db.query(
        "INSERT INTO users (name) VALUES (?)",
        route="sqlite",
        params=("Alice",),
    )
```

On normal exit, the transaction commits. If an exception escapes the block, HumemDB
rolls the selected route back before the exception continues.

## Manual control

```python
db.begin(route="sqlite")
try:
    db.query("UPDATE users SET active = 1", route="sqlite")
    db.commit(route="sqlite")
except Exception:
    db.rollback(route="sqlite")
    raise
```

## Batch writes

`executemany(...)` is intentionally limited to SQLite for now. That keeps the initial
bulk-write story simple and aligned with the current source-of-truth model.