from __future__ import annotations

import tempfile
from pathlib import Path

from humemdb import HumemDB


USER_COUNT = 2_000
ORDERS_PER_USER = 25


def build_users() -> list[tuple[int, str, str, str]]:
    cities = ("Berlin", "Paris", "Lisbon", "Seoul", "Tokyo")
    segments = ("enterprise", "startup", "research")
    rows: list[tuple[int, str, str, str]] = []
    for user_id in range(1, USER_COUNT + 1):
        rows.append(
            (
                user_id,
                f"User {user_id:04d}",
                segments[(user_id - 1) % len(segments)],
                cities[(user_id - 1) % len(cities)],
            )
        )
    return rows


def build_orders() -> list[tuple[int, int, str, int]]:
    rows: list[tuple[int, int, str, int]] = []
    order_id = 1
    for user_id in range(1, USER_COUNT + 1):
        for offset in range(ORDERS_PER_USER):
            status = "paid" if offset % 5 != 0 else "pending"
            total_cents = 1_500 + ((user_id * 97 + offset * 131) % 19_500)
            rows.append((order_id, user_id, status, total_cents))
            order_id += 1
    return rows


def main() -> None:
    users = build_users()
    orders = build_orders()

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        sqlite_path = root / "app.sqlite3"
        duckdb_path = root / "analytics.duckdb"

        with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
            db.query(
                (
                    "CREATE TABLE users ("
                    "id INTEGER PRIMARY KEY, "
                    "name TEXT NOT NULL, "
                    "segment TEXT NOT NULL, "
                    "city TEXT NOT NULL"
                    ")"
                )
            )
            db.query(
                (
                    "CREATE TABLE orders ("
                    "id INTEGER PRIMARY KEY, "
                    "user_id INTEGER NOT NULL, "
                    "status TEXT NOT NULL, "
                    "total_cents INTEGER NOT NULL"
                    ")"
                )
            )

            with db.transaction():
                db.executemany(
                    (
                        "INSERT INTO users (id, name, segment, city) "
                        "VALUES ($id, $name, $segment, $city)"
                    ),
                    [
                        {
                            "id": user_id,
                            "name": name,
                            "segment": segment,
                            "city": city,
                        }
                        for user_id, name, segment, city in users
                    ],
                )
                db.executemany(
                    (
                        "INSERT INTO orders (id, user_id, status, total_cents) "
                        "VALUES ($id, $user_id, $status, $total_cents)"
                    ),
                    [
                        {
                            "id": order_id,
                            "user_id": user_id,
                            "status": status,
                            "total_cents": total_cents,
                        }
                        for order_id, user_id, status, total_cents in orders
                    ],
                )

            sqlite_result = db.query(
                (
                    "SELECT id, name, segment, city "
                    "FROM users "
                    "WHERE city ILIKE 'berlin' "
                    "ORDER BY id "
                    "LIMIT 5"
                )
            )
            duckdb_result = db.query(
                (
                    "SELECT "
                    "  u.segment, "
                    "  COUNT(*) AS order_count, "
                    "  SUM(o.total_cents) AS gross_cents "
                    "FROM orders o "
                    "JOIN users u ON u.id = o.user_id "
                    "WHERE o.status = 'paid' "
                    "GROUP BY u.segment "
                    "ORDER BY gross_cents DESC"
                ),
                route="duckdb",
            )
            sqlite_join_result = db.query(
                (
                    "SELECT "
                    "  u.name, "
                    "  o.status, "
                    "  o.total_cents "
                    "FROM orders o "
                    "JOIN users u ON u.id = o.user_id "
                    "WHERE u.segment = 'enterprise' AND o.status = 'paid' "
                    "ORDER BY o.total_cents DESC "
                    "LIMIT 5"
                )
            )
            sqlite_counts = db.query(
                (
                    "SELECT "
                    "  COUNT(*) AS user_count, "
                    "  (SELECT COUNT(*) FROM orders) AS order_count "
                    "FROM users"
                )
            )

        assert sqlite_result.columns == ("id", "name", "segment", "city")
        assert sqlite_result.rows == (
            (1, "User 0001", "enterprise", "Berlin"),
            (6, "User 0006", "research", "Berlin"),
            (11, "User 0011", "startup", "Berlin"),
            (16, "User 0016", "enterprise", "Berlin"),
            (21, "User 0021", "research", "Berlin"),
        )
        assert duckdb_result.columns == ("segment", "order_count", "gross_cents")
        assert sqlite_counts.rows == ((USER_COUNT, USER_COUNT * ORDERS_PER_USER),)
        assert len(duckdb_result.rows) == 3
        assert duckdb_result.rows[0][1] > 10_000
        assert duckdb_result.rows[0][2] > duckdb_result.rows[-1][2]
        assert len(sqlite_join_result.rows) == 5
        assert sqlite_join_result.rows[0][1] == "paid"
        assert sqlite_join_result.rows[0][2] >= sqlite_join_result.rows[-1][2]

        print("SQLite counts:", sqlite_counts.rows)
        print("SQLite filtered users:", sqlite_result.rows)
        print("SQLite joined orders:", sqlite_join_result.rows)
        print("DuckDB aggregate:", duckdb_result.rows)


if __name__ == "__main__":
    main()
