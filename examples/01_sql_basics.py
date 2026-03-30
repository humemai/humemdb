from __future__ import annotations

import tempfile
from pathlib import Path
from time import perf_counter

from humemdb import HumemDB


USER_COUNT = 5_000
ORDER_COUNT_PER_USER = 10
PRODUCT_COUNT = 128
ITEMS_PER_ORDER = 3


def _make_timer() -> callable:
    start = perf_counter()
    last = start

    def report(step: str) -> None:
        nonlocal last
        now = perf_counter()
        print(
            f"[timing] {step}: +{now - last:.3f}s step, {now - start:.3f}s total"
        )
        last = now

    return report


def build_users() -> list[tuple[int, str, str, str, str, bool]]:
    cities = ("Berlin", "Paris", "Lisbon", "Seoul", "Tokyo", "Oslo")
    countries = ("DE", "FR", "PT", "KR", "JP", "NO")
    segments = ("enterprise", "startup", "research", "public-sector")
    rows: list[tuple[int, str, str, str, str, bool]] = []
    for user_id in range(1, USER_COUNT + 1):
        rows.append(
            (
                user_id,
                f"User {user_id:04d}",
                segments[(user_id - 1) % len(segments)],
                cities[(user_id - 1) % len(cities)],
                countries[(user_id - 1) % len(countries)],
                user_id % 7 != 0,
            )
        )
    return rows


def build_products() -> list[tuple[int, str, str, int]]:
    categories = ("storage", "compute", "analytics", "security")
    rows: list[tuple[int, str, str, int]] = []
    for product_id in range(1, PRODUCT_COUNT + 1):
        category = categories[(product_id - 1) % len(categories)]
        price_cents = 1_500 + ((product_id * 337) % 12_000)
        rows.append((product_id, f"SKU-{product_id:03d}", category, price_cents))
    return rows


def build_orders() -> list[tuple[int, int, str, str, int]]:
    rows: list[tuple[int, int, str, str, int]] = []
    order_id = 1
    for user_id in range(1, USER_COUNT + 1):
        for offset in range(ORDER_COUNT_PER_USER):
            status = "paid" if offset % 5 not in (0, 4) else "pending"
            order_day = f"2026-03-{((offset % 9) + 1):02d}"
            discount_cents = 0 if offset % 4 else 250
            rows.append((order_id, user_id, status, order_day, discount_cents))
            order_id += 1
    return rows


def build_order_items(
    orders: list[tuple[int, int, str, str, int]],
    products: list[tuple[int, str, str, int]],
) -> list[tuple[int, int, int, int, int]]:
    rows: list[tuple[int, int, int, int, int]] = []
    item_id = 1
    product_prices = {
        product_id: price_cents
        for product_id, _sku, _category, price_cents in products
    }

    for order_id, user_id, _status, _order_day, discount_cents in orders:
        for slot in range(ITEMS_PER_ORDER):
            product_id = ((order_id * 11 + user_id + slot * 7) % PRODUCT_COUNT) + 1
            quantity = 1 + ((order_id + slot) % 3)
            line_total_cents = product_prices[product_id] * quantity
            if slot == 0:
                line_total_cents = max(100, line_total_cents - discount_cents)
            rows.append(
                (item_id, order_id, product_id, quantity, line_total_cents)
            )
            item_id += 1
    return rows


def main() -> None:
    report = _make_timer()
    users = build_users()
    products = build_products()
    orders = build_orders()
    order_items = build_order_items(orders, products)
    report("built source datasets")

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)

        with HumemDB(root / "app") as db:
            with db.transaction():
                db.query(
                    (
                        "CREATE TABLE users ("
                        "id INTEGER PRIMARY KEY, "
                        "name TEXT NOT NULL, "
                        "segment TEXT NOT NULL, "
                        "city TEXT NOT NULL, "
                        "country TEXT NOT NULL, "
                        "active BOOLEAN NOT NULL"
                        ")"
                    )
                )
                db.query(
                    (
                        "CREATE TABLE products ("
                        "id INTEGER PRIMARY KEY, "
                        "sku TEXT NOT NULL, "
                        "category TEXT NOT NULL, "
                        "price_cents INTEGER NOT NULL"
                        ")"
                    )
                )
                db.query(
                    (
                        "CREATE TABLE orders ("
                        "id INTEGER PRIMARY KEY, "
                        "user_id INTEGER NOT NULL, "
                        "status TEXT NOT NULL, "
                        "order_day TEXT NOT NULL, "
                        "discount_cents INTEGER NOT NULL"
                        ")"
                    )
                )
                db.query(
                    (
                        "CREATE TABLE order_items ("
                        "id INTEGER PRIMARY KEY, "
                        "order_id INTEGER NOT NULL, "
                        "product_id INTEGER NOT NULL, "
                        "quantity INTEGER NOT NULL, "
                        "line_total_cents INTEGER NOT NULL"
                        ")"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO users (id, name, segment, city, country, active) "
                        "VALUES ($id, $name, $segment, $city, $country, $active)"
                    ),
                    [
                        {
                            "id": user_id,
                            "name": name,
                            "segment": segment,
                            "city": city,
                            "country": country,
                            "active": active,
                        }
                        for user_id, name, segment, city, country, active in users
                    ],
                )
                db.executemany(
                    (
                        "INSERT INTO products (id, sku, category, price_cents) "
                        "VALUES ($id, $sku, $category, $price_cents)"
                    ),
                    [
                        {
                            "id": product_id,
                            "sku": sku,
                            "category": category,
                            "price_cents": price_cents,
                        }
                        for product_id, sku, category, price_cents in products
                    ],
                )
                db.executemany(
                    (
                        "INSERT INTO orders ("
                        "id, user_id, status, order_day, discount_cents"
                        ") VALUES ("
                        "$id, $user_id, $status, $order_day, $discount_cents"
                        ")"
                    ),
                    [
                        {
                            "id": order_id,
                            "user_id": user_id,
                            "status": status,
                            "order_day": order_day,
                            "discount_cents": discount_cents,
                        }
                        for (
                            order_id,
                            user_id,
                            status,
                            order_day,
                            discount_cents,
                        ) in orders
                    ],
                )
                db.executemany(
                    (
                        "INSERT INTO order_items ("
                        "id, order_id, product_id, quantity, line_total_cents"
                        ") VALUES ("
                        "$id, $order_id, $product_id, $quantity, $line_total_cents"
                        ")"
                    ),
                    [
                        {
                            "id": item_id,
                            "order_id": order_id,
                            "product_id": product_id,
                            "quantity": quantity,
                            "line_total_cents": line_total_cents,
                        }
                        for (
                            item_id,
                            order_id,
                            product_id,
                            quantity,
                            line_total_cents,
                        ) in order_items
                    ],
                )
            report("created schema and inserted rows")

            filtered_users_result = db.query(
                (
                    "SELECT u.id, u.name, u.segment, u.city "
                    "FROM users u "
                    "WHERE u.city ILIKE 'berlin' "
                    "AND EXISTS ("
                    "  SELECT 1 FROM orders o "
                    "  WHERE o.user_id = u.id AND o.status = 'paid'"
                    ") "
                    "ORDER BY u.id "
                    "LIMIT 5"
                )
            )
            report("ran selective user read")
            segment_revenue_result = db.query(
                (
                    "WITH paid_items AS ("
                    "  SELECT "
                    "    u.segment, "
                    "    u.country, "
                    "    o.order_day, "
                    "    oi.line_total_cents "
                    "  FROM order_items oi "
                    "  JOIN orders o ON o.id = oi.order_id "
                    "  JOIN users u ON u.id = o.user_id "
                    "  WHERE o.status = 'paid' AND u.active = true"
                    "), "
                    "segment_daily AS ("
                    "  SELECT "
                    "    segment, "
                    "    country, "
                    "    order_day, "
                    "    SUM(line_total_cents) AS gross_cents "
                    "  FROM paid_items "
                    "  GROUP BY segment, country, order_day"
                    ") "
                    "SELECT "
                    "  segment, "
                    "  country, "
                    "  order_day, "
                    "  gross_cents, "
                    "  ROW_NUMBER() OVER ("
                    "    PARTITION BY segment ORDER BY gross_cents DESC, order_day"
                    "  ) AS revenue_rank "
                    "FROM segment_daily "
                    "ORDER BY segment, revenue_rank "
                    "LIMIT 8"
                )
            )
            report("ran analytical revenue read")
            top_order_items_result = db.query(
                (
                    "SELECT "
                    "  u.name, "
                    "  p.category, "
                    "  oi.quantity, "
                    "  oi.line_total_cents "
                    "FROM order_items oi "
                    "JOIN orders o ON o.id = oi.order_id "
                    "JOIN products p ON p.id = oi.product_id "
                    "JOIN users u ON u.id = o.user_id "
                    "WHERE u.segment = 'enterprise' AND o.status = 'paid' "
                    "ORDER BY oi.line_total_cents DESC, u.name "
                    "LIMIT 5"
                )
            )
            report("ran joined order-item read")
            product_mix_result = db.query(
                (
                    "WITH category_totals AS ("
                    "  SELECT p.category, SUM(oi.line_total_cents) AS gross_cents "
                    "  FROM order_items oi "
                    "  JOIN products p ON p.id = oi.product_id "
                    "  GROUP BY p.category"
                    "), product_mix AS ("
                    "  SELECT category, gross_cents FROM category_totals "
                    "  UNION ALL "
                    "  SELECT 'all', SUM(gross_cents) FROM category_totals"
                    ") "
                    "SELECT category, gross_cents FROM product_mix "
                    "ORDER BY gross_cents DESC"
                )
            )
            report("ran UNION ALL rollup")
            row_count_result = db.query(
                (
                    "SELECT "
                    "  COUNT(*) AS user_count, "
                    "  (SELECT COUNT(*) FROM orders) AS order_count, "
                    "  (SELECT COUNT(*) FROM order_items) AS order_item_count "
                    "FROM users"
                )
            )
            report("ran row-count summary")

        assert filtered_users_result.columns == ("id", "name", "segment", "city")
        assert filtered_users_result.rows == (
            (1, "User 0001", "enterprise", "Berlin"),
            (7, "User 0007", "research", "Berlin"),
            (13, "User 0013", "enterprise", "Berlin"),
            (19, "User 0019", "research", "Berlin"),
            (25, "User 0025", "enterprise", "Berlin"),
        )
        assert segment_revenue_result.columns == (
            "segment",
            "country",
            "order_day",
            "gross_cents",
            "revenue_rank",
        )
        assert row_count_result.rows == ((
            USER_COUNT,
            USER_COUNT * ORDER_COUNT_PER_USER,
            USER_COUNT * ORDER_COUNT_PER_USER * ITEMS_PER_ORDER,
        ),)
        assert len(segment_revenue_result.rows) == 8
        assert all(row[4] >= 1 for row in segment_revenue_result.rows)
        assert segment_revenue_result.rows[0][3] >= segment_revenue_result.rows[1][3]
        assert len(top_order_items_result.rows) == 5
        assert top_order_items_result.rows[0][3] >= top_order_items_result.rows[-1][3]
        assert product_mix_result.rows[0][0] == "all"
        assert product_mix_result.rows[0][1] > product_mix_result.rows[1][1]

        print("Row counts:", row_count_result.rows)
        print("Filtered users:", filtered_users_result.rows)
        print("Top joined order items:", top_order_items_result.rows)
        print("Daily segment revenue:", segment_revenue_result.rows)
        print("Unioned product mix:", product_mix_result.rows)


if __name__ == "__main__":
    main()
