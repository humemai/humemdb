from __future__ import annotations

import tempfile
from pathlib import Path
from time import perf_counter

from humemdb import HumemDB


DIMENSIONS = 8
SYNTHETIC_DIRECT_COUNT = 60_000


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


def _embedding(
    primary: float,
    secondary: float,
    tertiary: float,
    quaternary: float = 0.0,
) -> list[float]:
    return [
        primary,
        secondary,
        tertiary,
        quaternary,
        *([0.0] * (DIMENSIONS - 4)),
    ]


REFRESH_VECTOR = _embedding(0.92, 0.31, 0.18, 0.04)


def build_direct_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    catalog = (
        (1001, "graph", "gold", "guide", _embedding(1.0, 0.15, 0.05)),
        (1002, "graph", "gold", "guide", _embedding(0.98, 0.18, 0.04)),
        (1003, "graph", "silver", "reference", _embedding(0.9, 0.22, 0.02)),
        (1101, "sql", "gold", "guide", _embedding(0.2, 1.0, 0.05)),
        (1102, "sql", "silver", "reference", _embedding(0.25, 0.95, 0.08)),
        (1103, "sql", "bronze", "ops", _embedding(0.18, 0.82, 0.2)),
        (1201, "vector", "gold", "guide", _embedding(0.08, 0.18, 1.0)),
        (1202, "vector", "silver", "benchmark", _embedding(0.06, 0.25, 0.96)),
        (1203, "vector", "gold", "ops", _embedding(0.12, 0.12, 0.98, 0.08)),
    )
    for target_id, domain, tier, surface, embedding in catalog:
        rows.append(
            {
                "target_id": target_id,
                "embedding": embedding,
                "metadata": {
                    "domain": domain,
                    "tier": tier,
                    "surface": surface,
                },
            }
        )
    for offset in range(SYNTHETIC_DIRECT_COUNT):
        rows.append(
            {
                "target_id": 20_000 + offset,
                "embedding": _embedding(
                    0.02 + ((offset % 17) / 800.0),
                    0.55 + ((offset % 29) / 600.0),
                    0.18 + ((offset % 13) / 700.0),
                    (offset % 11) / 1000.0,
                ),
                "metadata": {
                    "domain": "archive",
                    "tier": "bulk",
                    "surface": "synthetic",
                },
            }
        )
    return rows


def run_direct_workflow(db) -> tuple[object, ...]:
    inserted_ids = db.insert_vectors(build_direct_rows())
    db.set_vector_metadata(
        [
            (1001, {"fresh": True, "owner": "docs"}),
            (1201, {"fresh": True, "owner": "benchmarks"}),
        ]
    )

    top_matches = db.search_vectors(
        _embedding(1.0, 0.12, 0.0),
        top_k=4,
        metric="cosine",
    )
    filtered_matches = db.search_vectors(
        _embedding(1.0, 0.12, 0.0),
        top_k=4,
        metric="cosine",
        filters={"domain": "graph", "tier": "gold"},
    )
    fresh_matches = db.search_vectors(
        _embedding(0.08, 0.18, 1.0),
        top_k=3,
        metric="cosine",
        filters={"fresh": True},
    )
    refreshed_ids = db.insert_vectors(
        [
            {
                "target_id": 1999,
                "embedding": REFRESH_VECTOR,
                "metadata": {
                    "domain": "graph",
                    "tier": "platinum",
                    "surface": "release",
                },
            }
        ]
    )
    refreshed_result = db.search_vectors(
        REFRESH_VECTOR,
        top_k=1,
        metric="cosine",
    )
    return (
        inserted_ids,
        top_matches,
        filtered_matches,
        fresh_matches,
        refreshed_ids,
        refreshed_result,
    )


def run_sql_workflow(db) -> tuple[object, ...]:
    with db.transaction():
        db.query(
            (
                "CREATE TABLE kb_chunks ("
                "id INTEGER PRIMARY KEY, "
                "collection TEXT NOT NULL, "
                "language TEXT NOT NULL, "
                "status TEXT NOT NULL, "
                "embedding BLOB)"
            )
        )
        db.executemany(
            (
                "INSERT INTO kb_chunks (id, collection, language, status, embedding) "
                "VALUES ($id, $collection, $language, $status, $embedding)"
            ),
            [
                {
                    "id": 1,
                    "collection": "graph-guides",
                    "language": "python",
                    "status": "published",
                    "embedding": _embedding(1.0, 0.1, 0.0),
                },
                {
                    "id": 2,
                    "collection": "graph-guides",
                    "language": "python",
                    "status": "published",
                    "embedding": _embedding(0.72, 0.28, 0.0),
                },
                {
                    "id": 3,
                    "collection": "graph-guides",
                    "language": "rust",
                    "status": "draft",
                    "embedding": _embedding(0.65, 0.35, 0.02),
                },
                {
                    "id": 4,
                    "collection": "vector-guides",
                    "language": "python",
                    "status": "published",
                    "embedding": _embedding(0.05, 0.18, 1.0),
                },
                {
                    "id": 5,
                    "collection": "vector-guides",
                    "language": "typescript",
                    "status": "published",
                    "embedding": _embedding(0.12, 0.2, 0.94),
                },
                {
                    "id": 6,
                    "collection": "vector-guides",
                    "language": "python",
                    "status": "archived",
                    "embedding": _embedding(0.0, 0.1, 0.88),
                },
            ],
        )

    before_update = db.query(
        (
            "SELECT id FROM kb_chunks WHERE collection = $collection "
            "AND status = 'published' ORDER BY embedding <=> $query LIMIT 3"
        ),
        params={
            "collection": "graph-guides",
            "query": _embedding(1.0, 0.1, 0.0),
        },
    )
    db.query(
        "UPDATE kb_chunks SET embedding = $embedding WHERE id = $id",
        params={"embedding": _embedding(1.0, 0.1, 0.0), "id": 2},
    )
    after_update = db.query(
        (
            "SELECT id FROM kb_chunks WHERE collection = $collection "
            "AND status = 'published' ORDER BY embedding <=> $query LIMIT 3"
        ),
        params={
            "collection": "graph-guides",
            "query": _embedding(1.0, 0.1, 0.0),
        },
    )
    db.query("DELETE FROM kb_chunks WHERE status = 'archived'")
    vector_guides = db.query(
        (
            "SELECT id FROM kb_chunks WHERE collection = $collection "
            "AND language = $language ORDER BY embedding <=> $query LIMIT 3"
        ),
        params={
            "collection": "vector-guides",
            "language": "python",
            "query": _embedding(0.0, 0.2, 1.0),
        },
    )
    counts = db.query(
        (
            "SELECT collection, COUNT(*) AS row_count FROM kb_chunks "
            "GROUP BY collection ORDER BY collection"
        )
    )
    return before_update, after_update, vector_guides, counts


def run_graph_workflow(db) -> tuple[object, ...]:
    profile_ids: dict[str, int] = {}
    with db.transaction():
        for name, cohort, role, embedding in (
            ("Ada", "alpha", "architect", _embedding(1.0, 0.1, 0.0)),
            ("Bea", "alpha", "analyst", _embedding(0.76, 0.24, 0.02)),
            ("Cory", "alpha", "ml", _embedding(0.08, 0.16, 1.0)),
            ("Drew", "beta", "ops", _embedding(0.0, 1.0, 0.2)),
        ):
            created = db.query(
                (
                    "CREATE (:Profile {"
                    "name: $name, cohort: $cohort, role: $role, embedding: $embedding})"
                ),
                params={
                    "name": name,
                    "cohort": cohort,
                    "role": role,
                    "embedding": embedding,
                },
            )
            profile_ids[name] = int(created.rows[0][0])

        for slug, area in (("routing", "graph"), ("retrieval", "vector")):
            db.query(
                "CREATE (:Topic {slug: $slug, area: $area})",
                params={"slug": slug, "area": area},
            )

        db.query(
            (
                "MATCH (a:Profile {name: 'Ada'}), (b:Profile {name: 'Bea'}) "
                "CREATE (a)-[:COLLABORATES {since: 2023, cadence: 'weekly'}]->(b)"
            )
        )
        db.query(
            (
                "MATCH (a:Profile {name: 'Bea'}), (b:Profile {name: 'Cory'}) "
                "CREATE (a)-[:COLLABORATES {since: 2024, cadence: 'daily'}]->(b)"
            )
        )
        db.query(
            (
                "MATCH (p:Profile {name: 'Ada'}), (t:Topic {slug: 'routing'}) "
                "CREATE (p)-[:TRACKS {priority: 1}]->(t)"
            )
        )
        db.query(
            (
                "MATCH (p:Profile {name: 'Cory'}), (t:Topic {slug: 'retrieval'}) "
                "CREATE (p)-[:TRACKS {priority: 2}]->(t)"
            )
        )

    initial_vector_result = db.query(
        (
            "MATCH (p:Profile {cohort: 'alpha'}) "
            "SEARCH p IN (VECTOR INDEX embedding FOR $query LIMIT 3) "
            "RETURN p.id ORDER BY p.id"
        ),
        params={"query": _embedding(1.0, 0.1, 0.0)},
    )
    db.query(
        "MATCH (p:Profile {name: 'Bea'}) SET p.embedding = $embedding",
        params={"embedding": _embedding(1.0, 0.1, 0.0)},
    )
    updated_vector_result = db.query(
        (
            "MATCH (p:Profile {cohort: 'alpha'}) "
            "SEARCH p IN (VECTOR INDEX embedding FOR $query LIMIT 3) "
            "RETURN p.id ORDER BY p.id"
        ),
        params={"query": _embedding(1.0, 0.1, 0.0)},
    )
    db.query(
        (
            "MATCH (a:Profile)-[r:COLLABORATES]->(b:Profile) "
            "WHERE a.name = 'Bea' DELETE r"
        )
    )
    db.query("MATCH (p:Profile {name: 'Drew'}) DETACH DELETE p")
    collaboration_rows = db.query(
        (
            "MATCH (a:Profile)-[r:COLLABORATES]->(b:Profile) "
            "RETURN a.name, r.cadence, b.name ORDER BY a.name, b.name"
        )
    )
    remaining_profiles = db.query(
        "MATCH (p:Profile) RETURN p.name ORDER BY p.name"
    )
    return (
        profile_ids,
        initial_vector_result,
        updated_vector_result,
        collaboration_rows,
        remaining_profiles,
    )


def main() -> None:
    report = _make_timer()
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)

        with HumemDB.open(root / "vectors-direct") as db:
            (
                inserted_ids,
                top_matches,
                filtered_matches,
                fresh_matches,
                refreshed_ids,
                refreshed_result,
            ) = run_direct_workflow(db)
            report("ran direct-vector workflow")

        with HumemDB.open(root / "vectors-rows") as db:
            (
                sql_before_update,
                sql_after_update,
                sql_vector_guides,
                sql_counts,
            ) = run_sql_workflow(db)
            report("ran SQL-owned vector workflow")

        with HumemDB.open(root / "vectors-graph") as db:
            (
                profile_ids,
                cypher_initial,
                cypher_updated,
                cypher_collaborations,
                cypher_remaining_profiles,
            ) = run_graph_workflow(db)
            report("ran graph-owned vector workflow")

        assert inserted_ids == (
            1001,
            1002,
            1003,
            1101,
            1102,
            1103,
            1201,
            1202,
            1203,
            *tuple(20_000 + offset for offset in range(SYNTHETIC_DIRECT_COUNT)),
        )
        assert len(top_matches.rows) == 4
        assert top_matches.rows[0][:3] == ("direct", "", 1001)
        assert tuple(row[2] for row in filtered_matches.rows) == (1001, 1002)
        assert tuple(row[2] for row in fresh_matches.rows) == (1201, 1001)
        assert refreshed_ids == (1999,)
        assert refreshed_result.rows[0][:3] == ("direct", "", 1999)
        assert abs(refreshed_result.rows[0][3] - 1.0) < 1e-6

        assert tuple(row[:3] for row in sql_before_update.rows) == (
            ("sql_row", "kb_chunks", 1),
            ("sql_row", "kb_chunks", 2),
        )
        assert tuple(row[:3] for row in sql_after_update.rows) == (
            ("sql_row", "kb_chunks", 1),
            ("sql_row", "kb_chunks", 2),
        )
        assert abs(sql_after_update.rows[0][3] - 1.0) < 1e-6
        assert abs(sql_after_update.rows[1][3] - 1.0) < 1e-6
        assert tuple(row[:3] for row in sql_vector_guides.rows) == (
            ("sql_row", "kb_chunks", 4),
        )
        assert sql_counts.rows == (("graph-guides", 3), ("vector-guides", 2))

        assert tuple(row[:3] for row in cypher_initial.rows) == (
            ("graph_node", "", profile_ids["Ada"]),
            ("graph_node", "", profile_ids["Bea"]),
            ("graph_node", "", profile_ids["Cory"]),
        )
        assert tuple(row[:3] for row in cypher_updated.rows) == (
            ("graph_node", "", profile_ids["Ada"]),
            ("graph_node", "", profile_ids["Bea"]),
            ("graph_node", "", profile_ids["Cory"]),
        )
        assert abs(cypher_updated.rows[0][3] - 1.0) < 1e-6
        assert abs(cypher_updated.rows[1][3] - 1.0) < 1e-6
        assert cypher_collaborations.rows == (("Ada", "weekly", "Bea"),)
        assert cypher_remaining_profiles.rows == (("Ada",), ("Bea",), ("Cory",))

        print("Direct top matches:", top_matches.rows)
        print("Direct filtered matches:", filtered_matches.rows)
        print("Direct fresh matches:", fresh_matches.rows)
        print("Direct vector count:", len(inserted_ids))
        print("SQL graph-guide matches:", sql_after_update.rows)
        print("SQL vector-guide matches:", sql_vector_guides.rows)
        print("Cypher alpha matches:", cypher_updated.rows)
        print("Cypher collaboration rows:", cypher_collaborations.rows)


if __name__ == "__main__":
    main()
