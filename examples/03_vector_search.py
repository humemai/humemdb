from __future__ import annotations

import tempfile
from pathlib import Path
from time import perf_counter

from humemdb import HumemDB
from humemdb.vector import IndexedVectorRuntimeConfig


DIMENSIONS = 8
SYNTHETIC_DIRECT_COUNT = 20_000
SQL_FILLER_COUNT = 1_024
GRAPH_FILLER_COUNT = 1_024
DEMO_HOT_MAX_ROWS = 256
DEMO_MERGE_BUFFER_FACTOR = 2
DIRECT_INDEX_NAME = "direct_similarity_idx"
SQL_INDEX_NAME = "docs_embedding_idx"
CYPHER_INDEX_NAME = "profile_embedding_idx"


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


def print_section(title: str) -> None:
    print()
    print(f"=== {title} ===")


def configure_demo_tiering(db: HumemDB) -> None:
    db._vector_runtime_config = IndexedVectorRuntimeConfig(
        hot_max_rows=DEMO_HOT_MAX_ROWS,
        merge_buffer_factor=DEMO_MERGE_BUFFER_FACTOR,
    )


def run_direct_workflow(db) -> tuple[object, ...]:
    inserted_ids = db.insert_vectors(build_direct_rows())
    initial_index_state = db.inspect_vector_index(index_name=DIRECT_INDEX_NAME)
    built_index = db.build_vector_index(index_name=DIRECT_INDEX_NAME)
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
    paused_index = db.pause_vector_index(index_name=DIRECT_INDEX_NAME)
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
    paused_state = db.inspect_vector_index(index_name=DIRECT_INDEX_NAME)
    refreshed_result = db.search_vectors(
        REFRESH_VECTOR,
        top_k=1,
        metric="cosine",
    )
    awaited_refresh = db.await_vector_index_refresh()
    resumed_index = db.resume_vector_index(index_name=DIRECT_INDEX_NAME)
    refreshed_index = db.refresh_vector_index(index_name=DIRECT_INDEX_NAME)
    dropped_index = db.drop_vector_index(index_name=DIRECT_INDEX_NAME)
    rebuilt_index = db.refresh_vector_index(index_name=DIRECT_INDEX_NAME)
    final_index_state = db.inspect_vector_index(index_name=DIRECT_INDEX_NAME)
    return (
        inserted_ids,
        initial_index_state,
        built_index,
        top_matches,
        filtered_matches,
        fresh_matches,
        paused_index,
        refreshed_ids,
        paused_state,
        refreshed_result,
        awaited_refresh,
        resumed_index,
        refreshed_index,
        dropped_index,
        rebuilt_index,
        final_index_state,
    )


def run_direct_vector_example(root: Path, report: callable) -> None:
    with HumemDB(root / "vectors-direct") as db:
        configure_demo_tiering(db)
        (
            inserted_ids,
            direct_initial_index,
            direct_built_index,
            top_matches,
            filtered_matches,
            fresh_matches,
            direct_paused_index,
            refreshed_ids,
            direct_paused_state,
            refreshed_result,
            direct_awaited_refresh,
            direct_resumed_index,
            direct_refreshed_index,
            direct_dropped_index,
            direct_rebuilt_index,
            direct_final_index,
        ) = run_direct_workflow(db)
        report("ran direct-vector workflow")

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
    assert direct_initial_index["name"] == DIRECT_INDEX_NAME
    assert direct_built_index["state"] == "ready"
    assert direct_built_index["cold_snapshot_rows"] > 0
    assert direct_built_index["hot_rows"] <= DEMO_HOT_MAX_ROWS
    assert direct_paused_index["maintenance_paused"] is True
    assert direct_paused_state["maintenance_paused"] is True
    assert direct_paused_state["pending_cold_rows"] > 0
    assert direct_awaited_refresh is False
    assert direct_resumed_index["maintenance_paused"] is False
    assert direct_refreshed_index["pending_cold_rows"] == 0
    assert direct_dropped_index["state"] == "disabled"
    assert direct_rebuilt_index["state"] == "ready"
    assert direct_final_index["state"] == "ready"

    print_section("1. Direct Vectors: unstructured vector memory")
    print("Use this when you want raw vector insert/search without tables or graphs.")
    print(
        "This demo lowers hot_max_rows so the cold ANN tier activates on a small dataset."
    )
    print("Direct vector index initial state:", direct_initial_index)
    print("Direct vector index lifecycle:", direct_built_index)
    print("Direct vector index paused for ingest:", direct_paused_index)
    print("Direct vector index state during ingest pause:", direct_paused_state)
    print("Direct vector index awaited refresh:", direct_awaited_refresh)
    print("Direct vector index resumed:", direct_resumed_index)
    print("Direct vector index refreshed after ingest:", direct_refreshed_index)
    print("Direct vector index dropped:", direct_dropped_index)
    print("Direct vector index rebuilt:", direct_rebuilt_index)
    print("Direct vector index final state:", direct_final_index)
    print("Direct top matches:", top_matches.rows)
    print("Direct filtered matches:", filtered_matches.rows)
    print("Direct fresh matches:", fresh_matches.rows)
    print("Direct vector count:", len(inserted_ids))


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
                *[
                    {
                        "id": 1_000 + idx,
                        "collection": "bulk-fill",
                        "language": "none",
                        "status": "published",
                        "embedding": _embedding(0.0, 1.0, 0.0, idx / 10_000.0),
                    }
                    for idx in range(SQL_FILLER_COUNT)
                ],
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
            "WHERE collection IN ('graph-guides', 'vector-guides') "
            "GROUP BY collection ORDER BY collection"
        )
    )
    created_index = db.query(
        f"CREATE INDEX {SQL_INDEX_NAME} ON kb_chunks USING ivfpq "
        "(embedding vector_cosine_ops)"
    )
    created_index_if_missing = db.query(
        f"CREATE INDEX IF NOT EXISTS {SQL_INDEX_NAME} ON kb_chunks USING ivfpq "
        "(embedding vector_cosine_ops)"
    )
    listed_indexes = db.query("SELECT * FROM humemdb_vector_indexes")
    paused_index = db.query(f"ALTER VECTOR INDEX {SQL_INDEX_NAME} PAUSE MAINTENANCE")
    db.query(
        (
            "INSERT INTO kb_chunks (id, collection, language, status, embedding) "
            "VALUES ($id, $collection, $language, $status, $embedding)"
        ),
        params={
            "id": 9_999,
            "collection": "bulk-fill",
            "language": "none",
            "status": "published",
            "embedding": REFRESH_VECTOR,
        },
    )
    listed_indexes_paused = db.query("SELECT * FROM humemdb_vector_indexes")
    resumed_index = db.query(
        f"ALTER VECTOR INDEX {SQL_INDEX_NAME} RESUME MAINTENANCE"
    )
    refreshed_index = db.query(f"REFRESH VECTOR INDEX {SQL_INDEX_NAME}")
    rebuilt_index = db.query(f"REBUILD VECTOR INDEX {SQL_INDEX_NAME}")
    dropped_index = db.query(f"DROP INDEX {SQL_INDEX_NAME}")
    dropped_index_if_present = db.query(f"DROP INDEX IF EXISTS {SQL_INDEX_NAME}")
    listed_indexes_after_drop = db.query("SELECT * FROM humemdb_vector_indexes")
    return (
        before_update,
        after_update,
        vector_guides,
        counts,
        created_index,
        created_index_if_missing,
        listed_indexes,
        paused_index,
        listed_indexes_paused,
        resumed_index,
        refreshed_index,
        rebuilt_index,
        dropped_index,
        dropped_index_if_present,
        listed_indexes_after_drop,
    )


def run_sql_vector_example(root: Path, report: callable) -> None:
    with HumemDB(root / "vectors-rows") as db:
        configure_demo_tiering(db)
        (
            sql_before_update,
            sql_after_update,
            sql_vector_guides,
            sql_counts,
            sql_created_index,
            sql_created_index_if_missing,
            sql_listed_indexes,
            sql_paused_index,
            sql_listed_indexes_paused,
            sql_resumed_index,
            sql_refreshed_index,
            sql_rebuilt_index,
            sql_dropped_index,
            sql_dropped_index_if_present,
            sql_listed_indexes_after_drop,
        ) = run_sql_workflow(db)
        report("ran SQL-owned vector workflow")

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
    assert sql_created_index.rows[0][0] == SQL_INDEX_NAME
    assert sql_created_index_if_missing.rows[0][0] == SQL_INDEX_NAME
    assert len(sql_listed_indexes.rows) == 1
    assert sql_paused_index.rows[0][-1] is True
    assert sql_listed_indexes_paused.rows[0][-1] is True
    assert sql_listed_indexes_paused.rows[0][8] > 0
    assert sql_resumed_index.rows[0][-1] is False
    assert sql_refreshed_index.rows[0][3] == "ready"
    assert sql_refreshed_index.rows[0][7] > 0
    assert sql_rebuilt_index.rows[0][3] == "ready"
    assert sql_rebuilt_index.rows[0][7] > 0
    assert sql_dropped_index.rows[0][3] == "disabled"
    assert sql_dropped_index_if_present.rows[0][3] == "disabled"
    assert sql_listed_indexes_after_drop.rows == ()

    print_section("2. SQL Vectors: table-owned embeddings")
    print("Use this when vectors belong to relational rows and SQL does the filtering.")
    print(
        "This demo lowers hot_max_rows and adds filler rows so the cold ANN "
        "tier becomes visible."
    )
    print("SQL vector index lifecycle:", sql_created_index.rows)
    print(
        "SQL vector index lifecycle (IF NOT EXISTS):",
        sql_created_index_if_missing.rows,
    )
    print("SQL vector index catalog:", sql_listed_indexes.rows)
    print("SQL vector index paused for ingest:", sql_paused_index.rows)
    print("SQL vector index catalog while paused:", sql_listed_indexes_paused.rows)
    print("SQL vector index resumed:", sql_resumed_index.rows)
    print("SQL vector index refresh:", sql_refreshed_index.rows)
    print("SQL vector index rebuild:", sql_rebuilt_index.rows)
    print("SQL vector index dropped:", sql_dropped_index.rows)
    print(
        "SQL vector index dropped (IF EXISTS):",
        sql_dropped_index_if_present.rows,
    )
    print("SQL vector index catalog after drop:", sql_listed_indexes_after_drop.rows)
    print("SQL graph-guide matches:", sql_after_update.rows)
    print("SQL vector-guide matches:", sql_vector_guides.rows)


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

        for idx in range(GRAPH_FILLER_COUNT):
            db.query(
                (
                    "CREATE (:ArchiveProfile {"
                    "name: $name, cohort: $cohort, role: $role, embedding: $embedding})"
                ),
                params={
                    "name": f"Archive {idx}",
                    "cohort": "archive",
                    "role": "filler",
                    "embedding": _embedding(0.0, 0.0, 1.0, idx / 10_000.0),
                },
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

    created_index = db.query(
        f"CREATE VECTOR INDEX {CYPHER_INDEX_NAME} IF NOT EXISTS "
        "FOR (p:Profile) ON (p.embedding) "
        "OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine'}}"
    )
    created_index_if_missing = db.query(
        f"CREATE VECTOR INDEX {CYPHER_INDEX_NAME} IF NOT EXISTS "
        "FOR (p:Profile) ON (p.embedding) "
        "OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine'}}"
    )

    initial_vector_result = db.query(
        (
            f"CALL db.index.vector.queryNodes('{CYPHER_INDEX_NAME}', 3, $query) "
            "YIELD node, score RETURN node.id, score"
        ),
        params={"query": _embedding(1.0, 0.1, 0.0)},
    )
    db.query(
        "MATCH (p:Profile {name: 'Bea'}) SET p.embedding = $embedding",
        params={"embedding": _embedding(1.0, 0.1, 0.0)},
    )
    updated_vector_result = db.query(
        (
            f"CALL db.index.vector.queryNodes('{CYPHER_INDEX_NAME}', 3, $query) "
            "YIELD node, score RETURN node.id, score"
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
    shown_indexes = db.query("SHOW VECTOR INDEXES")
    paused_index = db.query(
        f"ALTER VECTOR INDEX {CYPHER_INDEX_NAME} PAUSE MAINTENANCE"
    )
    db.query(
        (
            "CREATE (:Profile {"
            "name: $name, cohort: $cohort, role: $role, embedding: $embedding})"
        ),
        params={
            "name": "Eli",
            "cohort": "gamma",
            "role": "release",
            "embedding": REFRESH_VECTOR,
        },
    )
    shown_indexes_paused = db.query("SHOW VECTOR INDEXES")
    resumed_index = db.query(
        f"ALTER VECTOR INDEX {CYPHER_INDEX_NAME} RESUME MAINTENANCE"
    )
    refreshed_index = db.query(f"REFRESH VECTOR INDEX {CYPHER_INDEX_NAME}")
    rebuilt_index = db.query(f"REBUILD VECTOR INDEX {CYPHER_INDEX_NAME}")
    dropped_index = db.query(f"DROP VECTOR INDEX {CYPHER_INDEX_NAME}")
    dropped_index_if_present = db.query(
        f"DROP VECTOR INDEX IF EXISTS {CYPHER_INDEX_NAME}"
    )
    shown_indexes_after_drop = db.query("SHOW VECTOR INDEXES")
    return (
        profile_ids,
        initial_vector_result,
        updated_vector_result,
        collaboration_rows,
        remaining_profiles,
        created_index,
        created_index_if_missing,
        shown_indexes,
        paused_index,
        shown_indexes_paused,
        resumed_index,
        refreshed_index,
        rebuilt_index,
        dropped_index,
        dropped_index_if_present,
        shown_indexes_after_drop,
    )


def run_cypher_vector_example(root: Path, report: callable) -> None:
    with HumemDB(root / "vectors-graph") as db:
        configure_demo_tiering(db)
        (
            profile_ids,
            cypher_initial,
            cypher_updated,
            cypher_collaborations,
            cypher_remaining_profiles,
            cypher_created_index,
            cypher_created_index_if_missing,
            cypher_shown_indexes,
            cypher_paused_index,
            cypher_shown_indexes_paused,
            cypher_resumed_index,
            cypher_refreshed_index,
            cypher_rebuilt_index,
            cypher_dropped_index,
            cypher_dropped_index_if_present,
            cypher_shown_indexes_after_drop,
        ) = run_graph_workflow(db)
        report("ran graph-owned vector workflow")

    assert tuple(row[0] for row in cypher_initial.rows) == (
        profile_ids["Ada"],
        profile_ids["Bea"],
           profile_ids["Drew"],
    )
    assert tuple(row[0] for row in cypher_updated.rows) == (
        profile_ids["Ada"],
        profile_ids["Bea"],
           profile_ids["Drew"],
    )
    assert abs(cypher_updated.rows[0][1] - 1.0) < 1e-6
    assert abs(cypher_updated.rows[1][1] - 1.0) < 1e-6
    assert cypher_collaborations.rows == (("Ada", "weekly", "Bea"),)
    assert cypher_remaining_profiles.rows == (("Ada",), ("Bea",), ("Cory",))
    assert cypher_created_index.rows[0][0] == CYPHER_INDEX_NAME
    assert cypher_created_index_if_missing.rows[0][0] == CYPHER_INDEX_NAME
    assert len(cypher_shown_indexes.rows) == 1
    assert cypher_created_index.rows[0][3] == "ready"
    assert cypher_created_index.rows[0][7] > 0
    assert cypher_paused_index.rows[0][-1] is True
    assert cypher_shown_indexes_paused.rows[0][-1] is True
    assert cypher_shown_indexes_paused.rows[0][8] > 0
    assert cypher_resumed_index.rows[0][-1] is False
    assert cypher_refreshed_index.rows[0][3] == "ready"
    assert cypher_rebuilt_index.rows[0][3] == "ready"
    assert cypher_dropped_index.rows[0][3] == "disabled"
    assert cypher_dropped_index_if_present.rows[0][3] == "disabled"
    assert cypher_shown_indexes_after_drop.rows == ()

    print_section("3. Cypher Vectors: graph-owned embeddings")
    print(
        "Use this when vectors belong to graph nodes and Cypher does the "
        "pattern filtering."
    )
    print(
        "This demo lowers hot_max_rows and adds filler graph nodes so the "
        "cold ANN tier becomes visible."
    )
    print("Cypher vector index lifecycle:", cypher_created_index.rows)
    print(
        "Cypher vector index lifecycle (IF NOT EXISTS):",
        cypher_created_index_if_missing.rows,
    )
    print("Cypher vector index catalog:", cypher_shown_indexes.rows)
    print("Cypher vector index paused for ingest:", cypher_paused_index.rows)
    print(
        "Cypher vector index catalog while paused:",
        cypher_shown_indexes_paused.rows,
    )
    print("Cypher vector index resumed:", cypher_resumed_index.rows)
    print("Cypher vector index refresh:", cypher_refreshed_index.rows)
    print("Cypher vector index rebuild:", cypher_rebuilt_index.rows)
    print("Cypher vector index dropped:", cypher_dropped_index.rows)
    print(
        "Cypher vector index dropped (IF EXISTS):",
        cypher_dropped_index_if_present.rows,
    )
    print(
        "Cypher vector index catalog after drop:",
        cypher_shown_indexes_after_drop.rows,
    )
    print("Cypher alpha matches:", cypher_updated.rows)
    print("Cypher collaboration rows:", cypher_collaborations.rows)


def main() -> None:
    report = _make_timer()
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        print("This example shows three distinct vector use cases in HumemDB:")
        print("1. Direct API for unstructured vector memory")
        print("2. SQL for vectors attached to relational table rows")
        print("3. Cypher for vectors attached to graph nodes")
        print(
            "For demonstration, all three sections lower the hot-tier limit "
            "so the cold tier activates on small toy datasets."
        )

        run_direct_vector_example(root, report)
        run_sql_vector_example(root, report)
        run_cypher_vector_example(root, report)


if __name__ == "__main__":
    main()
