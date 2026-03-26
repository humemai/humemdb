from __future__ import annotations

import tempfile
from pathlib import Path
from time import perf_counter

from humemdb import HumemDB


USER_COUNT = 30_000
TEAM_COUNT = 64
TOPIC_COUNT = 256
DOCUMENT_COUNT = 20_000


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


def populate_nodes(db) -> None:
    for index in range(TEAM_COUNT):
        db.query(
            "CREATE (:Team {slug: $slug, region: $region, focus: $focus})",
            params={
                "slug": f"team-{index}",
                "region": ("emea", "amer", "apac", "public")[index % 4],
                "focus": ("infra", "graph", "ml", "ops")[index % 4],
            },
        )

    for index in range(TOPIC_COUNT):
        db.query(
            "CREATE (:Topic {slug: $slug, area: $area, priority: $priority})",
            params={
                "slug": f"topic-{index}",
                "area": ("storage", "routing", "vector", "security")[index % 4],
                "priority": 1 + (index % 3),
            },
        )

    for index in range(USER_COUNT):
        cohort = f"batch-{index % 6}"
        city = ("Berlin", "Paris", "Lisbon", "Seoul", "Tokyo", "Oslo")[
            index % 6
        ]
        nickname = None if index % 4 else f"nick-{index:03d}"
        db.query(
            (
                "CREATE (:User {"
                "name: $name, age: $age, active: $active, cohort: $cohort, "
                "city: $city, nickname: $nickname"
                "})"
            ),
            params={
                "name": f"User {index:03d}",
                "age": 24 + (index % 15),
                "active": index % 5 != 0,
                "cohort": cohort,
                "city": city,
                "nickname": nickname,
            },
        )

    for index in range(DOCUMENT_COUNT):
        db.query(
            (
                "CREATE (:Document {"
                "title: $title, status: $status, region: $region, score: $score"
                "})"
            ),
            params={
                "title": f"Document {index:03d}",
                "status": "published" if index % 4 != 0 else "draft",
                "region": ("emea", "amer", "apac")[index % 3],
                "score": 50 + ((index * 17) % 45),
            },
        )


def populate_edges(db) -> None:
    for index in range(USER_COUNT):
        user_name = f"User {index:03d}"
        teammate = f"User {(index + 1) % USER_COUNT:03d}"
        mentor = f"User {(index + 9) % USER_COUNT:03d}"
        team_slug = f"team-{index % TEAM_COUNT}"
        db.query(
            (
                "MATCH (a:User {name: $user_name}), (b:User {name: $peer_name}) "
                "CREATE (a)-[:KNOWS {"
                "since: $since, strength: $strength, note: $note"
                "}]->(b)"
            ),
            params={
                "user_name": user_name,
                "peer_name": teammate,
                "since": 2018 + (index % 6),
                "strength": 2 + (index % 5),
                "note": "met at meetup" if index % 3 == 0 else "works remotely",
            },
        )
        if index % 6 == 0:
            db.query(
                (
                    "MATCH (a:User {name: $user_name}), (b:User {name: $peer_name}) "
                    "CREATE (a)-[:MENTORS {since: $since, strength: $strength}]->(b)"
                ),
                params={
                    "user_name": user_name,
                    "peer_name": mentor,
                    "since": 2017 + (index % 4),
                    "strength": 5 + (index % 4),
                },
            )
        db.query(
            (
                "MATCH (u:User {name: $user_name}), (t:Team {slug: $team_slug}) "
                "CREATE (u)-[:MEMBER_OF {since: $since, role: $role}]->(t)"
            ),
            params={
                "user_name": user_name,
                "team_slug": team_slug,
                "since": 2020 + (index % 4),
                "role": ("engineer", "lead", "analyst")[index % 3],
            },
        )
        if index < DOCUMENT_COUNT:
            document_title = f"Document {index % DOCUMENT_COUNT:03d}"
            topic_slug = f"topic-{index % TOPIC_COUNT}"
            db.query(
                (
                    "MATCH (u:User {name: $user_name}), (d:Document {title: $title}) "
                    "CREATE (u)-[:AUTHORED {since: $since, channel: $channel}]->(d)"
                ),
                params={
                    "user_name": user_name,
                    "title": document_title,
                    "since": 2021 + (index % 3),
                    "channel": "review" if index % 2 else "analysis",
                },
            )
            db.query(
                (
                    "MATCH (d:Document {title: $title}), (t:Topic {slug: $slug}) "
                    "CREATE (d)-[:TAGGED {since: $since, weight: $weight}]->(t)"
                ),
                params={
                    "title": document_title,
                    "slug": topic_slug,
                    "since": 2022 + (index % 2),
                    "weight": 1 + (index % 4),
                },
            )


def mutate_graph(db) -> None:
    db.query(
        "MATCH (u:User {name: 'User 000'}) SET u.city = $city, u.nickname = $nickname",
        params={"city": "Berlin-hub", "nickname": "anchor-user"},
    )
    db.query(
        (
            "MATCH (a:User {name: 'User 000'})-[r:KNOWS]->(b:User {name: 'User 001'}) "
            "SET r.strength = $strength, r.note = $note"
        ),
        params={"strength": 9, "note": "paired on launch"},
    )
    db.query(
        (
            "MATCH (d:Document {title: 'Document 000'})"
            "-[r:TAGGED]->(t:Topic {slug: 'topic-0'}) DELETE r"
        )
    )
    db.query(
        (
            "CREATE (:User {"
            "name: 'Ephemeral User', age: 99, active: false, "
            "cohort: 'scratch', city: 'Nowhere'"
            "})"
        )
    )
    db.query("MATCH (u:User) WHERE u.name = 'Ephemeral User' DETACH DELETE u")


def main() -> None:
    report = _make_timer()
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)

        with HumemDB.open(root / "graph") as db:
            with db.transaction():
                populate_nodes(db)
                report("created graph nodes")
                populate_edges(db)
                report("created graph edges")
                mutate_graph(db)
                report("mutated graph state")

            relationship_result = db.query(
                (
                    "MATCH (u:User)-[r:KNOWS|MENTORS]->(peer:User) "
                    "WHERE u.name STARTS WITH 'User 0' AND peer.nickname IS NULL "
                    "RETURN u.name, r.type, r.since, peer.name "
                    "ORDER BY r.since DESC, u.name LIMIT 6"
                )
            )
            report("ran relationship read")
            reverse_result = db.query(
                (
                    "MATCH (:Team {slug: 'team-0'})"
                    "<-[r:MEMBER_OF]-(:User {active: true}) "
                    "RETURN r.type, r.since ORDER BY r.since DESC LIMIT 5"
                )
            )
            report("ran reverse relationship read")
            document_result = db.query(
                (
                    "MATCH (u:User)-[r:AUTHORED]->(d:Document) "
                    "WHERE d.status = 'published' AND d.title ENDS WITH '2' "
                    "RETURN u.name, d.title, r.channel ORDER BY d.title LIMIT 5"
                )
            )
            report("ran document read")
            distinct_result = db.query(
                (
                    "MATCH (u:User) WHERE u.city CONTAINS 'o' "
                    "RETURN DISTINCT u.cohort ORDER BY u.cohort OFFSET 1 LIMIT 3"
                )
            )
            report("ran DISTINCT/OFFSET read")
            updated_anchor = db.query(
                (
                    "MATCH (u:User) "
                    "WHERE u.nickname IS NOT NULL AND u.name = 'User 000' "
                    "RETURN u.name, u.city, u.nickname ORDER BY u.name LIMIT 1"
                )
            )
            removed_edge_result = db.query(
                (
                    "MATCH (d:Document {title: 'Document 000'})-[r:TAGGED]->(t:Topic) "
                    "RETURN t.slug ORDER BY t.slug"
                )
            )
            user_scan_result = db.query("MATCH (u:User) RETURN u.id ORDER BY u.id")
            document_scan_result = db.query(
                "MATCH (d:Document) RETURN d.title ORDER BY d.title"
            )
            knows_scan_result = db.query(
                (
                    "MATCH (u:User)-[r:KNOWS]->(peer:User) "
                    "RETURN r.type, r.since ORDER BY r.since"
                )
            )
            mentors_scan_result = db.query(
                (
                    "MATCH (u:User)-[r:MENTORS]->(peer:User) "
                    "RETURN r.type, r.since ORDER BY r.since"
                )
            )
            member_scan_result = db.query(
                (
                    "MATCH (u:User)-[r:MEMBER_OF]->(t:Team) "
                    "RETURN r.type, r.since ORDER BY r.since"
                )
            )
            authored_scan_result = db.query(
                (
                    "MATCH (u:User)-[r:AUTHORED]->(d:Document) "
                    "RETURN r.type, r.since ORDER BY r.since"
                )
            )
            tagged_scan_result = db.query(
                (
                    "MATCH (d:Document)-[r:TAGGED]->(t:Topic) "
                    "RETURN r.type, r.since ORDER BY r.since"
                )
            )
            report("ran graph count scans")

        assert relationship_result.columns == (
            "u.name",
            "r.type",
            "r.since",
            "peer.name",
        )
        assert len(relationship_result.rows) == 6
        assert {row[1] for row in relationship_result.rows}.issubset(
            {"KNOWS", "MENTORS"}
        )
        assert reverse_result.columns == ("r.type", "r.since")
        assert len(reverse_result.rows) == 5
        assert all(row[0] == "MEMBER_OF" for row in reverse_result.rows)
        assert len(document_result.rows) == 5
        assert all(row[1].endswith("2") for row in document_result.rows)
        assert len(distinct_result.rows) == 3
        assert distinct_result.rows == tuple(sorted(distinct_result.rows))
        assert all(row[0].startswith("batch-") for row in distinct_result.rows)
        assert updated_anchor.rows == (("User 000", "Berlin-hub", "anchor-user"),)
        assert removed_edge_result.rows == ()
        assert len(user_scan_result.rows) == USER_COUNT
        assert len(document_scan_result.rows) == DOCUMENT_COUNT
        edge_count_map = {
            "KNOWS": len(knows_scan_result.rows),
            "MENTORS": len(mentors_scan_result.rows),
            "MEMBER_OF": len(member_scan_result.rows),
            "AUTHORED": len(authored_scan_result.rows),
            "TAGGED": len(tagged_scan_result.rows),
        }
        assert edge_count_map["KNOWS"] == USER_COUNT
        assert edge_count_map["MEMBER_OF"] == USER_COUNT
        assert edge_count_map["AUTHORED"] == DOCUMENT_COUNT
        assert edge_count_map["TAGGED"] == DOCUMENT_COUNT - 1
        assert edge_count_map["MENTORS"] == (USER_COUNT // 6)

        print("Cypher relationship rows:", relationship_result.rows)
        print("Cypher reverse team membership rows:", reverse_result.rows)
        print("Cypher authored document rows:", document_result.rows)
        print("Cypher distinct cohort rows:", distinct_result.rows)
        print(
            "Cypher node counts:",
            {
                "users": len(user_scan_result.rows),
                "documents": len(document_scan_result.rows),
            },
        )
        print("Cypher edge counts:", edge_count_map)


if __name__ == "__main__":
    main()
