from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


def _load_module(relative_path: str, module_name: str):
    root = Path(__file__).resolve().parents[1]
    module_path = root / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class TestTranslationOverheadBenchmark(unittest.TestCase):
    def test_sql_translation_workloads_still_translate(self) -> None:
        benchmark_module = _load_module(
            "scripts/benchmarks/translation_overhead.py",
            "humemdb_translation_overhead_sql",
        )

        translate_sql = benchmark_module.translate_sql
        workloads = benchmark_module.SQL_WORKLOADS

        self.assertIn("case_and_exists", workloads)
        self.assertIn(
            "CASE WHEN EXISTS (",
            workloads["case_and_exists"].query,
        )
        self.assertIn("union_rollup", workloads)
        self.assertIn(
            "UNION ALL",
            workloads["union_rollup"].query,
        )
        self.assertIn("cte_multi_join", workloads)
        self.assertIn(
            "LEFT JOIN countries c ON c.id = u.country_id",
            workloads["cte_multi_join"].query,
        )
        self.assertIn("windowed_rank_cte", workloads)
        self.assertIn(
            "ROW_NUMBER() OVER (",
            workloads["windowed_rank_cte"].query,
        )

        for workload in workloads.values():
            sqlite_sql = translate_sql(workload.query, target="sqlite")
            duckdb_sql = translate_sql(workload.query, target="duckdb")

            self.assertTrue(sqlite_sql)
            self.assertTrue(duckdb_sql)

    def test_cypher_translation_workloads_still_parse_and_compile(self) -> None:
        benchmark_module = _load_module(
            "scripts/benchmarks/translation_overhead.py",
            "humemdb_translation_overhead",
        )

        parse_cypher = benchmark_module.parse_cypher
        plan_cypher_runtime = getattr(benchmark_module, "_plan_cypher_runtime")
        compile_cypher_bound = getattr(benchmark_module, "_compile_cypher_bound")
        workloads = benchmark_module.CYPHER_WORKLOADS

        self.assertIn("node_range_filter", workloads)
        self.assertIn(">= $min_age", workloads["node_range_filter"].query)
        self.assertIn("node_mixed_boolean", workloads)
        self.assertIn(
            "AND u.active = $active OR u.name = $name",
            workloads["node_mixed_boolean"].query,
        )
        self.assertIn("node_string_predicates", workloads)
        self.assertIn(
            "WHERE u.name STARTS WITH $prefix AND u.region CONTAINS $fragment",
            workloads["node_string_predicates"].query,
        )
        self.assertIn("node_null_predicates", workloads)
        self.assertIn(
            "WHERE u.nickname IS NULL AND u.region IS NOT NULL",
            workloads["node_null_predicates"].query,
        )
        self.assertIn("relationship_mixed_boolean", workloads)
        self.assertIn(
            "AND r.strength >= $strength OR b.name = $name",
            workloads["relationship_mixed_boolean"].query,
        )
        self.assertIn("node_parenthesized_boolean", workloads)
        self.assertIn(
            "(u.age >= $min_age OR u.name = $name) AND u.active = $active",
            workloads["node_parenthesized_boolean"].query,
        )
        self.assertIn("node_distinct_offset", workloads)
        self.assertIn(
            "RETURN DISTINCT u.region ORDER BY u.region OFFSET 5 LIMIT 10",
            workloads["node_distinct_offset"].query,
        )
        self.assertIn("relationship_untyped", workloads)
        self.assertIn(
            "MATCH (a:User)-[r]->(b:User)",
            workloads["relationship_untyped"].query,
        )
        self.assertIn("relationship_offset_window", workloads)
        self.assertIn(
            "ORDER BY r.since DESC OFFSET 25 LIMIT 50",
            workloads["relationship_offset_window"].query,
        )
        self.assertIn("relationship_string_predicates", workloads)
        self.assertIn(
            "WHERE r.note CONTAINS $fragment AND b.name ENDS WITH $suffix",
            workloads["relationship_string_predicates"].query,
        )
        self.assertIn("relationship_null_predicates", workloads)
        self.assertIn(
            "WHERE r.note IS NOT NULL",
            workloads["relationship_null_predicates"].query,
        )
        self.assertIn("relationship_type_alternation", workloads)
        self.assertIn(
            "[r:KNOWS|FOLLOWS]",
            workloads["relationship_type_alternation"].query,
        )
        self.assertIn("relationship_anonymous_endpoints", workloads)
        self.assertIn(
            "MATCH (:User {region: $region})-[r:KNOWS]->(:User {active: $active})",
            workloads["relationship_anonymous_endpoints"].query,
        )

        for workload in workloads.values():
            parsed_plan = parse_cypher(workload.query)
            runtime_plan = plan_cypher_runtime(workload.query)
            compiled = compile_cypher_bound(runtime_plan, workload.params)

            self.assertIsNotNone(parsed_plan)
            self.assertTrue(compiled.sql)
            self.assertIsInstance(compiled.params, tuple)
