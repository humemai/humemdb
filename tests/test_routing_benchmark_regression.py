from __future__ import annotations

import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


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


class TestRoutingBenchmarkRegression(unittest.TestCase):
    def test_ivf_pq_recall_policy_scales_by_rows_and_top_k(self) -> None:
        sweep_module = _load_module(
            "scripts/benchmarks/vector_search_real_sweep.py",
            "humemdb_vector_search_real_sweep_recall_policy",
        )

        sweep_target_for = sweep_module._recall_target_for

        self.assertEqual(
            sweep_target_for(rows=100_000, top_k=10)["target_recall"],
            0.95,
        )
        self.assertEqual(
            sweep_target_for(rows=1_000_000, top_k=10)["target_recall"],
            0.93,
        )
        self.assertEqual(
            sweep_target_for(rows=10_000_000, top_k=50)["target_recall"],
            0.95,
        )
        self.assertEqual(
            sweep_target_for(rows=25_000_000, top_k=10)["target_recall"],
            0.89,
        )
        self.assertEqual(
            sweep_target_for(rows=100_000_000, top_k=50)["target_recall"],
            0.93,
        )
        self.assertEqual(
            sweep_target_for(rows=25_000_000, top_k=50)["target_recall"],
            0.94,
        )
        self.assertEqual(
            sweep_target_for(rows=25_000_000, top_k=50)["scale_label"],
            "25M",
        )
        with self.assertRaisesRegex(ValueError, "Supported values: 10, 50"):
            sweep_target_for(rows=100_000, top_k=25)

    def test_real_vector_dataset_info_exposes_stackoverflow_filter_groups(self) -> None:
        benchmark_module = _load_module(
            "scripts/benchmarks/vector_search_real.py",
            "humemdb_vector_search_real",
        )
        dataset_info = benchmark_module._dataset_info

        stackoverflow = dataset_info("stackoverflow-xlarge")
        msmarco = dataset_info("msmarco-10m")

        self.assertEqual(stackoverflow.dimensions, 384)
        self.assertIn("questions", stackoverflow.group_names)
        self.assertIn("answers", stackoverflow.group_names)
        self.assertIn("comments", stackoverflow.group_names)
        self.assertEqual(msmarco.dimensions, 1024)
        self.assertEqual(msmarco.group_names, ("all",))

    def test_real_vector_proportional_allocations_stay_stable(self) -> None:
        benchmark_module = _load_module(
            "scripts/benchmarks/vector_search_real.py",
            "humemdb_vector_search_real_allocations",
        )
        proportional_allocations = benchmark_module._proportional_allocations

        allocations = proportional_allocations(
            {"questions": 5_000_000, "answers": 12_000_000, "comments": 8_000_000},
            total=1_000_000,
        )

        self.assertEqual(sum(allocations.values()), 1_000_000)
        self.assertGreater(allocations["answers"], allocations["comments"])
        self.assertGreater(allocations["comments"], allocations["questions"])

    def test_real_vector_sweep_auto_filters_expand_for_stackoverflow(self) -> None:
        sweep_module = _load_module(
            "scripts/benchmarks/vector_search_real_sweep.py",
            "humemdb_vector_search_real_sweep",
        )
        filter_sources_for_dataset = sweep_module._filter_sources_for_dataset

        self.assertEqual(
            filter_sources_for_dataset(
                dataset="msmarco-10m",
                raw="auto",
            ),
            [None],
        )
        self.assertEqual(
            filter_sources_for_dataset(
                dataset="stackoverflow-xlarge",
                raw="auto",
            ),
            [None, "questions", "answers", "comments"],
        )

    def test_real_vector_benchmark_requires_min_queries_and_repetitions(self) -> None:
        benchmark_module = _load_module(
            "scripts/benchmarks/vector_search_real.py",
            "humemdb_vector_search_real_sampling",
        )
        validate = benchmark_module._validate_benchmark_sampling

        with self.assertRaisesRegex(ValueError, "at least 100 queries"):
            validate(99, 3)
        with self.assertRaisesRegex(ValueError, "at least 3 repetitions"):
            validate(100, 2)
        validate(100, 3)

    def test_real_vector_benchmark_multi_top_k_reuses_build_output(self) -> None:
        benchmark_module = _load_module(
            "scripts/benchmarks/vector_search_real.py",
            "humemdb_vector_search_real_multi_topk",
        )
        finalize_top_k_reports = benchmark_module._finalize_top_k_reports

        report = finalize_top_k_reports(
            base_report={
                "dataset": "msmarco-10m",
                "rows": 100_000,
                "queries": 100,
            },
            top_k_reports=[
                {
                    "top_k": 10,
                    "latency_summaries_ms": {"lancedb_indexed_global": {"mean": 1.0}},
                    "recalls_at_k": {"lancedb_indexed_global": 0.95},
                },
                {
                    "top_k": 50,
                    "latency_summaries_ms": {"lancedb_indexed_global": {"mean": 2.0}},
                    "recalls_at_k": {"lancedb_indexed_global": 0.97},
                },
            ],
        )

        self.assertEqual(report["top_k_grid"], [10, 50])
        self.assertEqual(len(report["top_k_reports"]), 2)
        self.assertEqual(report["top_k_reports"][1]["top_k"], 50)

    def test_real_vector_sweep_requires_min_queries_and_repetitions(self) -> None:
        sweep_module = _load_module(
            "scripts/benchmarks/vector_search_real_sweep.py",
            "humemdb_vector_search_real_sweep_sampling",
        )

        sweep_validate = sweep_module._validate_benchmark_sampling

        with self.assertRaisesRegex(ValueError, "at least 100 queries"):
            sweep_validate(32, 3)
        with self.assertRaisesRegex(ValueError, "at least 3 repetitions"):
            sweep_validate(100, 1)
        sweep_validate(100, 3)

    def test_sql_workloads_still_emit_router_feature_shapes(self) -> None:
        benchmark_module = _load_module(
            "scripts/benchmarks/duckdb_direct_read.py",
            "humemdb_duckdb_direct_read",
        )
        workloads = benchmark_module.QUERY_WORKLOADS
        sql_feature_dict = benchmark_module._sql_feature_dict

        self.assertIn("event_exists_region_filter", workloads)
        self.assertIn(
            "WHERE EXISTS (",
            workloads["event_exists_region_filter"].query,
        )
        self.assertIn("document_distinct_owner_regions", workloads)
        self.assertIn(
            "SELECT DISTINCT users.region, documents.language",
            workloads["document_distinct_owner_regions"].query,
        )
        self.assertIn("memory_owner_exists_projection", workloads)
        self.assertIn(
            "WHERE EXISTS (",
            workloads["memory_owner_exists_projection"].query,
        )

        exists_features = sql_feature_dict(
            workloads["event_exists_region_filter"].query
        )
        distinct_features = sql_feature_dict(
            workloads["document_distinct_owner_regions"].query
        )
        memory_exists_features = sql_feature_dict(
            workloads["memory_owner_exists_projection"].query
        )

        self.assertEqual(exists_features["exists_count"], 1)
        self.assertFalse(bool(exists_features["has_distinct"]))
        self.assertTrue(bool(exists_features["is_read_only"]))
        self.assertEqual(distinct_features["join_count"], 1)
        self.assertEqual(distinct_features["exists_count"], 0)
        self.assertTrue(bool(distinct_features["has_distinct"]))
        self.assertEqual(memory_exists_features["exists_count"], 1)
        self.assertTrue(bool(memory_exists_features["has_order_by"]))
        self.assertTrue(bool(memory_exists_features["has_limit"]))

    def test_cypher_graph_workloads_still_parse_and_compile(self) -> None:
        benchmark_module = _load_module(
            "scripts/benchmarks/cypher_graph_path.py",
            "humemdb_cypher_graph_path",
        )
        dataset_counts = benchmark_module._dataset_counts
        workloads_for = benchmark_module._workloads
        compile_workload = benchmark_module._compile_workload
        cypher_feature_dict = benchmark_module._cypher_feature_dict
        apply_graph_index_set = benchmark_module._apply_graph_index_set
        sqlite_plan_summary_from_details = benchmark_module._sqlite_plan_summary_from_details

        dataset = dataset_counts(5_000, 3, 2)
        workloads = workloads_for(dataset)

        self.assertIn("social_mixed_boolean", workloads)
        self.assertIn(
            "AND b.active = $active OR a.name = $name",
            workloads["social_mixed_boolean"].query,
        )
        self.assertIn("social_expand_untyped", workloads)
        self.assertIn(
            "MATCH (a:User)-[r]->(b:User)",
            workloads["social_expand_untyped"].query,
        )
        self.assertIn("user_distinct_region_offset", workloads)
        self.assertIn(
            "RETURN DISTINCT u.region ORDER BY u.region OFFSET 5 LIMIT 10",
            workloads["user_distinct_region_offset"].query,
        )
        self.assertIn("social_expand_type_alternation", workloads)
        self.assertIn(
            "[r:KNOWS|FOLLOWS]",
            workloads["social_expand_type_alternation"].query,
        )
        self.assertIn("social_expand_offset", workloads)
        self.assertIn(
            "ORDER BY r.since DESC OFFSET 50 LIMIT 250",
            workloads["social_expand_offset"].query,
        )
        self.assertIn("social_expand_anonymous_endpoints", workloads)
        self.assertIn(
            "MATCH (:User {region: $region})-[r:KNOWS]->(:User {active: $active})",
            workloads["social_expand_anonymous_endpoints"].query,
        )
        self.assertIn("social_type_filtered_region_expand", workloads)
        self.assertIn(
            "WHERE r.type = $type AND a.region = $region AND b.active = $active",
            workloads["social_type_filtered_region_expand"].query,
        )
        self.assertIn("social_reverse_expand_ordered", workloads)
        self.assertIn(
            "MATCH (b:User)<-[r:KNOWS]-(a:User)",
            workloads["social_reverse_expand_ordered"].query,
        )
        self.assertIn(
            "ORDER BY r.since DESC LIMIT 500",
            workloads["social_reverse_expand_ordered"].query,
        )
        self.assertIn("team_membership_role_region", workloads)
        self.assertIn(
            "WHERE r.role = $role AND g.region = $region",
            workloads["team_membership_role_region"].query,
        )
        self.assertIn("team_membership_type_band", workloads)
        self.assertIn(
            "WHERE r.type = $type AND g.size_band = $size_band AND u.active = $active",
            workloads["team_membership_type_band"].query,
        )
        self.assertIn("author_expand_unordered", workloads)
        self.assertEqual(
            workloads["author_expand_ordered"].comparison_group,
            "author_expand_score_order",
        )
        self.assertEqual(
            workloads["author_expand_unordered"].order_variant,
            "unordered",
        )
        self.assertIn("team_membership_role_region_unordered", workloads)
        self.assertEqual(
            workloads["team_membership_role_region"].comparison_group,
            "team_membership_name_order",
        )
        self.assertIn("tagged_weight_domain_unordered", workloads)
        self.assertEqual(
            workloads["tagged_weight_domain"].order_variant,
            "ordered",
        )
        self.assertIn("tagged_weight_domain", workloads)
        self.assertIn(
            "WHERE r.weight = $weight AND t.domain = $domain",
            workloads["tagged_weight_domain"].query,
        )

        distinct_features = cypher_feature_dict(
            workloads["user_distinct_region_offset"]
        )
        edge_property_features = cypher_feature_dict(
            workloads["team_membership_role_region"]
        )
        type_filter_features = cypher_feature_dict(
            workloads["social_type_filtered_region_expand"]
        )

        self.assertTrue(bool(distinct_features["has_distinct"]))
        self.assertTrue(bool(distinct_features["has_order_by"]))
        self.assertTrue(bool(distinct_features["has_offset"]))
        self.assertTrue(bool(distinct_features["has_limit"]))
        self.assertGreaterEqual(
            int(edge_property_features["edge_property_join_count"]),
            1,
        )
        self.assertFalse(bool(edge_property_features["anchors_edge_properties"]))
        self.assertTrue(bool(type_filter_features["direct_edge_type_filter"]))

        plan_summary = sqlite_plan_summary_from_details(
            [
                (
                    "SEARCH graph_edge_properties AS r_filter USING INDEX "
                    "idx_graph_edge_props_lookup "
                    "(key=? AND value_type=? AND value=? AND edge_id=?)"
                ),
                (
                    "SEARCH graph_edges AS r USING COVERING INDEX "
                    "idx_graph_edges_to_type_from "
                    "(to_node_id=? AND type=? AND from_node_id=?)"
                ),
                "SEARCH graph_nodes AS g USING INTEGER PRIMARY KEY (rowid=?)",
                "USE TEMP B-TREE FOR ORDER BY",
            ]
        )
        self.assertEqual(plan_summary["edge_property_search_count"], 1)
        self.assertEqual(plan_summary["edge_search_count"], 1)
        self.assertEqual(plan_summary["node_search_count"], 1)
        self.assertTrue(bool(plan_summary["uses_temp_btree"]))
        self.assertEqual(len(plan_summary["index_mentions"]), 2)

        with self.assertRaisesRegex(ValueError, "Unknown graph index set"):
            apply_graph_index_set(None, index_set="not-a-real-index-set")

        unordered_compiled = compile_workload(
            workloads["team_membership_role_region_unordered"]
        )
        author_ordered_compiled = compile_workload(
            workloads["author_expand_ordered"]
        )
        social_mixed_boolean_compiled = compile_workload(
            workloads["social_mixed_boolean"]
        )
        team_membership_ordered_compiled = compile_workload(
            workloads["team_membership_role_region"]
        )
        self.assertNotIn("ORDER BY u.id, r.id, g.id", unordered_compiled.sql)
        self.assertTrue(unordered_compiled.sql.endswith("LIMIT 250"))
        self.assertNotIn(
            "JOIN graph_node_properties AS d_return_2",
            author_ordered_compiled.sql,
        )
        self.assertIn("FROM (SELECT u.id AS __left_id", author_ordered_compiled.sql)
        self.assertIn("d_order_0", author_ordered_compiled.sql)
        self.assertIn(
            "narrowed.__order_value_0 AS \"__value_2\"",
            author_ordered_compiled.sql,
        )
        self.assertIn(
            "ORDER BY narrowed.__order_0 DESC, narrowed.__order_1 DESC",
            author_ordered_compiled.sql,
        )
        self.assertNotIn(
            "JOIN graph_nodes AS u ON u.id = narrowed.__left_id",
            author_ordered_compiled.sql,
        )
        self.assertNotIn(
            "JOIN graph_edges AS edge_rel ON edge_rel.id = narrowed.__edge_id",
            author_ordered_compiled.sql,
        )
        self.assertNotIn(
            "JOIN graph_nodes AS d ON d.id = narrowed.__right_id",
            author_ordered_compiled.sql,
        )
        self.assertIn(
            "u_return_0.node_id = narrowed.__left_id",
            author_ordered_compiled.sql,
        )
        self.assertIn(
            "d_return_1.node_id = narrowed.__right_id",
            author_ordered_compiled.sql,
        )
        self.assertIn(
            (
                "FROM (SELECT \"a.id\" AS __left_id, \"r.id\" AS __edge_id, "
                "\"b.id\" AS __right_id"
            ),
            social_mixed_boolean_compiled.sql,
        )
        self.assertIn(" UNION ", social_mixed_boolean_compiled.sql)
        self.assertIn("AS matched", social_mixed_boolean_compiled.sql)
        self.assertNotIn(
            "FROM (SELECT u.id AS __left_id",
            team_membership_ordered_compiled.sql,
        )

        for workload in workloads.values():
            compiled = compile_workload(workload)

            self.assertTrue(compiled.sql)
            self.assertIsInstance(compiled.params, tuple)

    def test_sql_threshold_report_keeps_representative_crossovers_stable(self) -> None:
        report_module = _load_module(
            "scripts/benchmarks/routing_threshold_report.py",
            "humemdb_routing_threshold_report_sql",
        )
        sql_workload_report = report_module._sql_workload_report
        recommended_sql_olap_thresholds = report_module._recommended_sql_olap_thresholds

        runs = [
            {
                "scale_value": 10_000,
                "workloads": {
                    "event_point_lookup": {
                        "family": "relational",
                        "shape": "point_lookup",
                        "selectivity": "high",
                        "sql_features": {
                            "aggregate_count": 0,
                            "exists_count": 0,
                            "join_count": 0,
                        },
                        "sqlite": {"mean_ms": 0.05},
                        "duckdb": {"mean_ms": 15.0},
                    },
                    "event_cte_daily_rollup": {
                        "family": "relational",
                        "shape": "cte_rollup",
                        "selectivity": "broad",
                        "sql_features": {
                            "aggregate_count": 1,
                            "cte_count": 1,
                            "exists_count": 0,
                            "has_group_by": True,
                            "has_limit": True,
                            "has_order_by": True,
                            "has_distinct": False,
                            "join_count": 0,
                        },
                        "sqlite": {"mean_ms": 18.0},
                        "duckdb": {"mean_ms": 24.0},
                    },
                },
            },
            {
                "scale_value": 100_000,
                "workloads": {
                    "event_point_lookup": {
                        "family": "relational",
                        "shape": "point_lookup",
                        "selectivity": "high",
                        "sql_features": {
                            "aggregate_count": 0,
                            "exists_count": 0,
                            "join_count": 0,
                        },
                        "sqlite": {"mean_ms": 0.08},
                        "duckdb": {"mean_ms": 14.0},
                    },
                    "event_cte_daily_rollup": {
                        "family": "relational",
                        "shape": "cte_rollup",
                        "selectivity": "broad",
                        "sql_features": {
                            "aggregate_count": 1,
                            "cte_count": 1,
                            "exists_count": 0,
                            "has_group_by": True,
                            "has_limit": True,
                            "has_order_by": True,
                            "has_distinct": False,
                            "join_count": 0,
                        },
                        "sqlite": {"mean_ms": 190.0},
                        "duckdb": {"mean_ms": 41.0},
                    },
                },
            },
        ]

        report = sql_workload_report(runs)
        recommendation = recommended_sql_olap_thresholds(runs)

        by_workload = {entry["workload"]: entry for entry in report}
        self.assertIsNone(by_workload["event_point_lookup"]["first_duckdb_scale"])
        self.assertEqual(
            by_workload["event_cte_daily_rollup"]["first_duckdb_scale"],
            100_000,
        )
        self.assertEqual(
            by_workload["event_cte_daily_rollup"]["sql_features"]["cte_count"],
            1,
        )
        self.assertTrue(recommendation["benchmark_calibrated"])
        self.assertEqual(
            recommendation["evidence"]["duckdb_winning_workloads"],
            ["event_cte_daily_rollup"],
        )
        self.assertEqual(
            recommendation["evidence"]["sqlite_preferring_workloads"],
            ["event_point_lookup"],
        )
        self.assertEqual(
            recommendation["rules"],
            [
                {
                    "min_aggregate_count": 1,
                    "min_cte_count": 1,
                    "min_exists_count": 0,
                    "min_join_count": 0,
                    "min_window_count": 0,
                    "require_distinct": False,
                    "require_group_by": True,
                    "require_order_by_or_limit": True,
                }
            ],
        )

    def test_sql_threshold_report_extracts_exists_and_distinct_rules(self) -> None:
        report_module = _load_module(
            "scripts/benchmarks/routing_threshold_report.py",
            "humemdb_routing_threshold_report_sql_rules",
        )
        recommended_sql_olap_thresholds = report_module._recommended_sql_olap_thresholds

        runs = [
            {
                "scale_value": 10_000,
                "workloads": {
                    "event_point_lookup": {
                        "family": "relational",
                        "shape": "point_lookup",
                        "selectivity": "high",
                        "sql_features": {
                            "aggregate_count": 0,
                            "cte_count": 0,
                            "exists_count": 0,
                            "has_distinct": False,
                            "has_group_by": False,
                            "has_limit": False,
                            "has_order_by": False,
                            "join_count": 0,
                            "window_count": 0,
                        },
                        "sqlite": {"mean_ms": 0.05},
                        "duckdb": {"mean_ms": 14.0},
                    },
                    "event_exists_region_filter": {
                        "family": "mixed",
                        "shape": "exists_filter",
                        "selectivity": "medium",
                        "sql_features": {
                            "aggregate_count": 0,
                            "cte_count": 0,
                            "exists_count": 1,
                            "has_distinct": False,
                            "has_group_by": False,
                            "has_limit": False,
                            "has_order_by": False,
                            "join_count": 0,
                            "window_count": 0,
                        },
                        "sqlite": {"mean_ms": 9.0},
                        "duckdb": {"mean_ms": 11.0},
                    },
                    "document_distinct_owner_regions": {
                        "family": "document",
                        "shape": "distinct_join_projection",
                        "selectivity": "medium",
                        "sql_features": {
                            "aggregate_count": 0,
                            "cte_count": 0,
                            "exists_count": 0,
                            "has_distinct": True,
                            "has_group_by": False,
                            "has_limit": False,
                            "has_order_by": False,
                            "join_count": 1,
                            "window_count": 0,
                        },
                        "sqlite": {"mean_ms": 8.0},
                        "duckdb": {"mean_ms": 10.0},
                    },
                },
            },
            {
                "scale_value": 1_000_000,
                "workloads": {
                    "event_point_lookup": {
                        "family": "relational",
                        "shape": "point_lookup",
                        "selectivity": "high",
                        "sql_features": {
                            "aggregate_count": 0,
                            "cte_count": 0,
                            "exists_count": 0,
                            "has_distinct": False,
                            "has_group_by": False,
                            "has_limit": False,
                            "has_order_by": False,
                            "join_count": 0,
                            "window_count": 0,
                        },
                        "sqlite": {"mean_ms": 0.07},
                        "duckdb": {"mean_ms": 16.0},
                    },
                    "event_exists_region_filter": {
                        "family": "mixed",
                        "shape": "exists_filter",
                        "selectivity": "medium",
                        "sql_features": {
                            "aggregate_count": 0,
                            "cte_count": 0,
                            "exists_count": 1,
                            "has_distinct": False,
                            "has_group_by": False,
                            "has_limit": False,
                            "has_order_by": False,
                            "join_count": 0,
                            "window_count": 0,
                        },
                        "sqlite": {"mean_ms": 130.0},
                        "duckdb": {"mean_ms": 31.0},
                    },
                    "document_distinct_owner_regions": {
                        "family": "document",
                        "shape": "distinct_join_projection",
                        "selectivity": "medium",
                        "sql_features": {
                            "aggregate_count": 0,
                            "cte_count": 0,
                            "exists_count": 0,
                            "has_distinct": True,
                            "has_group_by": False,
                            "has_limit": False,
                            "has_order_by": False,
                            "join_count": 1,
                            "window_count": 0,
                        },
                        "sqlite": {"mean_ms": 145.0},
                        "duckdb": {"mean_ms": 28.0},
                    },
                },
            },
        ]

        recommendation = recommended_sql_olap_thresholds(runs)

        self.assertTrue(recommendation["benchmark_calibrated"])
        self.assertEqual(
            recommendation["evidence"]["duckdb_winning_workloads"],
            ["event_exists_region_filter", "document_distinct_owner_regions"],
        )
        self.assertEqual(
            recommendation["evidence"]["sqlite_preferring_workloads"],
            ["event_point_lookup"],
        )
        self.assertIn(
            {
                "min_join_count": 0,
                "min_aggregate_count": 0,
                "min_cte_count": 0,
                "min_window_count": 0,
                "min_exists_count": 1,
                "require_group_by": False,
                "require_distinct": False,
                "require_order_by_or_limit": False,
            },
            recommendation["rules"],
        )
        self.assertIn(
            {
                "min_join_count": 1,
                "min_aggregate_count": 0,
                "min_cte_count": 0,
                "min_window_count": 0,
                "min_exists_count": 0,
                "require_group_by": False,
                "require_distinct": True,
                "require_order_by_or_limit": False,
            },
            recommendation["rules"],
        )

    def test_cypher_threshold_report_keeps_representative_crossovers_stable(
        self,
    ) -> None:
        report_module = _load_module(
            "scripts/benchmarks/routing_threshold_report.py",
            "humemdb_routing_threshold_report_cypher",
        )
        cypher_workload_report = report_module._cypher_workload_report
        cypher_graph_index_diagnostics = report_module._cypher_graph_index_diagnostics

        report = cypher_workload_report(
            [
                {
                    "scale_value": 100_000,
                    "workloads": {
                        "anchored_user_lookup": {
                            "family": "graph",
                            "shape": "node_lookup",
                            "selectivity": "high",
                            "cypher_features": {
                                "edge_property_join_count": 0,
                            },
                            "sqlite_plan_summary": {
                                "edge_search_count": 0,
                            },
                            "sqlite_raw_sql": {"mean_ms": 0.3},
                            "duckdb_raw_sql": {"mean_ms": 10.0},
                        },
                        "broad_social_fanout": {
                            "family": "graph",
                            "shape": "fanout",
                            "selectivity": "broad",
                            "cypher_features": {
                                "edge_property_join_count": 0,
                            },
                            "sqlite_plan_summary": {
                                "edge_search_count": 1,
                            },
                            "sqlite_raw_sql": {"mean_ms": 21.0},
                            "duckdb_raw_sql": {"mean_ms": 27.0},
                        },
                    },
                },
                {
                    "scale_value": 1_000_000,
                    "workloads": {
                        "anchored_user_lookup": {
                            "family": "graph",
                            "shape": "node_lookup",
                            "selectivity": "high",
                            "cypher_features": {
                                "edge_property_join_count": 0,
                            },
                            "sqlite_plan_summary": {
                                "edge_search_count": 0,
                            },
                            "sqlite_raw_sql": {"mean_ms": 0.5},
                            "duckdb_raw_sql": {"mean_ms": 12.0},
                        },
                        "broad_social_fanout": {
                            "family": "graph",
                            "shape": "fanout",
                            "selectivity": "broad",
                            "cypher_features": {
                                "edge_property_join_count": 0,
                            },
                            "sqlite_plan_summary": {
                                "edge_search_count": 1,
                            },
                            "sqlite_raw_sql": {"mean_ms": 215.0},
                            "duckdb_raw_sql": {"mean_ms": 60.0},
                        },
                    },
                },
            ]
        )

        by_workload = {entry["workload"]: entry for entry in report}
        self.assertIsNone(by_workload["anchored_user_lookup"]["first_duckdb_scale"])
        self.assertEqual(
            by_workload["broad_social_fanout"]["first_duckdb_scale"],
            1_000_000,
        )
        self.assertEqual(
            by_workload["broad_social_fanout"]["cypher_features"][
                "edge_property_join_count"
            ],
            0,
        )
        self.assertEqual(
            by_workload["broad_social_fanout"]["sqlite_plan_summary"][
                "edge_search_count"
            ],
            1,
        )

        diagnostics = cypher_graph_index_diagnostics(report)
        self.assertEqual(
            diagnostics["property_join_heavy_workloads"],
            [],
        )
        self.assertEqual(
            diagnostics["temp_btree_workloads"],
            [],
        )
        self.assertEqual(diagnostics["sort_cost_overhead"], [])

    def test_cypher_graph_index_diagnostics_highlight_property_join_pressure(
        self,
    ) -> None:
        report_module = _load_module(
            "scripts/benchmarks/routing_threshold_report.py",
            "humemdb_routing_threshold_report_cypher_graph_index",
        )
        cypher_graph_index_diagnostics = report_module._cypher_graph_index_diagnostics

        diagnostics = cypher_graph_index_diagnostics(
            [
                {
                    "workload": "team_membership_role_region",
                    "family": "graph",
                    "shape": "edge_property_plus_endpoint_filter",
                    "selectivity": "medium",
                    "comparison_group": "team_membership_name_order",
                    "order_variant": "ordered",
                    "cypher_features": {
                        "node_property_join_count": 1,
                        "edge_property_join_count": 2,
                        "anchors_node_properties": False,
                        "anchors_edge_properties": False,
                        "direct_edge_type_filter": False,
                    },
                    "sqlite_plan_summary": {
                        "uses_temp_btree": True,
                    },
                    "winners": [
                        {
                            "scale": 100000,
                            "sqlite_mean_ms": 1.8,
                            "duckdb_mean_ms": 9.5,
                        }
                    ],
                },
                {
                    "workload": "team_membership_role_region_unordered",
                    "family": "graph",
                    "shape": "edge_property_plus_endpoint_filter",
                    "selectivity": "medium",
                    "comparison_group": "team_membership_name_order",
                    "order_variant": "unordered",
                    "cypher_features": {
                        "node_property_join_count": 1,
                        "edge_property_join_count": 2,
                        "anchors_node_properties": False,
                        "anchors_edge_properties": False,
                        "direct_edge_type_filter": False,
                    },
                    "sqlite_plan_summary": {
                        "uses_temp_btree": False,
                    },
                    "winners": [
                        {
                            "scale": 100000,
                            "sqlite_mean_ms": 1.1,
                            "duckdb_mean_ms": 9.0,
                        }
                    ],
                },
                {
                    "workload": "social_type_filtered_region_expand",
                    "family": "graph",
                    "shape": "endpoint_plus_type_filter",
                    "selectivity": "medium",
                    "comparison_group": None,
                    "order_variant": None,
                    "cypher_features": {
                        "node_property_join_count": 1,
                        "edge_property_join_count": 0,
                        "anchors_node_properties": True,
                        "anchors_edge_properties": False,
                        "direct_edge_type_filter": True,
                    },
                    "sqlite_plan_summary": {
                        "uses_temp_btree": False,
                    },
                },
            ]
        )

        self.assertEqual(
            diagnostics["property_join_heavy_workloads"],
            [
                "team_membership_role_region",
                "team_membership_role_region_unordered",
            ],
        )
        self.assertEqual(
            diagnostics["temp_btree_workloads"],
            ["team_membership_role_region"],
        )
        self.assertEqual(
            diagnostics["direct_type_filter_workloads"],
            ["social_type_filtered_region_expand"],
        )
        self.assertEqual(
            diagnostics["node_property_anchor_workloads"],
            ["social_type_filtered_region_expand"],
        )
        self.assertEqual(
            diagnostics["edge_property_anchor_workloads"],
            [],
        )
        self.assertEqual(
            diagnostics["candidate_index_workloads"],
            ["team_membership_role_region"],
        )
        self.assertEqual(
            diagnostics["sort_cost_overhead"],
            [
                {
                    "comparison_group": "team_membership_name_order",
                    "ordered_workload": "team_membership_role_region",
                    "unordered_workload": "team_membership_role_region_unordered",
                    "avg_sqlite_order_overhead_ms": 0.7,
                }
            ],
        )

    def test_vector_threshold_report_keeps_filtered_crossovers_stable(self) -> None:
        report_module = _load_module(
            "scripts/benchmarks/routing_threshold_report.py",
            "humemdb_routing_threshold_report_vector",
        )
        vector_workload_report = report_module._vector_workload_report

        report = vector_workload_report(
            [
                {
                    "rows": 2_000,
                    "dimensions": 256,
                    "top_k": 10,
                    "filtered_candidate_count": 200,
                    "lancedb_strategy": "default",
                    "latency_mean_ms": {
                        "numpy_f32_filtered": 0.25,
                        "lancedb_indexed_filtered": 0.40,
                    },
                    "recalls_at_k": {
                        "lancedb_indexed_filtered": 0.99,
                    },
                },
                {
                    "rows": 50_000,
                    "dimensions": 256,
                    "top_k": 10,
                    "filtered_candidate_count": 2_000,
                    "lancedb_strategy": "tuned",
                    "latency_mean_ms": {
                        "numpy_f32_filtered": 1.80,
                        "lancedb_indexed_filtered": 0.70,
                    },
                    "recalls_at_k": {
                        "lancedb_indexed_filtered": 0.97,
                    },
                },
                {
                    "rows": 50_000,
                    "dimensions": 768,
                    "top_k": 10,
                    "filtered_candidate_count": 2_000,
                    "lancedb_strategy": "tuned",
                    "latency_mean_ms": {
                        "numpy_f32_filtered": 2.20,
                        "lancedb_indexed_filtered": 0.90,
                    },
                    "recalls_at_k": {
                        "lancedb_indexed_filtered": 0.90,
                    },
                },
            ],
            min_indexed_recall=0.95,
        )

        by_workload = {entry["workload"]: entry for entry in report}
        self.assertEqual(
            by_workload["vector_dims256_topk10"]["first_indexed_scale"],
            50_000,
        )
        self.assertIsNone(
            by_workload["vector_dims768_topk10"]["first_indexed_scale"]
        )

    def test_vector_threshold_print_section_uses_indexed_crossover_key(self) -> None:
        report_module = _load_module(
            "scripts/benchmarks/routing_threshold_report.py",
            "humemdb_routing_threshold_report_vector_print",
        )
        print_section = report_module._print_section

        output = io.StringIO()
        with redirect_stdout(output):
            print_section(
                "Vector routing crossover summary",
                [
                    {
                        "workload": "vector_dims256_topk10",
                        "family": "vector",
                        "shape": "candidate_filtered_ann",
                        "dimensions": 256,
                        "top_k": 10,
                        "first_indexed_scale": 50_000,
                        "winners": [],
                    },
                    {
                        "workload": "vector_dims768_topk10",
                        "family": "vector",
                        "shape": "candidate_filtered_ann",
                        "dimensions": 768,
                        "top_k": 10,
                        "first_indexed_scale": None,
                        "winners": [],
                    },
                ],
            )

        rendered = output.getvalue()
        self.assertIn("Indexed first wins at scale 50000", rendered)
        self.assertIn("no indexed crossover in current sweep", rendered)

    def test_real_vector_threshold_report_groups_by_dataset_and_filter(self) -> None:
        report_module = _load_module(
            "scripts/benchmarks/routing_threshold_report.py",
            "humemdb_routing_threshold_report_real_vector",
        )
        vector_workload_report = report_module._vector_workload_report

        report = vector_workload_report(
            [
                {
                    "dataset": "stackoverflow-xlarge",
                    "rows": 10_000,
                    "top_k": 10,
                    "filter_source": None,
                    "sample_mode": "stratified",
                    "latency_mean_ms": {
                        "numpy_f32_global": 3.0,
                        "lancedb_indexed_global": 4.0,
                    },
                    "recalls_at_k": {
                        "lancedb_indexed_global": 0.98,
                    },
                },
                {
                    "dataset": "stackoverflow-xlarge",
                    "rows": 100_000,
                    "top_k": 10,
                    "filter_source": None,
                    "sample_mode": "stratified",
                    "latency_mean_ms": {
                        "numpy_f32_global": 12.0,
                        "lancedb_indexed_global": 6.0,
                    },
                    "recalls_at_k": {
                        "lancedb_indexed_global": 0.97,
                    },
                },
                {
                    "dataset": "stackoverflow-xlarge",
                    "rows": 1_000_000,
                    "top_k": 10,
                    "filter_source": None,
                    "sample_mode": "stratified",
                    "latency_mean_ms": {
                        "lancedb_indexed_global": 5.0,
                    },
                    "recalls_at_k": {
                        "lancedb_indexed_global": None,
                    },
                },
                {
                    "dataset": "stackoverflow-xlarge",
                    "rows": 10_000,
                    "top_k": 10,
                    "filter_source": "questions",
                    "sample_mode": "stratified",
                    "filtered_candidate_count": 4_000,
                    "latency_mean_ms": {
                        "numpy_f32_filtered": 2.0,
                        "lancedb_indexed_filtered": 1.5,
                    },
                    "recalls_at_k": {
                        "lancedb_indexed_filtered": 0.96,
                    },
                },
            ],
            min_indexed_recall=0.95,
        )

        by_workload = {entry["workload"]: entry for entry in report}
        self.assertEqual(
            by_workload["vector_stackoverflow-xlarge_global_topk10"][
                "first_indexed_scale"
            ],
            100_000,
        )
        self.assertEqual(
            by_workload["vector_stackoverflow-xlarge_global_topk10"]["winners"][-1][
                "winner"
            ],
            "unmeasured_exact",
        )
        self.assertEqual(
            by_workload["vector_stackoverflow-xlarge_questions_topk10"][
                "first_indexed_scale"
            ],
            10_000,
        )

    def test_real_vector_report_skips_numpy_exact_above_default_cutoff(self) -> None:
        benchmark_module = _load_module(
            "scripts/benchmarks/vector_search_real.py",
            "humemdb_vector_search_real_cutoff",
        )

        config = benchmark_module.BenchmarkConfig(
            dataset="msmarco-10m",
            rows=1_000_001,
            queries=100,
            top_k=10,
            warmup=0,
            repetitions=3,
            metric="cosine",
            seed=0,
            filter_source=None,
            sample_mode="auto",
            batch_size=1_024,
            lancedb_index_type="IVF_PQ",
            numpy_exact_max_rows=100_000,
        )

        dataset_info = benchmark_module.DatasetInfo(
            name="msmarco-10m",
            meta_path=Path("/tmp/fake.meta.json"),
            count=2_000_000,
            dimensions=2,
            group_names=("all",),
        )
        selected_ranges = [
            benchmark_module.SelectedRange(
                row_start=0,
                shard_path=Path("/tmp/fake.shard.f32"),
                shard_count=1_000_001,
                offset=0,
                count=1_000_001,
                group_id=0,
                group_name="all",
            )
        ]
        query_indexes = benchmark_module.np.arange(
            100,
            dtype=benchmark_module.np.int64,
        )
        group_ids = benchmark_module.np.zeros(1_000_001, dtype=benchmark_module.np.int8)
        group_lookup = {0: "all"}
        queries = [
            benchmark_module.np.zeros(2, dtype=benchmark_module.np.float32)
            for _ in range(100)
        ]
        fake_table = mock.Mock()
        fake_db = mock.Mock()
        fake_db.__enter__ = mock.Mock(return_value=fake_db)
        fake_db.__exit__ = mock.Mock(return_value=None)
        fake_db._duckdb = mock.Mock()
        packaged_ground_truth = benchmark_module.PackagedGroundTruth(
            gt_path=Path("/tmp/fake.gt.jsonl"),
            query_ids=query_indexes.copy(),
            neighbors_by_query_id={
                int(query_index): tuple(range(10)) for query_index in query_indexes
            },
        )

        with (
            mock.patch.object(
                benchmark_module,
                "_dataset_info",
                return_value=dataset_info,
            ),
            mock.patch.object(
                benchmark_module,
                "_plan_dataset_subset",
                return_value=(selected_ranges, group_ids, group_lookup),
            ),
            mock.patch.object(
                benchmark_module,
                "_query_indexes",
                return_value=query_indexes,
            ),
            mock.patch.object(
                benchmark_module,
                "_load_query_vectors",
                return_value=queries,
            ),
            mock.patch.object(
                benchmark_module,
                "_load_packaged_ground_truth",
                return_value=packaged_ground_truth,
            ),
            mock.patch.object(benchmark_module, "HumemDB") as humemdb_cls,
            mock.patch.object(
                benchmark_module,
                "_seed_lancedb_table_from_selected_ranges",
                return_value=fake_table,
            ),
            mock.patch.object(
                benchmark_module,
                "_build_lancedb_index",
                return_value=10.0,
            ),
            mock.patch.object(
                benchmark_module,
                "_build_lancedb_scalar_index",
                return_value=0.0,
            ),
            mock.patch.object(
                benchmark_module,
                "_search_lancedb_indexed",
                side_effect=lambda *_args, **_kwargs: tuple(range(10)),
            ),
            mock.patch.object(benchmark_module, "_time_callable") as time_callable,
            mock.patch.object(benchmark_module, "_ExactVectorIndex") as exact_index_cls,
        ):
            time_callable.return_value = benchmark_module.TimingSummary(
                mean=1.0,
                stdev=0.0,
                minimum=1.0,
                maximum=1.0,
            )
            report = benchmark_module.run_benchmark(config)

        exact_index_cls.assert_not_called()
        humemdb_cls.assert_not_called()
        self.assertFalse(report["numpy_exact_enabled"])
        self.assertEqual(report["numpy_exact_max_rows"], 100_000)
        self.assertEqual(report["artifact_sizes_bytes"]["numpy_f32_matrix"], 0)
        self.assertEqual(
            report["ground_truth_source"],
            "packaged_gt_subset_filtered",
        )
        self.assertNotIn("numpy_f32_global", report["latency_summaries_ms"])
        self.assertEqual(report["recalls_at_k"]["lancedb_indexed_global"], 1.0)
        self.assertIsNone(report["stage_timings_ms"]["numpy_f32_build"])
        self.assertEqual(
            report["cold_tier_ingest_path"],
            "Selected shard memmaps -> Arrow batches -> LanceDB -> build index",
        )

    def test_real_vector_threshold_recommendations_emit_dataset_guidance(self) -> None:
        report_module = _load_module(
            "scripts/benchmarks/routing_threshold_report.py",
            "humemdb_routing_threshold_report_real_vector_recommendations",
        )
        recommend = report_module._recommended_real_vector_thresholds

        recommendation = recommend(
            [
                {
                    "dataset": "stackoverflow-xlarge",
                    "filter_source": None,
                    "top_k": 10,
                    "shape": "global_ann",
                    "first_indexed_scale": 100_000,
                    "winners": [
                        {"scale": 10_000, "winner": "numpy_exact"},
                        {"scale": 100_000, "winner": "lancedb_indexed"},
                    ],
                },
                {
                    "dataset": "stackoverflow-xlarge",
                    "filter_source": "questions",
                    "top_k": 10,
                    "shape": "candidate_filtered_ann",
                    "first_indexed_scale": 10_000,
                    "winners": [
                        {
                            "scale": 10_000,
                            "winner": "lancedb_indexed",
                            "filtered_candidate_count": 4_000,
                        }
                    ],
                },
            ]
        )

        self.assertTrue(recommendation["dataset_aware"])
        by_filter = {
            entry["filter_source"]: entry
            for entry in recommendation["recommendations"]
        }
        self.assertEqual(by_filter[None]["default_route"], "numpy_exact")
        self.assertEqual(by_filter[None]["switch_to_indexed_at_rows"], 100_000)
        self.assertEqual(by_filter["questions"]["default_route"], "lancedb_indexed")
        self.assertEqual(
            by_filter["questions"]["min_filtered_candidate_count_for_indexed"],
            4_000,
        )

    def test_routing_sweep_writes_representative_scale_summaries(self) -> None:
        sweep_module = _load_module(
            "scripts/benchmarks/routing_sweep.py",
            "humemdb_routing_sweep",
        )
        run_sql_sweep = sweep_module._run_sql_sweep
        run_cypher_sweep = sweep_module._run_cypher_sweep

        def fake_run(
            command: list[str],
            *,
            check: bool,
            env: dict[str, str],
        ) -> None:
            self.assertTrue(check)
            self.assertIn("HUMEMDB_THREADS", env)
            output_path = Path(command[command.index("--output-json") + 1])
            if command[1].endswith("duckdb_direct_read.py"):
                rows = int(command[command.index("--rows") + 1])
                payload = {
                    "workloads": {
                        "event_point_lookup": {
                            "family": "relational",
                            "shape": "point_lookup",
                            "selectivity": "high",
                            "sql_features": {
                                "aggregate_count": 0,
                                "join_count": 0,
                            },
                            "sqlite": {"mean_ms": 0.05},
                            "duckdb": {"mean_ms": float(rows) / 1_000.0},
                        }
                    }
                }
            else:
                nodes = int(command[command.index("--nodes") + 1])
                index_set = command[command.index("--index-set") + 1]
                payload = {
                    "index_set": index_set,
                    "workloads": {
                        "anchored_user_lookup": {
                            "family": "graph",
                            "shape": "node_lookup",
                            "selectivity": "high",
                            "sqlite_raw_sql": {"mean_ms": 0.2},
                            "duckdb_raw_sql": {"mean_ms": float(nodes) / 10_000.0},
                        }
                    }
                }
            output_path.write_text(json.dumps(payload), encoding="utf-8")

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            env = os.environ.copy()
            env["HUMEMDB_THREADS"] = "4"

            with mock.patch.object(
                sweep_module.subprocess,
                "run",
                side_effect=fake_run,
            ):
                sql_summary = run_sql_sweep(
                    scales=(10_000, 100_000),
                    warmup=0,
                    repetitions=1,
                    output_dir=output_dir,
                    env=env,
                )
                cypher_summary = run_cypher_sweep(
                    scales=(100_000,),
                    index_set="targeted-covering",
                    warmup=0,
                    repetitions=1,
                    output_dir=output_dir,
                    env=env,
                )

            self.assertEqual(
                [run["scale_value"] for run in sql_summary["runs"]],
                [10_000, 100_000],
            )
            self.assertEqual(
                [run["scale_key"] for run in sql_summary["runs"]],
                ["rows", "rows"],
            )
            self.assertEqual(
                [run["scale_value"] for run in cypher_summary["runs"]],
                [100_000],
            )
            self.assertEqual(cypher_summary["index_set"], "targeted-covering")
            self.assertEqual(
                [run["scale_key"] for run in cypher_summary["runs"]],
                ["nodes"],
            )
            self.assertTrue((output_dir / "sql_summary.json").exists())
            self.assertTrue((output_dir / "cypher_summary.json").exists())

    def test_routing_sweep_writes_real_vector_scale_summaries(self) -> None:
        sweep_module = _load_module(
            "scripts/benchmarks/routing_sweep.py",
            "humemdb_routing_sweep_vector_real",
        )
        run_vector_sweep = sweep_module._run_vector_sweep

        def fake_run(
            command: list[str],
            *,
            check: bool,
            env: dict[str, str],
            stdout=None,
            text: bool | None = None,
        ):
            self.assertTrue(check)
            self.assertIn("HUMEMDB_THREADS", env)
            self.assertIn("vector_search_real_sweep.py", command[1])
            self.assertEqual(
                command[command.index("--dataset") + 1],
                "stackoverflow-xlarge",
            )
            self.assertEqual(command[command.index("--filter-sources") + 1], "auto")
            self.assertIn("--output-json", command)
            self.assertIn("--intermediate-dir", command)
            self.assertIsNotNone(stdout)
            self.assertTrue(text)
            payload = {
                "dataset": "stackoverflow-xlarge",
                "grid": {
                    "rows": [10_000],
                    "top_k": [10],
                    "filter_sources": ["none", "questions"],
                },
                "scenario_summaries": [
                    {
                        "dataset": "stackoverflow-xlarge",
                        "rows": 10_000,
                        "top_k": 10,
                        "filter_source": None,
                        "latency_mean_ms": {
                            "numpy_f32_global": 3.0,
                            "lancedb_indexed_global": 2.0,
                        },
                        "recalls_at_k": {"lancedb_indexed_global": 0.97},
                    }
                ],
                "overall": {"scenario_count": 1},
            }
            return mock.Mock(stdout=json.dumps(payload))

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            env = os.environ.copy()
            env["HUMEMDB_THREADS"] = "4"

            with mock.patch.object(
                sweep_module.subprocess,
                "run",
                side_effect=fake_run,
            ):
                vector_summary = run_vector_sweep(
                    scales=(10_000,),
                    top_k_grid=(10,),
                    queries=100,
                    warmup=0,
                    repetitions=3,
                    dataset="stackoverflow-xlarge",
                    filter_sources="auto",
                    sample_mode="stratified",
                    output_dir=output_dir,
                    env=env,
                )

            self.assertEqual(vector_summary["benchmark"], "vector_real_routing_sweep")
            self.assertEqual(vector_summary["dataset"], "stackoverflow-xlarge")
            self.assertTrue((output_dir / "vector_summary.json").exists())

    def test_real_vector_sweep_persists_intermediate_outputs(self) -> None:
        sweep_module = _load_module(
            "scripts/benchmarks/vector_search_real_sweep.py",
            "humemdb_vector_search_real_sweep_persist",
        )
        build_payload = sweep_module._build_payload
        persist_outputs = sweep_module._persist_outputs

        report = {
            "dataset": "stackoverflow-xlarge",
            "rows": 10_000,
            "dimensions": 384,
            "top_k": 10,
            "filter_source": "questions",
            "sample_mode": "stratified",
            "filtered_candidate_count": 4_000,
            "lancedb_index_settings": {"index_type": "IVF_PQ"},
            "lancedb_search_settings": {},
            "latency_summaries_ms": {
                "numpy_f32_filtered": {"mean": 2.0},
                "lancedb_indexed_filtered": {"mean": 1.5},
            },
            "recalls_at_k": {"lancedb_indexed_filtered": 0.96},
            "stage_timings_ms": {"dataset_load": 10.0},
            "memory_snapshots_bytes": {
                "after_dataset_load": 10,
                "after_numpy_exact_build": 12,
                "after_lancedb_table_create": 20,
                "after_lancedb_index_build": 30,
                "after_numpy_exact_search": 31,
                "after_lancedb_indexed_search": 32,
                "after_query_and_recall": 33,
            },
            "memory_stage_deltas_bytes": {
                "after_numpy_exact_build": 2,
                "after_lancedb_table_create": 8,
                "after_lancedb_index_build": 10,
                "after_numpy_exact_search": 1,
                "after_lancedb_indexed_search": 1,
                "after_query_and_recall": 1,
            },
        }

        payload = build_payload(
            dataset="stackoverflow-xlarge",
            rows_grid=[10_000],
            top_k_grid=[10],
            queries=100,
            warmup=1,
            repetitions=3,
            metric="cosine",
            sample_mode="stratified",
            filter_sources=[None, "questions"],
            lancedb_index_type="IVF_PQ",
            reports=[report],
        )

        scenario = payload["scenario_summaries"][0]
        self.assertEqual(scenario["recall_target_at_k"], 0.95)
        self.assertEqual(scenario["recall_target_scale"], "100K")
        self.assertTrue(scenario["meets_recall_target"])
        self.assertEqual(payload["overall"]["met_recall_target_count"], 1)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_json = Path(tmpdir) / "summary.json"
            intermediate_dir = Path(tmpdir) / "scenarios"
            persist_outputs(
                payload=payload,
                output_json=output_json,
                intermediate_dir=intermediate_dir,
                latest_reports=[report],
            )

            self.assertTrue(output_json.exists())
            self.assertTrue(
                (
                    intermediate_dir
                    / "stackoverflow-xlarge_rows10000_topk10_questions.json"
                ).exists()
            )

    def test_real_vector_sweep_expands_multi_top_k_payloads(self) -> None:
        sweep_module = _load_module(
            "scripts/benchmarks/vector_search_real_sweep.py",
            "humemdb_vector_search_real_sweep_multi_topk",
        )
        run_command = sweep_module._run_benchmark_command

        payload = {
            "dataset": "msmarco-10m",
            "rows": 100_000,
            "dimensions": 1024,
            "metric": "cosine",
            "queries": 100,
            "filter_source": None,
            "sample_mode": "auto",
            "filtered_candidate_count": None,
            "available_filter_sources": ["all"],
            "cold_tier_ingest_path": (
                "SQLite -> DuckDB (scan) -> Arrow batches -> LanceDB -> build index"
            ),
            "lancedb_thread_limit": "8",
            "arrow_cpu_count": 8,
            "numpy_thread_limit": 8,
            "lancedb_index_settings": {"index_type": "IVF_PQ"},
            "lancedb_search_settings": {},
            "stage_timings_ms": {"lancedb_index_build": 1.0},
            "artifact_sizes_bytes": {"numpy_f32_matrix": 0, "query_batch_f32": 0},
            "memory_snapshots_bytes": {},
            "memory_stage_deltas_bytes": {},
            "memory_peak_rss_bytes": 0,
            "top_k_grid": [10, 50],
            "top_k_reports": [
                {
                    "top_k": 10,
                    "latency_summaries_ms": {"lancedb_indexed_global": {"mean": 1.0}},
                    "recalls_at_k": {"lancedb_indexed_global": 0.95},
                },
                {
                    "top_k": 50,
                    "latency_summaries_ms": {"lancedb_indexed_global": {"mean": 2.0}},
                    "recalls_at_k": {"lancedb_indexed_global": 0.97},
                },
            ],
        }

        with mock.patch.object(sweep_module.subprocess, "run") as run_mock:
            run_mock.return_value = mock.Mock(stdout=json.dumps(payload))
            reports = run_command(
                dataset="msmarco-10m",
                rows=100_000,
                top_k_grid=[10, 50],
                queries=100,
                warmup=1,
                repetitions=3,
                metric="cosine",
                sample_mode="auto",
                filter_source=None,
                lancedb_index_type="IVF_PQ",
                lancedb_num_partitions=None,
                lancedb_num_sub_vectors=None,
                lancedb_num_bits=8,
                lancedb_sample_rate=256,
                lancedb_max_iterations=50,
                lancedb_nprobes=None,
                lancedb_refine_factor=None,
            )

        self.assertEqual([report["top_k"] for report in reports], [10, 50])

    def test_real_vector_memory_stage_deltas_are_derived_in_order(self) -> None:
        benchmark_module = _load_module(
            "scripts/benchmarks/vector_search_real.py",
            "humemdb_vector_search_real_memory_deltas",
        )
        memory_stage_deltas = benchmark_module._memory_stage_deltas

        deltas = memory_stage_deltas(
            {
                "start": 100,
                "after_dataset_load": 140,
                "after_numpy_exact_build": 180,
                "after_lancedb_table_create": 220,
                "after_lancedb_index_build": 260,
                "after_numpy_exact_search": 290,
                "after_lancedb_indexed_search": 310,
                "after_query_and_recall": 315,
                "final": 300,
            }
        )

        self.assertIsNone(deltas["start"])
        self.assertEqual(deltas["after_dataset_load"], 40)
        self.assertEqual(deltas["after_numpy_exact_build"], 40)
        self.assertEqual(deltas["after_lancedb_table_create"], 40)
        self.assertEqual(deltas["after_lancedb_index_build"], 40)
        self.assertEqual(deltas["after_numpy_exact_search"], 30)
        self.assertEqual(deltas["after_lancedb_indexed_search"], 20)
        self.assertEqual(deltas["after_query_and_recall"], 5)
