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
    def test_sql_workloads_still_emit_router_feature_shapes(self) -> None:
        benchmark_module = _load_module(
            "scripts/benchmarks/duckdb_direct_read.py",
            "humemdb_duckdb_direct_read",
        )
        workloads = getattr(benchmark_module, "QUERY_WORKLOADS")
        sql_feature_dict = getattr(benchmark_module, "_sql_feature_dict")

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
        dataset_counts = getattr(benchmark_module, "_dataset_counts")
        workloads_for = getattr(benchmark_module, "_workloads")
        compile_workload = getattr(benchmark_module, "_compile_workload")

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
        self.assertIn("social_reverse_expand_ordered", workloads)
        self.assertIn(
            "MATCH (b:User)<-[r:KNOWS]-(a:User)",
            workloads["social_reverse_expand_ordered"].query,
        )
        self.assertIn(
            "ORDER BY r.since DESC LIMIT 500",
            workloads["social_reverse_expand_ordered"].query,
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
        sql_workload_report = getattr(report_module, "_sql_workload_report")
        recommended_sql_olap_thresholds = getattr(
            report_module,
            "_recommended_sql_olap_thresholds",
        )

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
        recommended_sql_olap_thresholds = getattr(
            report_module,
            "_recommended_sql_olap_thresholds",
        )

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
        cypher_workload_report = getattr(report_module, "_cypher_workload_report")

        report = cypher_workload_report(
            [
                {
                    "scale_value": 100_000,
                    "workloads": {
                        "anchored_user_lookup": {
                            "family": "graph",
                            "shape": "node_lookup",
                            "selectivity": "high",
                            "sqlite_raw_sql": {"mean_ms": 0.3},
                            "duckdb_raw_sql": {"mean_ms": 10.0},
                        },
                        "broad_social_fanout": {
                            "family": "graph",
                            "shape": "fanout",
                            "selectivity": "broad",
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
                            "sqlite_raw_sql": {"mean_ms": 0.5},
                            "duckdb_raw_sql": {"mean_ms": 12.0},
                        },
                        "broad_social_fanout": {
                            "family": "graph",
                            "shape": "fanout",
                            "selectivity": "broad",
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

    def test_vector_threshold_report_keeps_filtered_crossovers_stable(self) -> None:
        report_module = _load_module(
            "scripts/benchmarks/routing_threshold_report.py",
            "humemdb_routing_threshold_report_vector",
        )
        vector_workload_report = getattr(report_module, "_vector_workload_report")

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
        print_section = getattr(report_module, "_print_section")

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

    def test_routing_sweep_writes_representative_scale_summaries(self) -> None:
        sweep_module = _load_module(
            "scripts/benchmarks/routing_sweep.py",
            "humemdb_routing_sweep",
        )
        run_sql_sweep = getattr(sweep_module, "_run_sql_sweep")
        run_cypher_sweep = getattr(sweep_module, "_run_cypher_sweep")

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
                payload = {
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
            self.assertEqual(
                [run["scale_key"] for run in cypher_summary["runs"]],
                ["nodes"],
            )
            self.assertTrue((output_dir / "sql_summary.json").exists())
            self.assertTrue((output_dir / "cypher_summary.json").exists())

    def test_routing_sweep_writes_vector_scale_summaries(self) -> None:
        sweep_module = _load_module(
            "scripts/benchmarks/routing_sweep.py",
            "humemdb_routing_sweep_vector",
        )
        run_vector_sweep = getattr(sweep_module, "_run_vector_sweep")

        def fake_run(
            _command: list[str],
            *,
            check: bool,
            env: dict[str, str],
            stdout=None,
            text: bool | None = None,
        ):
            self.assertTrue(check)
            self.assertIn("HUMEMDB_THREADS", env)
            self.assertIsNotNone(stdout)
            self.assertTrue(text)
            payload = {
                "acceptance_thresholds": {"indexed_recall": 0.95},
                "grid": {
                    "rows": [2_000, 10_000],
                    "dimensions": [256],
                    "top_k": [10],
                },
                "scenario_summaries": [
                    {
                        "rows": 2_000,
                        "dimensions": 256,
                        "top_k": 10,
                        "filtered_candidate_count": 200,
                        "lancedb_strategy": "default",
                        "latency_mean_ms": {
                            "numpy_f32_filtered": 0.20,
                            "lancedb_indexed_filtered": 0.35,
                        },
                        "recalls_at_k": {"lancedb_indexed_filtered": 0.99},
                    },
                    {
                        "rows": 10_000,
                        "dimensions": 256,
                        "top_k": 10,
                        "filtered_candidate_count": 500,
                        "lancedb_strategy": "tuned",
                        "latency_mean_ms": {
                            "numpy_f32_filtered": 0.80,
                            "lancedb_indexed_filtered": 0.45,
                        },
                        "recalls_at_k": {"lancedb_indexed_filtered": 0.97},
                    },
                ],
                "overall": {"scenario_count": 2},
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
                    scales=(2_000, 10_000),
                    dimensions_grid=(256,),
                    top_k_grid=(10,),
                    queries=8,
                    warmup=0,
                    repetitions=1,
                    output_dir=output_dir,
                    env=env,
                )

            self.assertEqual(vector_summary["benchmark"], "vector_routing_sweep")
            self.assertEqual(
                vector_summary["grid"]["rows"],
                [2_000, 10_000],
            )
            self.assertEqual(
                [scenario["rows"] for scenario in vector_summary["scenario_summaries"]],
                [2_000, 10_000],
            )
            self.assertTrue((output_dir / "vector_summary.json").exists())
