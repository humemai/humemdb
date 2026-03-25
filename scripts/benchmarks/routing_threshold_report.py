from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Summarize routing crossover points from JSON outputs produced by the "
            "routing benchmark sweep."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path(
            "scripts/benchmarks/results/routing_sweep/"
            "routing_sweep_summary.json"
        ),
        help="Merged routing sweep summary JSON file.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to persist the summarized threshold report as JSON.",
    )
    return parser.parse_args()


def _winner(sqlite_mean: float, duckdb_mean: float) -> str:
    return "sqlite" if sqlite_mean <= duckdb_mean else "duckdb"


def _sql_workload_report(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    first_run_workloads = runs[0]["workloads"]
    report: list[dict[str, Any]] = []
    for workload_name, metadata in first_run_workloads.items():
        winners: list[dict[str, Any]] = []
        for run in runs:
            workload = run["workloads"][workload_name]
            sqlite_mean = workload["sqlite"]["mean_ms"]
            duckdb_mean = workload["duckdb"]["mean_ms"]
            winners.append(
                {
                    "scale": run["scale_value"],
                    "winner": _winner(sqlite_mean, duckdb_mean),
                    "sqlite_mean_ms": sqlite_mean,
                    "duckdb_mean_ms": duckdb_mean,
                }
            )
        first_duckdb_scale = next(
            (entry["scale"] for entry in winners if entry["winner"] == "duckdb"),
            None,
        )
        report.append(
            {
                "workload": workload_name,
                "family": metadata["family"],
                "shape": metadata["shape"],
                "selectivity": metadata["selectivity"],
                "sql_features": metadata.get("sql_features"),
                "first_duckdb_scale": first_duckdb_scale,
                "winners": winners,
            }
        )
    return report


def _recommended_sql_olap_thresholds(
    runs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Emit a conservative runtime calibration block from SQL crossover evidence."""

    sql_report = _sql_workload_report(runs)
    duckdb_winning = [
        entry["workload"]
        for entry in sql_report
        if entry["first_duckdb_scale"] is not None
    ]
    sqlite_preferring = [
        entry["workload"]
        for entry in sql_report
        if entry["first_duckdb_scale"] is None
    ]
    candidate_rules = _recommended_sql_olap_rules(sql_report)

    return {
        "benchmark_calibrated": bool(duckdb_winning and sqlite_preferring),
        "min_join_count": 0,
        "min_aggregate_count": 0,
        "min_cte_count": 0,
        "min_window_count": 0,
        "require_order_by_or_limit": False,
        "rules": candidate_rules,
        "evidence": {
            "duckdb_winning_workloads": duckdb_winning,
            "sqlite_preferring_workloads": sqlite_preferring,
        },
    }


def _recommended_sql_olap_rules(
    sql_report: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return conservative DuckDB routing rules learned from SQL crossover data."""

    sqlite_preferring = [
        entry
        for entry in sql_report
        if entry["first_duckdb_scale"] is None
    ]
    seen: set[tuple[tuple[str, Any], ...]] = set()
    rules: list[dict[str, Any]] = []

    for entry in sql_report:
        if entry["first_duckdb_scale"] is None:
            continue
        features = entry.get("sql_features")
        if not isinstance(features, dict):
            continue
        candidate = {
            "min_join_count": int(features.get("join_count", 0)),
            "min_aggregate_count": int(features.get("aggregate_count", 0)),
            "min_cte_count": int(features.get("cte_count", 0)),
            "min_window_count": int(features.get("window_count", 0)),
            "min_exists_count": int(features.get("exists_count", 0)),
            "require_group_by": bool(features.get("has_group_by", False)),
            "require_distinct": bool(features.get("has_distinct", False)),
            "require_order_by_or_limit": bool(
                features.get("has_order_by", False)
                or features.get("has_limit", False)
            ),
        }
        candidate_key = tuple(sorted(candidate.items()))
        if candidate_key in seen:
            continue
        if any(
            _sql_features_match_rule(pref.get("sql_features"), candidate)
            for pref in sqlite_preferring
        ):
            continue
        seen.add(candidate_key)
        rules.append(candidate)

    return sorted(rules, key=_sql_rule_sort_key)


def _sql_features_match_rule(
    features: dict[str, Any] | None,
    rule: dict[str, Any],
) -> bool:
    """Return whether one benchmark feature payload satisfies one rule payload."""

    if not isinstance(features, dict):
        return False

    if int(features.get("join_count", 0)) < int(rule.get("min_join_count", 0)):
        return False
    if int(features.get("aggregate_count", 0)) < int(
        rule.get("min_aggregate_count", 0)
    ):
        return False
    if int(features.get("cte_count", 0)) < int(rule.get("min_cte_count", 0)):
        return False
    if int(features.get("window_count", 0)) < int(
        rule.get("min_window_count", 0)
    ):
        return False
    if int(features.get("exists_count", 0)) < int(
        rule.get("min_exists_count", 0)
    ):
        return False
    if bool(rule.get("require_group_by", False)) and not bool(
        features.get("has_group_by", False)
    ):
        return False
    if bool(rule.get("require_distinct", False)) and not bool(
        features.get("has_distinct", False)
    ):
        return False
    if bool(rule.get("require_order_by_or_limit", False)) and not (
        bool(features.get("has_order_by", False))
        or bool(features.get("has_limit", False))
    ):
        return False
    return True


def _sql_rule_sort_key(
    rule: dict[str, Any],
) -> tuple[int, int, int, int, int, int, int, int]:
    """Return a stable sort key for emitted SQL routing rules."""

    return (
        int(rule.get("min_window_count", 0)),
        int(rule.get("min_exists_count", 0)),
        int(rule.get("min_cte_count", 0)),
        int(rule.get("min_aggregate_count", 0)),
        int(rule.get("min_join_count", 0)),
        int(bool(rule.get("require_group_by", False))),
        int(bool(rule.get("require_distinct", False))),
        int(bool(rule.get("require_order_by_or_limit", False))),
    )


def _cypher_workload_report(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    first_run_workloads = runs[0]["workloads"]
    report: list[dict[str, Any]] = []
    for workload_name, metadata in first_run_workloads.items():
        winners: list[dict[str, Any]] = []
        for run in runs:
            workload = run["workloads"][workload_name]
            sqlite_mean = workload["sqlite_raw_sql"]["mean_ms"]
            duckdb_mean = workload["duckdb_raw_sql"]["mean_ms"]
            winners.append(
                {
                    "scale": run["scale_value"],
                    "winner": _winner(sqlite_mean, duckdb_mean),
                    "sqlite_mean_ms": sqlite_mean,
                    "duckdb_mean_ms": duckdb_mean,
                }
            )
        first_duckdb_scale = next(
            (entry["scale"] for entry in winners if entry["winner"] == "duckdb"),
            None,
        )
        report.append(
            {
                "workload": workload_name,
                "family": metadata["family"],
                "shape": metadata["shape"],
                "selectivity": metadata["selectivity"],
                "first_duckdb_scale": first_duckdb_scale,
                "winners": winners,
            }
        )
    return report


def _vector_workload_report(
    scenario_summaries: list[dict[str, Any]],
    *,
    min_indexed_recall: float,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for scenario in scenario_summaries:
        key = (scenario["dimensions"], scenario["top_k"])
        grouped.setdefault(key, []).append(scenario)

    report: list[dict[str, Any]] = []
    for (dimensions, top_k), scenarios in sorted(grouped.items()):
        ordered = sorted(scenarios, key=lambda scenario: scenario["rows"])
        winners: list[dict[str, Any]] = []
        for scenario in ordered:
            numpy_mean = scenario["latency_mean_ms"]["numpy_f32_filtered"]
            indexed_mean = scenario["latency_mean_ms"]["lancedb_indexed_filtered"]
            indexed_recall = scenario["recalls_at_k"]["lancedb_indexed_filtered"]
            winner = "numpy_exact"
            if indexed_recall >= min_indexed_recall and indexed_mean < numpy_mean:
                winner = "lancedb_indexed"
            winners.append(
                {
                    "scale": scenario["rows"],
                    "winner": winner,
                    "numpy_f32_filtered_mean_ms": numpy_mean,
                    "lancedb_indexed_filtered_mean_ms": indexed_mean,
                    "lancedb_indexed_filtered_recall": indexed_recall,
                    "filtered_candidate_count": scenario["filtered_candidate_count"],
                    "strategy": scenario["lancedb_strategy"],
                }
            )

        first_indexed_scale = next(
            (
                entry["scale"]
                for entry in winners
                if entry["winner"] == "lancedb_indexed"
            ),
            None,
        )
        report.append(
            {
                "workload": f"vector_dims{dimensions}_topk{top_k}",
                "family": "vector",
                "shape": "candidate_filtered_ann",
                "dimensions": dimensions,
                "top_k": top_k,
                "first_indexed_scale": first_indexed_scale,
                "winners": winners,
            }
        )

    return report


def _print_section(title: str, report: list[dict[str, Any]]) -> None:
    print(title)
    print("-" * len(title))
    for entry in report:
        if "first_duckdb_scale" in entry:
            first_scale = entry["first_duckdb_scale"]
            if first_scale is None:
                threshold_text = "no DuckDB crossover in current sweep"
            else:
                threshold_text = f"DuckDB first wins at scale {first_scale}"
        elif "first_indexed_scale" in entry:
            first_scale = entry["first_indexed_scale"]
            if first_scale is None:
                threshold_text = "no indexed crossover in current sweep"
            else:
                threshold_text = f"Indexed first wins at scale {first_scale}"
        else:
            raise KeyError(
                "Report entries must include either 'first_duckdb_scale' or "
                "'first_indexed_scale'."
            )
        print(
            f"{entry['workload']}: {threshold_text} "
            f"(family={entry['family']}, shape={entry['shape']}, "
            f"selectivity={entry.get('selectivity', 'n/a')})"
        )
    print()


def main() -> None:
    args = _parse_args()
    summary = json.loads(args.input.read_text(encoding="utf-8"))
    report: dict[str, Any] = {
        "source": str(args.input),
        "thread_limit": summary.get("thread_limit", "unknown"),
    }

    sql_summary = summary.get("sql")
    if isinstance(sql_summary, dict):
        sql_report = _sql_workload_report(sql_summary["runs"])
        report["sql"] = sql_report
        report.setdefault("recommended_runtime", {})["sql_olap_thresholds"] = (
            _recommended_sql_olap_thresholds(sql_summary["runs"])
        )
        _print_section("SQL routing crossover summary", sql_report)

    cypher_summary = summary.get("cypher")
    if isinstance(cypher_summary, dict):
        cypher_report = _cypher_workload_report(cypher_summary["runs"])
        report["cypher"] = cypher_report
        _print_section("Cypher routing crossover summary", cypher_report)

    vector_summary = summary.get("vector")
    if isinstance(vector_summary, dict):
        acceptance_thresholds = vector_summary.get("acceptance_thresholds", {})
        indexed_threshold = acceptance_thresholds.get("indexed_recall", 0.95)
        vector_report = _vector_workload_report(
            vector_summary["scenario_summaries"],
            min_indexed_recall=indexed_threshold,
        )
        report["vector"] = vector_report
        _print_section("Vector routing crossover summary", vector_report)

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
