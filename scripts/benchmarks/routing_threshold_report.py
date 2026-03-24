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
                "first_duckdb_scale": first_duckdb_scale,
                "winners": winners,
            }
        )
    return report


def _cypher_workload_report(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    first_run_workloads = runs[0]["workloads"]
    report: list[dict[str, Any]] = []
    for workload_name, metadata in first_run_workloads.items():
        winners: list[dict[str, Any]] = []
        for run in runs:
            workload = run["workloads"][workload_name]
            sqlite_mean = workload["sqlite_cypher"]["mean_ms"]
            duckdb_mean = workload["duckdb_cypher"]["mean_ms"]
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


def _print_section(title: str, report: list[dict[str, Any]]) -> None:
    print(title)
    print("-" * len(title))
    for entry in report:
        first_duckdb_scale = entry["first_duckdb_scale"]
        if first_duckdb_scale is None:
            threshold_text = "no DuckDB crossover in current sweep"
        else:
            threshold_text = f"DuckDB first wins at scale {first_duckdb_scale}"
        print(
            f"{entry['workload']}: {threshold_text} "
            f"(family={entry['family']}, shape={entry['shape']}, "
            f"selectivity={entry['selectivity']})"
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
        _print_section("SQL routing crossover summary", sql_report)

    cypher_summary = summary.get("cypher")
    if isinstance(cypher_summary, dict):
        cypher_report = _cypher_workload_report(cypher_summary["runs"])
        report["cypher"] = cypher_report
        _print_section("Cypher routing crossover summary", cypher_report)

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
