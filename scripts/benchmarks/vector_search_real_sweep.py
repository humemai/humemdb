from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


_MIN_BENCHMARK_QUERIES = 100
_MIN_BENCHMARK_REPETITIONS = 3

_RECALL_POLICY_BY_TOP_K: dict[int, tuple[tuple[int, float], ...]] = {
    10: (
        (100_000, 0.95),
        (1_000_000, 0.93),
        (10_000_000, 0.90),
        (25_000_000, 0.89),
        (100_000_000, 0.88),
    ),
    50: (
        (100_000, 0.98),
        (1_000_000, 0.96),
        (10_000_000, 0.95),
        (25_000_000, 0.94),
        (100_000_000, 0.93),
    ),
}


def _progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sweep the fixed hot-tier NumPy exact path and cold-tier LanceDB IVF_PQ "
            "path across real dataset scales."
        )
    )
    parser.add_argument(
        "--dataset",
        choices=("msmarco-10m", "stackoverflow-xlarge"),
        required=True,
    )
    parser.add_argument("--rows-grid", default="100000,1000000")
    parser.add_argument("--top-k-grid", default="10")
    parser.add_argument("--queries", type=int, default=_MIN_BENCHMARK_QUERIES)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--metric", choices=("cosine", "dot", "l2"), default="cosine")
    parser.add_argument(
        "--sample-mode",
        choices=("auto", "prefix", "stratified"),
        default="auto",
    )
    parser.add_argument(
        "--filter-sources",
        default="auto",
        help=(
            "Comma-separated filter families for stackoverflow-xlarge, or 'auto' "
            "to use all shipped corpora plus the unfiltered scenario."
        ),
    )
    parser.add_argument(
        "--lancedb-index-type",
        choices=("IVF_PQ",),
        default="IVF_PQ",
    )
    parser.add_argument("--lancedb-num-partitions", type=int)
    parser.add_argument("--lancedb-num-sub-vectors", type=int)
    parser.add_argument("--lancedb-num-bits", type=int, default=8)
    parser.add_argument("--lancedb-sample-rate", type=int, default=256)
    parser.add_argument("--lancedb-max-iterations", type=int, default=50)
    parser.add_argument("--lancedb-nprobes", type=int)
    parser.add_argument("--lancedb-refine-factor", type=int)
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to persist the rolling and final summary as JSON.",
    )
    parser.add_argument(
        "--intermediate-dir",
        type=Path,
        default=None,
        help="Optional directory where per-scenario JSON results are written.",
    )
    parser.add_argument("--output", choices=("text", "json"), default="text")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    _validate_benchmark_sampling(args.queries, args.repetitions)
    rows_grid = _parse_int_grid(args.rows_grid, flag="--rows-grid")
    top_k_grid = _parse_int_grid(args.top_k_grid, flag="--top-k-grid")
    _validate_recall_policy_top_k_grid(top_k_grid)
    filter_sources = _filter_sources_for_dataset(
        dataset=args.dataset,
        raw=args.filter_sources,
    )

    reports = []
    scenario_id = 0
    total_scenarios = len(rows_grid) * len(filter_sources)
    _progress(
        "[vector_search_real_sweep] starting "
        f"{len(rows_grid) * len(top_k_grid) * len(filter_sources)} scenario results "
        f"across {total_scenarios} shared builds for dataset={args.dataset}"
    )
    for rows in rows_grid:
        for filter_source in filter_sources:
            scenario_id += 1
            _progress(
                "[vector_search_real_sweep] build "
                f"{scenario_id}/{total_scenarios}: rows={rows} "
                f"top_k_grid={top_k_grid} filter={filter_source or 'none'}"
            )
            scenario_reports = _run_scenario(
                dataset=args.dataset,
                rows=rows,
                top_k_grid=top_k_grid,
                queries=args.queries,
                warmup=args.warmup,
                repetitions=args.repetitions,
                metric=args.metric,
                sample_mode=args.sample_mode,
                filter_source=filter_source,
                lancedb_index_type=args.lancedb_index_type,
                lancedb_num_partitions=args.lancedb_num_partitions,
                lancedb_num_sub_vectors=args.lancedb_num_sub_vectors,
                lancedb_num_bits=args.lancedb_num_bits,
                lancedb_sample_rate=args.lancedb_sample_rate,
                lancedb_max_iterations=args.lancedb_max_iterations,
                lancedb_nprobes=args.lancedb_nprobes,
                lancedb_refine_factor=args.lancedb_refine_factor,
            )
            reports.extend(scenario_reports)
            payload = _build_payload(
                dataset=args.dataset,
                rows_grid=rows_grid,
                top_k_grid=top_k_grid,
                queries=args.queries,
                warmup=args.warmup,
                repetitions=args.repetitions,
                metric=args.metric,
                sample_mode=args.sample_mode,
                filter_sources=filter_sources,
                lancedb_index_type=args.lancedb_index_type,
                reports=reports,
            )
            _persist_outputs(
                payload=payload,
                output_json=args.output_json,
                intermediate_dir=args.intermediate_dir,
                latest_reports=scenario_reports,
            )

    payload = _build_payload(
        dataset=args.dataset,
        rows_grid=rows_grid,
        top_k_grid=top_k_grid,
        queries=args.queries,
        warmup=args.warmup,
        repetitions=args.repetitions,
        metric=args.metric,
        sample_mode=args.sample_mode,
        filter_sources=filter_sources,
        lancedb_index_type=args.lancedb_index_type,
        reports=reports,
    )

    if args.output_json is not None or args.intermediate_dir is not None:
        _persist_outputs(
            payload=payload,
            output_json=args.output_json,
            intermediate_dir=args.intermediate_dir,
            latest_reports=None,
        )

    if args.output == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    _print_text_report(payload)


def _build_payload(
    *,
    dataset: str,
    rows_grid: list[int],
    top_k_grid: list[int],
    queries: int,
    warmup: int,
    repetitions: int,
    metric: str,
    sample_mode: str,
    filter_sources: list[str | None],
    lancedb_index_type: str,
    reports: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "dataset": dataset,
        "grid": {
            "rows": rows_grid,
            "top_k": top_k_grid,
            "queries": queries,
            "warmup": warmup,
            "repetitions": repetitions,
            "metric": metric,
            "sample_mode": sample_mode,
            "filter_sources": [
                source if source is not None else "none"
                for source in filter_sources
            ],
            "lancedb_index_type": lancedb_index_type,
            "recall_policy": _recall_policy_table(),
        },
        "scenario_summaries": [_summarize_scenario(report) for report in reports],
    }
    payload["overall"] = _summarize_overall(payload["scenario_summaries"])
    return payload


def _parse_int_grid(raw: str, *, flag: str) -> list[int]:
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if not parts:
        raise ValueError(f"{flag} must contain at least one integer.")
    return [int(part) for part in parts]


def _persist_outputs(
    *,
    payload: dict[str, Any],
    output_json: Path | None,
    intermediate_dir: Path | None,
    latest_reports: list[dict[str, Any]] | None,
) -> None:
    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    if intermediate_dir is None or latest_reports is None:
        return
    intermediate_dir.mkdir(parents=True, exist_ok=True)
    for latest_report in latest_reports:
        scenario_path = intermediate_dir / _scenario_file_name(latest_report)
        scenario_path.write_text(
            json.dumps(latest_report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def _scenario_file_name(report: dict[str, Any]) -> str:
    dataset = str(report["dataset"])
    filter_source = report.get("filter_source") or "global"
    rows = int(report["rows"])
    top_k = int(report["top_k"])
    return f"{dataset}_rows{rows}_topk{top_k}_{filter_source}.json"


def _validate_benchmark_sampling(queries: int, repetitions: int) -> None:
    if queries < _MIN_BENCHMARK_QUERIES:
        raise ValueError(
            "Real vector sweep requires at least "
            f"{_MIN_BENCHMARK_QUERIES} queries."
        )
    if repetitions < _MIN_BENCHMARK_REPETITIONS:
        raise ValueError(
            "Real vector sweep requires at least "
            f"{_MIN_BENCHMARK_REPETITIONS} repetitions."
        )


def _filter_sources_for_dataset(*, dataset: str, raw: str) -> list[str | None]:
    if dataset == "msmarco-10m":
        return [None]
    if raw == "auto":
        return [None, "questions", "answers", "comments"]
    values = [part.strip() for part in raw.split(",") if part.strip()]
    if not values:
        return [None]
    result: list[str | None] = []
    for value in values:
        result.append(None if value == "none" else value)
    return result


def _run_scenario(
    *,
    dataset: str,
    rows: int,
    top_k_grid: list[int],
    queries: int,
    warmup: int,
    repetitions: int,
    metric: str,
    sample_mode: str,
    filter_source: str | None,
    lancedb_index_type: str,
    lancedb_num_partitions: int | None,
    lancedb_num_sub_vectors: int | None,
    lancedb_num_bits: int,
    lancedb_sample_rate: int,
    lancedb_max_iterations: int,
    lancedb_nprobes: int | None,
    lancedb_refine_factor: int | None,
) -> list[dict[str, Any]]:
    return _run_benchmark_command(
        dataset=dataset,
        rows=rows,
        top_k_grid=top_k_grid,
        queries=queries,
        warmup=warmup,
        repetitions=repetitions,
        metric=metric,
        sample_mode=sample_mode,
        filter_source=filter_source,
        lancedb_index_type=lancedb_index_type,
        lancedb_num_partitions=lancedb_num_partitions,
        lancedb_num_sub_vectors=lancedb_num_sub_vectors,
        lancedb_num_bits=lancedb_num_bits,
        lancedb_sample_rate=lancedb_sample_rate,
        lancedb_max_iterations=lancedb_max_iterations,
        lancedb_nprobes=lancedb_nprobes,
        lancedb_refine_factor=lancedb_refine_factor,
    )


def _run_benchmark_command(
    *,
    dataset: str,
    rows: int,
    top_k_grid: list[int],
    queries: int,
    warmup: int,
    repetitions: int,
    metric: str,
    sample_mode: str,
    filter_source: str | None,
    lancedb_index_type: str,
    lancedb_num_partitions: int | None,
    lancedb_num_sub_vectors: int | None,
    lancedb_num_bits: int,
    lancedb_sample_rate: int,
    lancedb_max_iterations: int,
    lancedb_nprobes: int | None,
    lancedb_refine_factor: int | None,
) -> list[dict[str, Any]]:
    script_path = Path(__file__).with_name("vector_search_real.py")
    command = [
        sys.executable,
        str(script_path),
        "--dataset",
        dataset,
        "--rows",
        str(rows),
        "--queries",
        str(queries),
        "--top-k-grid",
        ",".join(str(value) for value in top_k_grid),
        "--warmup",
        str(warmup),
        "--repetitions",
        str(repetitions),
        "--metric",
        metric,
        "--sample-mode",
        sample_mode,
        "--lancedb-index-type",
        lancedb_index_type,
        "--output",
        "json",
    ]
    if filter_source is not None:
        command.extend(["--filter-source", filter_source])
    if lancedb_num_partitions is not None:
        command.extend(["--lancedb-num-partitions", str(lancedb_num_partitions)])
    if lancedb_num_sub_vectors is not None:
        command.extend(["--lancedb-num-sub-vectors", str(lancedb_num_sub_vectors)])
    command.extend(["--lancedb-num-bits", str(lancedb_num_bits)])
    command.extend(["--lancedb-sample-rate", str(lancedb_sample_rate)])
    command.extend(["--lancedb-max-iterations", str(lancedb_max_iterations)])
    if lancedb_nprobes is not None:
        command.extend(["--lancedb-nprobes", str(lancedb_nprobes)])
    if lancedb_refine_factor is not None:
        command.extend(["--lancedb-refine-factor", str(lancedb_refine_factor)])

    completed = subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    payload = json.loads(completed.stdout)
    if "top_k_reports" not in payload:
        return [payload]
    shared = {
        key: value
        for key, value in payload.items()
        if key not in {"top_k_reports", "top_k_grid"}
    }
    return [
        {
            **shared,
            "top_k": int(report["top_k"]),
            "latency_summaries_ms": report["latency_summaries_ms"],
            "recalls_at_k": report["recalls_at_k"],
        }
        for report in payload["top_k_reports"]
    ]


def _summarize_scenario(report: dict[str, Any]) -> dict[str, Any]:
    recall_target = _recall_target_for(
        rows=int(report["rows"]),
        top_k=int(report["top_k"]),
    )
    indexed_key = (
        "lancedb_indexed_filtered"
        if report["filter_source"] is not None
        else "lancedb_indexed_global"
    )
    actual_recall = report["recalls_at_k"].get(indexed_key)
    return {
        "dataset": report["dataset"],
        "rows": report["rows"],
        "dimensions": report["dimensions"],
        "top_k": report["top_k"],
        "filter_source": report["filter_source"],
        "sample_mode": report["sample_mode"],
        "filtered_candidate_count": report["filtered_candidate_count"],
        "lancedb_index_settings": report["lancedb_index_settings"],
        "lancedb_search_settings": report["lancedb_search_settings"],
        "recall_target_at_k": recall_target["target_recall"],
        "recall_target_scale": recall_target["scale_label"],
        "recall_target_source": "policy",
        "meets_recall_target": (
            actual_recall is not None
            and actual_recall >= recall_target["target_recall"]
        ),
        "latency_mean_ms": {
            name: summary["mean"]
            for name, summary in report["latency_summaries_ms"].items()
        },
        "recalls_at_k": report["recalls_at_k"],
        "stage_timings_ms": report["stage_timings_ms"],
        "memory_snapshots_bytes": report["memory_snapshots_bytes"],
        "memory_stage_deltas_bytes": report.get("memory_stage_deltas_bytes", {}),
        "memory_summary_bytes": {
            "numpy_exact_build": report.get("memory_stage_deltas_bytes", {}).get(
                "after_numpy_exact_build"
            ),
            "numpy_exact_search": report.get("memory_stage_deltas_bytes", {}).get(
                "after_numpy_exact_search"
            ),
            "lancedb_table_create": report.get("memory_stage_deltas_bytes", {}).get(
                "after_lancedb_table_create"
            ),
            "lancedb_index_build": report.get("memory_stage_deltas_bytes", {}).get(
                "after_lancedb_index_build"
            ),
            "lancedb_indexed_search": report.get("memory_stage_deltas_bytes", {}).get(
                "after_lancedb_indexed_search"
            ),
            "recall_eval": report.get("memory_stage_deltas_bytes", {}).get(
                "after_query_and_recall"
            ),
            "peak_rss": report.get("memory_peak_rss_bytes"),
        },
    }


def _summarize_overall(scenarios: list[dict[str, Any]]) -> dict[str, Any]:
    filtered = [
        scenario for scenario in scenarios if scenario["filter_source"] is not None
    ]
    global_runs = [
        scenario for scenario in scenarios if scenario["filter_source"] is None
    ]
    return {
        "scenario_count": len(scenarios),
        "global_scenario_count": len(global_runs),
        "filtered_scenario_count": len(filtered),
        "met_recall_target_count": sum(
            1 for scenario in scenarios if scenario["meets_recall_target"]
        ),
    }


def _validate_recall_policy_top_k_grid(top_k_grid: list[int]) -> None:
    for top_k in top_k_grid:
        _recall_target_for(rows=1, top_k=top_k)


def _recall_policy_table() -> dict[str, dict[str, float]]:
    return {
        str(top_k): {
            str(row_limit): target
            for row_limit, target in thresholds
        }
        for top_k, thresholds in _RECALL_POLICY_BY_TOP_K.items()
    }


def _recall_target_for(*, rows: int, top_k: int) -> dict[str, Any]:
    thresholds = _RECALL_POLICY_BY_TOP_K.get(top_k)
    if thresholds is None:
        supported = ", ".join(str(value) for value in sorted(_RECALL_POLICY_BY_TOP_K))
        raise ValueError(
            "No recall policy is defined for "
            f"top_k={top_k}. Supported values: {supported}."
        )
    for row_limit, target_recall in thresholds:
        if rows <= row_limit:
            return {
                "scale_rows": row_limit,
                "scale_label": _format_scale_label(row_limit),
                "target_recall": target_recall,
            }
    row_limit, target_recall = thresholds[-1]
    return {
        "scale_rows": row_limit,
        "scale_label": f">{_format_scale_label(row_limit)}",
        "target_recall": target_recall,
    }


def _format_scale_label(rows: int) -> str:
    if rows >= 1_000_000:
        whole_millions = rows // 1_000_000
        return f"{whole_millions}M"
    whole_thousands = rows // 1_000
    return f"{whole_thousands}K"


def _print_text_report(payload: dict[str, Any]) -> None:
    print("Real vector routing sweep")
    print(f"  dataset: {payload['dataset']}")
    print(f"  rows grid: {payload['grid']['rows']}")
    print(f"  top_k grid: {payload['grid']['top_k']}")
    print(f"  filter sources: {payload['grid']['filter_sources']}")
    print(f"  sample mode: {payload['grid']['sample_mode']}")
    print(f"  recall policy: {payload['grid']['recall_policy']}")
    print()
    print("Scenario summaries")
    for scenario in payload["scenario_summaries"]:
        filter_name = scenario["filter_source"] or "none"
        indexed_key = (
            "lancedb_indexed_filtered"
            if scenario["filter_source"] is not None
            else "lancedb_indexed_global"
        )
        numpy_key = (
            "numpy_f32_filtered"
            if scenario["filter_source"] is not None
            else "numpy_f32_global"
        )
        recall = scenario["recalls_at_k"].get(indexed_key)
        numpy_mean = scenario["latency_mean_ms"].get(numpy_key)
        numpy_text = "n/a"
        if numpy_mean is not None:
            numpy_text = f"{numpy_mean:.2f}ms"
        recall_text = "n/a"
        if recall is not None:
            recall_text = f"{recall:.4f}"
        status_text = "pass" if scenario["meets_recall_target"] else "fail"
        print(
            "  "
            f"rows={scenario['rows']} top_k={scenario['top_k']} filter={filter_name} "
            f"numpy={numpy_text} "
            f"lancedb={scenario['latency_mean_ms'][indexed_key]:.2f}ms "
            f"recall={recall_text} "
            f"target>={scenario['recall_target_at_k']:.2f}"
            f"@{scenario['recall_target_scale']} "
            f"status={status_text}"
        )


if __name__ == "__main__":
    main()
