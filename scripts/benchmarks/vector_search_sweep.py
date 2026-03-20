from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from pathlib import Path
from typing import Any


def _progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sweep vector benchmark scenarios to estimate a routing rule of thumb "
            "between NumPy and LanceDB."
        )
    )
    parser.add_argument("--rows-grid", default="2000,10000,50000")
    parser.add_argument("--dimensions-grid", default="64,256,768")
    parser.add_argument("--top-k-grid", default="10")
    parser.add_argument("--queries", type=int, default=16)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument(
        "--skip-numpy-sq8",
        action="store_true",
        help=(
            "Skip the scalar-int8 NumPy approximation path. Recommended for most "
            "current sweeps because NumPy SQ8 is usually slower than NumPy FP32 in "
            "this implementation."
        ),
    )
    parser.add_argument("--metric", choices=("cosine", "dot", "l2"), default="cosine")
    parser.add_argument("--buckets", type=int, default=128)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--lancedb-mode",
        choices=("default", "tuned"),
        default="default",
        help=(
            "Compare against LanceDB library defaults or the best candidate from "
            "the curated tuning search."
        ),
    )
    parser.add_argument(
        "--lancedb-tuned-family",
        choices=("all", "ivf_pq", "ivf_flat", "ivf_hnsw_sq"),
        default="all",
        help="Restrict tuned LanceDB search to one candidate family.",
    )
    parser.add_argument("--min-indexed-recall", type=float, default=0.95)
    parser.add_argument("--min-sq8-recall", type=float, default=0.99)
    parser.add_argument("--output", choices=("text", "json"), default="text")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    rows_grid = _parse_int_grid(args.rows_grid, flag="--rows-grid")
    dimensions_grid = _parse_int_grid(args.dimensions_grid, flag="--dimensions-grid")
    top_k_grid = _parse_int_grid(args.top_k_grid, flag="--top-k-grid")
    total_scenarios = len(rows_grid) * len(dimensions_grid) * len(top_k_grid)

    _progress(
        "[vector_search_sweep] starting "
        f"{total_scenarios} scenarios with lancedb_mode={args.lancedb_mode}, "
        f"queries={args.queries}, repetitions={args.repetitions}"
    )

    reports = []
    scenario_id = 0
    for rows in rows_grid:
        for dimensions in dimensions_grid:
            for top_k in top_k_grid:
                _progress(
                    "[vector_search_sweep] scenario "
                    f"{scenario_id + 1}/{total_scenarios}: "
                    f"rows={rows} dims={dimensions} top_k={top_k}"
                )
                report = _run_scenario(
                    rows=rows,
                    dimensions=dimensions,
                    top_k=top_k,
                    queries=args.queries,
                    warmup=args.warmup,
                    repetitions=args.repetitions,
                    skip_numpy_sq8=args.skip_numpy_sq8,
                    metric=args.metric,
                    buckets=args.buckets,
                    seed=args.seed + scenario_id,
                    lancedb_mode=args.lancedb_mode,
                    lancedb_tuned_family=args.lancedb_tuned_family,
                    target_recall=args.min_indexed_recall,
                )
                reports.append(report)
                _progress(
                    "[vector_search_sweep] completed scenario "
                    f"{scenario_id + 1}/{total_scenarios}: "
                    f"rows={rows} dims={dimensions} top_k={top_k}"
                )
                scenario_id += 1

    scenario_summaries = [
        _summarize_scenario(
            report,
            min_indexed_recall=args.min_indexed_recall,
            min_sq8_recall=args.min_sq8_recall,
        )
        for report in reports
    ]
    overall = _summarize_overall(
        scenario_summaries,
        min_indexed_recall=args.min_indexed_recall,
    )

    payload = {
        "grid": {
            "rows": rows_grid,
            "dimensions": dimensions_grid,
            "top_k": top_k_grid,
            "queries": args.queries,
            "warmup": args.warmup,
            "repetitions": args.repetitions,
            "skip_numpy_sq8": args.skip_numpy_sq8,
            "metric": args.metric,
            "buckets": args.buckets,
            "seed": args.seed,
            "lancedb_mode": args.lancedb_mode,
            "lancedb_tuned_family": args.lancedb_tuned_family,
        },
        "scenario_summaries": scenario_summaries,
        "overall": overall,
    }

    if args.output == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    _print_text_report(payload)


def _parse_int_grid(value: str, *, flag: str) -> list[int]:
    parts = [part.strip() for part in value.split(",") if part.strip()]
    if not parts:
        raise ValueError(f"{flag} must contain at least one integer.")
    parsed = []
    for part in parts:
        try:
            parsed_value = int(part)
        except ValueError as exc:
            raise ValueError(f"{flag} contains a non-integer value: {part!r}.") from exc
        if parsed_value < 1:
            raise ValueError(f"{flag} values must be at least 1.")
        parsed.append(parsed_value)
    return parsed


def _run_scenario(
    *,
    rows: int,
    dimensions: int,
    top_k: int,
    queries: int,
    warmup: int,
    repetitions: int,
    skip_numpy_sq8: bool,
    metric: str,
    buckets: int,
    seed: int,
    lancedb_mode: str,
    lancedb_tuned_family: str,
    target_recall: float,
) -> dict[str, Any]:
    script_path = Path(__file__).with_name("vector_search.py")
    command = [
        sys.executable,
        str(script_path),
        "--rows",
        str(rows),
        "--dimensions",
        str(dimensions),
        "--top-k",
        str(top_k),
        "--queries",
        str(queries),
        "--warmup",
        str(warmup),
        "--repetitions",
        str(repetitions),
        *(["--skip-numpy-sq8"] if skip_numpy_sq8 else []),
        "--metric",
        metric,
        "--buckets",
        str(buckets),
        "--seed",
        str(seed),
        "--output",
        "json",
    ]
    completed = subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    report = json.loads(completed.stdout)
    if lancedb_mode == "tuned":
        report["tuned_lancedb"] = _run_tuned_lancedb(
            rows=rows,
            dimensions=dimensions,
            top_k=top_k,
            queries=queries,
            warmup=warmup,
            repetitions=repetitions,
            skip_numpy_sq8=skip_numpy_sq8,
            metric=metric,
            buckets=buckets,
            seed=seed,
            candidate_family=lancedb_tuned_family,
            target_recall=target_recall,
        )
    return report


def _run_tuned_lancedb(
    *,
    rows: int,
    dimensions: int,
    top_k: int,
    queries: int,
    warmup: int,
    repetitions: int,
    skip_numpy_sq8: bool,
    metric: str,
    buckets: int,
    seed: int,
    candidate_family: str,
    target_recall: float,
) -> dict[str, Any]:
    script_path = Path(__file__).with_name("vector_search_tune_lancedb.py")
    _progress(
        "[vector_search_sweep] tuning LanceDB candidate set for "
        f"rows={rows} dims={dimensions} top_k={top_k} "
        f"family={candidate_family}"
    )
    command = [
        sys.executable,
        str(script_path),
        "--rows",
        str(rows),
        "--dimensions",
        str(dimensions),
        "--top-k",
        str(top_k),
        "--queries",
        str(queries),
        "--warmup",
        str(warmup),
        "--repetitions",
        str(repetitions),
        *(["--skip-numpy-sq8"] if skip_numpy_sq8 else []),
        "--metric",
        metric,
        "--buckets",
        str(buckets),
        "--seed",
        str(seed),
        "--candidate-family",
        candidate_family,
        "--target-recall",
        str(target_recall),
        "--output",
        "json",
    ]
    completed = subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    payload = json.loads(completed.stdout)
    if payload["best"] is not None:
        selected = payload["best"]
        selected["selection_reason"] = "lowest_latency_meeting_target_recall"
        _progress(
            "[vector_search_sweep] tuned LanceDB selected "
            f"{selected['name']} for rows={rows} dims={dimensions} "
            f"with recall={selected['indexed_recall_global']:.4f}"
        )
        return selected

    fallback = max(
        payload["results"],
        key=lambda result: (
            result["indexed_recall_global"],
            -result["indexed_latency_global_ms"],
        ),
    )
    fallback["selection_reason"] = "highest_recall_fallback"
    _progress(
        "[vector_search_sweep] no tuned LanceDB candidate met target recall for "
        f"rows={rows} dims={dimensions}; fallback={fallback['name']} "
        f"recall={fallback['indexed_recall_global']:.4f}"
    )
    return fallback


def _summarize_scenario(
    report: dict[str, Any],
    *,
    min_indexed_recall: float,
    min_sq8_recall: float,
) -> dict[str, Any]:
    stage = report["stage_timings_ms"]
    latency = report["latency_summaries_ms"]
    recall = report["recalls_at_k"]
    tuned_lancedb = report.get("tuned_lancedb")
    sq8_enabled = "numpy_sq8_build" in stage and "numpy_sq8_global" in latency

    numpy_exact_setup = stage["sqlite_load_to_numpy"] + stage["numpy_f32_build"]
    numpy_sq8_setup = (
        stage["sqlite_load_to_numpy"] + stage["numpy_sq8_build"]
        if sq8_enabled
        else None
    )
    lancedb_flat_setup = stage["lancedb_table_create"]
    if tuned_lancedb is None:
        lancedb_indexed_setup = (
            stage["lancedb_table_create"] + stage["lancedb_index_build"]
        )
        lancedb_indexed_global = latency["lancedb_indexed_default_global"]["mean"]
        lancedb_indexed_filtered = latency["lancedb_indexed_default_filtered"]["mean"]
        indexed_recall = recall["lancedb_indexed_default_global"]
        indexed_filtered_recall = recall["lancedb_indexed_default_filtered"]
        lancedb_strategy = "default"
        lancedb_candidate_name = "default_indexed"
        lancedb_index_settings = report["lancedb_index_settings"]
        lancedb_search_settings = report["lancedb_search_settings"]
        lancedb_selection_reason = "library_default"
    else:
        lancedb_indexed_setup = (
            tuned_lancedb["table_create_ms"] + tuned_lancedb["index_build_ms"]
        )
        lancedb_indexed_global = tuned_lancedb["indexed_latency_global_ms"]
        lancedb_indexed_filtered = tuned_lancedb["indexed_latency_filtered_ms"]
        indexed_recall = tuned_lancedb["indexed_recall_global"]
        indexed_filtered_recall = tuned_lancedb["indexed_recall_filtered"]
        lancedb_strategy = "tuned"
        lancedb_candidate_name = tuned_lancedb["name"]
        lancedb_index_settings = tuned_lancedb["index_settings"]
        lancedb_search_settings = tuned_lancedb["search_settings"]
        lancedb_selection_reason = tuned_lancedb["selection_reason"]

    numpy_exact_global = latency["numpy_f32_global"]["mean"]
    numpy_sq8_global = latency["numpy_sq8_global"]["mean"] if sq8_enabled else None
    lancedb_flat_global = latency["lancedb_flat_global"]["mean"]

    indexed_break_even = _break_even_queries(
        setup_a=numpy_exact_setup,
        latency_a=numpy_exact_global,
        setup_b=lancedb_indexed_setup,
        latency_b=lancedb_indexed_global,
    )
    flat_break_even = _break_even_queries(
        setup_a=numpy_exact_setup,
        latency_a=numpy_exact_global,
        setup_b=lancedb_flat_setup,
        latency_b=lancedb_flat_global,
    )

    sq8_recall = recall["numpy_sq8_global"] if sq8_enabled else None
    recommendation = _recommendation(
        indexed_latency=lancedb_indexed_global,
        indexed_recall=indexed_recall,
        indexed_break_even=indexed_break_even,
        numpy_exact_latency=numpy_exact_global,
        sq8_latency=numpy_sq8_global,
        sq8_recall=sq8_recall,
        min_indexed_recall=min_indexed_recall,
        min_sq8_recall=min_sq8_recall,
    )

    summary = {
        "rows": report["row_count"],
        "dimensions": report["config"]["dimensions"],
        "top_k": report["config"]["top_k"],
        "queries": report["config"]["queries"],
        "filtered_candidate_count": report["filtered_candidate_count"],
        "lancedb_thread_limit": report["lancedb_thread_limit"],
        "lancedb_strategy": lancedb_strategy,
        "lancedb_candidate_name": lancedb_candidate_name,
        "lancedb_selection_reason": lancedb_selection_reason,
        "lancedb_index_settings": lancedb_index_settings,
        "lancedb_search_settings": lancedb_search_settings,
        "stage_timings_ms": stage,
        "artifact_sizes_bytes": report["artifact_sizes_bytes"],
        "latency_mean_ms": {
            "numpy_f32_global": numpy_exact_global,
            "lancedb_flat_global": lancedb_flat_global,
            "lancedb_indexed_global": lancedb_indexed_global,
            "numpy_f32_filtered": latency["numpy_f32_filtered"]["mean"],
            "lancedb_flat_filtered": latency["lancedb_flat_filtered"]["mean"],
            "lancedb_indexed_filtered": lancedb_indexed_filtered,
        },
        "recalls_at_k": {
            **recall,
            "lancedb_indexed_global": indexed_recall,
            "lancedb_indexed_filtered": indexed_filtered_recall,
        },
        "setup_totals_ms": {
            "numpy_f32": numpy_exact_setup,
            "lancedb_flat": lancedb_flat_setup,
            "lancedb_indexed": lancedb_indexed_setup,
        },
        "break_even_queries": {
            "lancedb_flat_vs_numpy_f32": flat_break_even,
            "lancedb_indexed_vs_numpy_f32": indexed_break_even,
        },
        "recommendation": recommendation,
    }
    if sq8_enabled:
        summary["latency_mean_ms"]["numpy_sq8_global"] = numpy_sq8_global
        summary["latency_mean_ms"]["numpy_sq8_filtered"] = latency[
            "numpy_sq8_filtered"
        ]["mean"]
        summary["setup_totals_ms"]["numpy_sq8"] = numpy_sq8_setup
    return summary


def _break_even_queries(
    *,
    setup_a: float,
    latency_a: float,
    setup_b: float,
    latency_b: float,
) -> int | None:
    if latency_b >= latency_a:
        return None
    latency_gain = latency_a - latency_b
    setup_penalty = max(setup_b - setup_a, 0.0)
    if setup_penalty == 0.0:
        return 0
    return math.ceil(setup_penalty / latency_gain)


def _recommendation(
    *,
    indexed_latency: float,
    indexed_recall: float,
    indexed_break_even: int | None,
    numpy_exact_latency: float,
    sq8_latency: float | None,
    sq8_recall: float | None,
    min_indexed_recall: float,
    min_sq8_recall: float,
) -> str:
    if indexed_recall < min_indexed_recall:
        if (
            sq8_recall is not None
            and sq8_latency is not None
            and sq8_recall >= min_sq8_recall
            and sq8_latency < numpy_exact_latency
        ):
            return (
                "Prefer NumPy exact as the default route. Scalar-int8 is a viable "
                "in-memory compromise here, but LanceDB indexed recall is below the "
                "current acceptance threshold."
            )
        return (
            "Prefer NumPy exact as the default route. LanceDB indexed is faster only "
            "at lower recall than the current acceptance threshold."
        )

    if indexed_latency < numpy_exact_latency:
        if indexed_break_even in {None, 0}:
            return (
                "LanceDB indexed is the practical default once you need an indexed "
                "backend; its latency win offsets setup cost immediately."
            )
        return (
            "Prefer NumPy exact for short-lived workloads, but switch to LanceDB "
            f"indexed once you expect roughly {indexed_break_even} or more global "
            "queries per loaded collection."
        )

    if (
        sq8_recall is not None
        and sq8_latency is not None
        and sq8_recall >= min_sq8_recall
        and sq8_latency < numpy_exact_latency
    ):
        return (
            "Prefer NumPy exact as the baseline. Scalar-int8 is a useful in-memory "
            "optimization here, while LanceDB indexed does not yet beat NumPy exact "
            "on latency at the required recall."
        )
    return (
        "Prefer NumPy exact as the baseline. LanceDB indexed does not yet show a "
        "latency-and-recall advantage for this scenario."
    )


def _summarize_overall(
    scenario_summaries: list[dict[str, Any]],
    *,
    min_indexed_recall: float,
) -> dict[str, Any]:
    indexed_acceptable = [
        scenario
        for scenario in scenario_summaries
        if scenario["recalls_at_k"]["lancedb_indexed_global"] >= min_indexed_recall
    ]
    indexed_faster = [
        scenario
        for scenario in indexed_acceptable
        if scenario["latency_mean_ms"]["lancedb_indexed_global"]
        < scenario["latency_mean_ms"]["numpy_f32_global"]
    ]
    numpy_faster = [
        scenario
        for scenario in scenario_summaries
        if scenario["latency_mean_ms"]["numpy_f32_global"]
        <= scenario["latency_mean_ms"]["lancedb_indexed_global"]
    ]

    tuned_count = sum(
        1 for scenario in scenario_summaries if scenario["lancedb_strategy"] == "tuned"
    )

    return {
        "scenario_count": len(scenario_summaries),
        "numpy_exact_faster_or_equal_count": len(numpy_faster),
        "indexed_acceptable_recall_count": len(indexed_acceptable),
        "indexed_faster_with_acceptable_recall_count": len(indexed_faster),
        "tuned_scenario_count": tuned_count,
        "max_rows_where_numpy_exact_faster_or_equal": (
            max(scenario["rows"] for scenario in numpy_faster) if numpy_faster else None
        ),
        "min_rows_where_indexed_is_faster_with_acceptable_recall": (
            min(scenario["rows"] for scenario in indexed_faster)
            if indexed_faster
            else None
        ),
        "notes": [
            "Treat this as a preliminary routing summary, not a frozen public policy.",
            (
                "The sweep compares against either LanceDB library defaults or a "
                "curated tuned candidate, depending on --lancedb-mode."
            ),
            (
                "Re-run the sweep on larger grids and with your expected query "
                "volume before freezing thresholds."
            ),
        ],
    }


def _print_text_report(payload: dict[str, Any]) -> None:
    grid = payload["grid"]
    overall = payload["overall"]

    print("Vector routing sweep")
    print(f"  rows grid: {grid['rows']}")
    print(f"  dimensions grid: {grid['dimensions']}")
    print(f"  top_k grid: {grid['top_k']}")
    print(f"  queries per scenario: {grid['queries']}")
    print(f"  repetitions: {grid['repetitions']}")
    print(f"  metric: {grid['metric']}")
    print(f"  lancedb mode: {grid['lancedb_mode']}")
    print()
    print("Scenario summaries")
    for scenario in payload["scenario_summaries"]:
        indexed_break_even = scenario["break_even_queries"][
            "lancedb_indexed_vs_numpy_f32"
        ]
        break_even_text = (
            str(indexed_break_even)
            if indexed_break_even is not None
            else "not reached"
        )
        summary_line = (
            "  "
            f"rows={scenario['rows']} dims={scenario['dimensions']} "
            f"top_k={scenario['top_k']} "
            f"numpy={scenario['latency_mean_ms']['numpy_f32_global']:.2f}ms "
            f"lancedb_{scenario['lancedb_strategy']}="
            f"{scenario['latency_mean_ms']['lancedb_indexed_global']:.2f}ms "
            "indexed_recall="
            f"{scenario['recalls_at_k']['lancedb_indexed_global']:.4f} "
            f"indexed_break_even_queries={break_even_text}"
        )
        if "numpy_sq8_global" in scenario["latency_mean_ms"]:
            summary_line = summary_line.replace(
                "lancedb_",
                f"sq8={scenario['latency_mean_ms']['numpy_sq8_global']:.2f}ms lancedb_",
                1,
            )
        print(summary_line)
        print(
            "    lancedb_candidate: "
            f"{scenario['lancedb_candidate_name']} "
            f"({scenario['lancedb_selection_reason']})"
        )
        print(f"    recommendation: {scenario['recommendation']}")
    print()
    print("Overall summary")
    print(f"  scenarios: {overall['scenario_count']}")
    print(
        "  numpy_exact_faster_or_equal_count: "
        f"{overall['numpy_exact_faster_or_equal_count']}"
    )
    print(
        "  indexed_faster_with_acceptable_recall_count: "
        f"{overall['indexed_faster_with_acceptable_recall_count']}"
    )
    print(f"  tuned_scenario_count: {overall['tuned_scenario_count']}")
    print(
        "  max_rows_where_numpy_exact_faster_or_equal: "
        f"{overall['max_rows_where_numpy_exact_faster_or_equal']}"
    )
    print(
        "  min_rows_where_indexed_is_faster_with_acceptable_recall: "
        f"{overall['min_rows_where_indexed_is_faster_with_acceptable_recall']}"
    )
    print()
    print("Notes")
    for note in overall["notes"]:
        print(f"  - {note}")


if __name__ == "__main__":
    main()
