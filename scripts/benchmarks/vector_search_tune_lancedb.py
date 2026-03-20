from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def _progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Search a curated LanceDB tuning set and pick the lowest-latency profile "
            "that meets a target recall."
        )
    )
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--dimensions", type=int, default=384)
    parser.add_argument("--queries", type=int, default=16)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--metric", choices=("cosine", "dot", "l2"), default="cosine")
    parser.add_argument("--buckets", type=int, default=128)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--target-recall", type=float, default=0.95)
    parser.add_argument(
        "--skip-numpy-sq8",
        action="store_true",
        help="Skip the scalar-int8 NumPy approximation path in candidate runs.",
    )
    parser.add_argument(
        "--candidate-family",
        choices=("all", "ivf_pq", "ivf_flat", "ivf_hnsw_sq"),
        default="all",
        help="Restrict the curated tuning search to one LanceDB index family.",
    )
    parser.add_argument("--output", choices=("text", "json"), default="text")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    candidates = _candidate_profiles(
        rows=args.rows,
        dimensions=args.dimensions,
        candidate_family=args.candidate_family,
    )
    _progress(
        "[vector_search_tune_lancedb] starting "
        f"{len(candidates)} candidates for rows={args.rows} "
        f"dims={args.dimensions} top_k={args.top_k} "
        f"family={args.candidate_family}"
    )
    results = []
    for index, candidate in enumerate(candidates):
        _progress(
            "[vector_search_tune_lancedb] candidate "
            f"{index + 1}/{len(candidates)}: {candidate['name']}"
        )
        result = _run_candidate(candidate, args=args, seed_offset=index)
        results.append(result)
        _progress(
            "[vector_search_tune_lancedb] finished "
            f"{candidate['name']}: latency="
            f"{result['indexed_latency_global_ms']:.2f}ms "
            f"recall={result['indexed_recall_global']:.4f}"
        )
    acceptable = [
        result
        for result in results
        if result["indexed_recall_global"] >= args.target_recall
    ]
    best = min(
        acceptable,
        key=lambda result: result["indexed_latency_global_ms"],
    ) if acceptable else None
    payload = {
        "candidate_family": args.candidate_family,
        "target_recall": args.target_recall,
        "rows": args.rows,
        "dimensions": args.dimensions,
        "top_k": args.top_k,
        "queries": args.queries,
        "results": results,
        "best": best,
    }
    if args.output == "json":
        if best is None:
            _progress("[vector_search_tune_lancedb] no candidate met target recall")
        else:
            _progress(
                "[vector_search_tune_lancedb] best acceptable candidate: "
                f"{best['name']} with recall={best['indexed_recall_global']:.4f}"
            )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    _print_text_report(payload)


def _candidate_profiles(
    *,
    rows: int,
    dimensions: int,
    candidate_family: str,
) -> list[dict[str, Any]]:
    moderate_partitions = max(8, min(128, int(rows**0.5)))
    aggressive_partitions = max(8, min(256, int(rows**0.5) * 2))
    exhaustive_partitions = max(8, min(512, int(rows**0.5) * 4))
    preferred_sub_vectors = _preferred_num_sub_vectors(dimensions)
    candidates = [
        {
            "name": "default_ivf_pq",
            "index_type": "IVF_PQ",
        },
        {
            "name": "ivf_pq_probe128_refine8",
            "index_type": "IVF_PQ",
            "num_partitions": aggressive_partitions,
            "num_sub_vectors": preferred_sub_vectors,
            "nprobes": 128,
            "refine_factor": 8,
        },
        {
            "name": "ivf_pq_probe256_refine16",
            "index_type": "IVF_PQ",
            "num_partitions": aggressive_partitions,
            "num_sub_vectors": preferred_sub_vectors,
            "nprobes": 256,
            "refine_factor": 16,
        },
        {
            "name": "ivf_pq_probe512_refine16",
            "index_type": "IVF_PQ",
            "num_partitions": exhaustive_partitions,
            "num_sub_vectors": preferred_sub_vectors,
            "nprobes": 512,
            "refine_factor": 16,
        },
        {
            "name": "ivf_pq_probe512_refine32",
            "index_type": "IVF_PQ",
            "num_partitions": exhaustive_partitions,
            "num_sub_vectors": preferred_sub_vectors,
            "nprobes": 512,
            "refine_factor": 32,
        },
        {
            "name": "ivf_flat_probe64",
            "index_type": "IVF_FLAT",
            "num_partitions": moderate_partitions,
            "nprobes": 64,
        },
        {
            "name": "ivf_flat_probe128",
            "index_type": "IVF_FLAT",
            "num_partitions": aggressive_partitions,
            "nprobes": 128,
        },
        {
            "name": "ivf_flat_probe256",
            "index_type": "IVF_FLAT",
            "num_partitions": aggressive_partitions,
            "nprobes": 256,
        },
        {
            "name": "ivf_flat_probe512",
            "index_type": "IVF_FLAT",
            "num_partitions": exhaustive_partitions,
            "nprobes": 512,
        },
        {
            "name": "ivf_hnsw_sq_ef160",
            "index_type": "IVF_HNSW_SQ",
            "num_partitions": 1,
            "m": 32,
            "ef_construction": 300,
            "ef": 160,
        },
        {
            "name": "ivf_hnsw_sq_ef320",
            "index_type": "IVF_HNSW_SQ",
            "num_partitions": 1,
            "m": 32,
            "ef_construction": 300,
            "ef": 320,
        },
        {
            "name": "ivf_hnsw_sq_m48_ef320",
            "index_type": "IVF_HNSW_SQ",
            "num_partitions": 1,
            "m": 48,
            "ef_construction": 400,
            "ef": 320,
        },
        {
            "name": "ivf_hnsw_sq_m64_ef400",
            "index_type": "IVF_HNSW_SQ",
            "num_partitions": 1,
            "m": 64,
            "ef_construction": 500,
            "ef": 400,
        },
    ]
    if candidate_family == "all":
        return candidates

    family_index_type = {
        "ivf_pq": "IVF_PQ",
        "ivf_flat": "IVF_FLAT",
        "ivf_hnsw_sq": "IVF_HNSW_SQ",
    }[candidate_family]
    return [
        candidate
        for candidate in candidates
        if candidate["index_type"] == family_index_type
    ]


def _preferred_num_sub_vectors(dimensions: int) -> int:
    if dimensions % 16 == 0:
        return max(1, dimensions // 16)
    if dimensions % 8 == 0:
        return max(1, dimensions // 8)
    return 1


def _run_candidate(
    candidate: dict[str, Any],
    *,
    args: argparse.Namespace,
    seed_offset: int,
) -> dict[str, Any]:
    script_path = Path(__file__).with_name("vector_search.py")
    command = [
        sys.executable,
        str(script_path),
        "--rows",
        str(args.rows),
        "--dimensions",
        str(args.dimensions),
        "--queries",
        str(args.queries),
        "--top-k",
        str(args.top_k),
        "--warmup",
        str(args.warmup),
        "--repetitions",
        str(args.repetitions),
        "--metric",
        args.metric,
        "--buckets",
        str(args.buckets),
        "--seed",
        str(args.seed + seed_offset),
        "--lancedb-index-type",
        candidate["index_type"],
        "--output",
        "json",
    ]
    if args.skip_numpy_sq8:
        command.append("--skip-numpy-sq8")
    if "num_partitions" in candidate:
        command.extend(["--lancedb-num-partitions", str(candidate["num_partitions"])])
    if "num_sub_vectors" in candidate:
        command.extend(["--lancedb-num-sub-vectors", str(candidate["num_sub_vectors"])])
    if "nprobes" in candidate:
        command.extend(["--lancedb-nprobes", str(candidate["nprobes"])])
    if "refine_factor" in candidate:
        command.extend(["--lancedb-refine-factor", str(candidate["refine_factor"])])
    if "ef" in candidate:
        command.extend(["--lancedb-ef", str(candidate["ef"])])
    if "m" in candidate:
        command.extend(["--lancedb-m", str(candidate["m"])])
    if "ef_construction" in candidate:
        command.extend([
            "--lancedb-ef-construction",
            str(candidate["ef_construction"]),
        ])
    completed = subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    report = json.loads(completed.stdout)
    return {
        "name": candidate["name"],
        "index_settings": report["lancedb_index_settings"],
        "search_settings": report["lancedb_search_settings"],
        "indexed_latency_global_ms": report["latency_summaries_ms"][
            "lancedb_indexed_default_global"
        ]["mean"],
        "indexed_latency_filtered_ms": report["latency_summaries_ms"][
            "lancedb_indexed_default_filtered"
        ]["mean"],
        "indexed_recall_global": report["recalls_at_k"][
            "lancedb_indexed_default_global"
        ],
        "indexed_recall_filtered": report["recalls_at_k"][
            "lancedb_indexed_default_filtered"
        ],
        "index_build_ms": report["stage_timings_ms"]["lancedb_index_build"],
        "table_create_ms": report["stage_timings_ms"]["lancedb_table_create"],
        "numpy_exact_latency_global_ms": report["latency_summaries_ms"][
            "numpy_f32_global"
        ]["mean"],
    }


def _print_text_report(payload: dict[str, Any]) -> None:
    print("LanceDB tuning search")
    print(f"  target_recall: {payload['target_recall']:.4f}")
    print(f"  rows: {payload['rows']}")
    print(f"  dimensions: {payload['dimensions']}")
    print(f"  top_k: {payload['top_k']}")
    print(f"  queries: {payload['queries']}")
    print()
    print("Candidates")
    for result in payload["results"]:
        summary_line = (
            f"  {result['name']}: "
            f"indexed_latency={result['indexed_latency_global_ms']:.2f}ms "
            f"indexed_recall={result['indexed_recall_global']:.4f} "
            f"index_build={result['index_build_ms']:.2f}ms"
        )
        print(summary_line)
    print()
    if payload["best"] is None:
        print("No candidate met the target recall.")
        return
    print("Best acceptable candidate")
    print(f"  name: {payload['best']['name']}")
    print(
        "  indexed_latency_global_ms: "
        f"{payload['best']['indexed_latency_global_ms']:.2f}"
    )
    print(f"  indexed_recall_global: {payload['best']['indexed_recall_global']:.4f}")
    print(f"  index_settings: {payload['best']['index_settings']}")
    print(f"  search_settings: {payload['best']['search_settings']}")


if __name__ == "__main__":
    main()
