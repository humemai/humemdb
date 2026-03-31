from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Literal


BenchmarkName = Literal["sql", "cypher", "vector"]


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the routing sweep runner."""

    parser = argparse.ArgumentParser(
        description=(
            "Run scale sweeps for the relational and Cypher routing benchmarks and "
            "persist merged JSON summaries."
        )
    )
    parser.add_argument(
        "--benchmark",
        choices=("all", "sql", "cypher", "vector"),
        default="all",
        help="Which benchmark family to sweep.",
    )
    parser.add_argument(
        "--sql-scales",
        default="10000,100000,1000000",
        help="Comma-separated event-row scales for the SQL benchmark.",
    )
    parser.add_argument(
        "--cypher-scales",
        default="100000,1000000",
        help="Comma-separated node-count scales for the Cypher benchmark.",
    )
    parser.add_argument(
        "--cypher-index-set",
        default="baseline",
        help="Named extra graph-index set to pass to the Cypher benchmark.",
    )
    parser.add_argument(
        "--vector-dataset",
        choices=("msmarco-10m", "stackoverflow-xlarge"),
        default="stackoverflow-xlarge",
        help="Dataset to use for the real-data vector sweep.",
    )
    parser.add_argument(
        "--vector-filter-sources",
        default="auto",
        help=(
            "Comma-separated filter families, or 'auto', for the real-data "
            "vector sweep."
        ),
    )
    parser.add_argument(
        "--vector-sample-mode",
        choices=("auto", "prefix", "stratified"),
        default="auto",
        help="Sampling strategy for the real-data vector benchmark.",
    )
    parser.add_argument(
        "--vector-scales",
        default="2000,10000,50000",
        help="Comma-separated row-count scales for the vector benchmark.",
    )
    parser.add_argument(
        "--vector-top-k-grid",
        default="10",
        help="Comma-separated top-k values to include in the vector sweep.",
    )
    parser.add_argument(
        "--vector-queries",
        type=int,
        default=100,
        help="Query count to pass through to the vector benchmark sweep.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="Warmup iterations to pass through to each benchmark script.",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=3,
        help="Timed repetitions to pass through to each benchmark script.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("scripts/benchmarks/results/routing_sweep"),
        help="Directory where per-scale JSON outputs and merged summaries are written.",
    )
    return parser.parse_args()


def _parse_scales(raw: str) -> tuple[int, ...]:
    """Parse one comma-separated integer scale list."""

    values = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("At least one scale must be provided.")
    return values


def _sql_config(rows: int) -> dict[str, int]:
    """Return SQL benchmark fixture sizes for the requested row scale."""

    if rows <= 10_000:
        return {"users": 2_000, "tags": 256, "batch_size": 2_000}
    if rows <= 100_000:
        return {"users": 10_000, "tags": 512, "batch_size": 5_000}
    if rows <= 1_000_000:
        return {"users": 50_000, "tags": 1_024, "batch_size": 20_000}
    return {"users": 200_000, "tags": 2_048, "batch_size": 50_000}


def _cypher_config(nodes: int) -> dict[str, int]:
    """Return graph benchmark fixture sizes for the requested node scale."""

    if nodes <= 100_000:
        return {"fanout": 4, "tag_fanout": 2, "batch_size": 5_000}
    return {"fanout": 4, "tag_fanout": 2, "batch_size": 20_000}


def _run_benchmark(command: list[str], *, env: dict[str, str]) -> None:
    """Run one child benchmark command and stream a short progress line."""

    print("[routing_sweep] running", " ".join(command))
    subprocess.run(command, check=True, env=env)


def _load_json(path: Path) -> dict[str, object]:
    """Load one JSON file produced by a benchmark helper."""

    return json.loads(path.read_text(encoding="utf-8"))


def _run_sql_sweep(
    *,
    scales: tuple[int, ...],
    warmup: int,
    repetitions: int,
    output_dir: Path,
    env: dict[str, str],
) -> dict[str, object]:
    """Run the SQL routing benchmark across the requested row scales."""

    benchmark_file = Path(__file__).with_name("duckdb_direct_read.py")
    runs: list[dict[str, object]] = []
    for rows in scales:
        config = _sql_config(rows)
        output_path = output_dir / f"sql_rows_{rows}.json"
        command = [
            sys.executable,
            str(benchmark_file),
            "--rows",
            str(rows),
            "--users",
            str(config["users"]),
            "--tags",
            str(config["tags"]),
            "--warmup",
            str(warmup),
            "--repetitions",
            str(repetitions),
            "--batch-size",
            str(config["batch_size"]),
            "--output-json",
            str(output_path),
        ]
        _run_benchmark(command, env=env)
        run = _load_json(output_path)
        run["scale_key"] = "rows"
        run["scale_value"] = rows
        runs.append(run)
    summary = {
        "benchmark": "sql_routing_sweep",
        "scales": list(scales),
        "warmup": warmup,
        "repetitions": repetitions,
        "runs": runs,
    }
    summary_path = output_dir / "sql_summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def _run_cypher_sweep(
    *,
    scales: tuple[int, ...],
    index_set: str,
    warmup: int,
    repetitions: int,
    output_dir: Path,
    env: dict[str, str],
) -> dict[str, object]:
    """Run the Cypher routing benchmark across the requested node scales."""

    benchmark_file = Path(__file__).with_name("cypher_graph_path.py")
    runs: list[dict[str, object]] = []
    for nodes in scales:
        config = _cypher_config(nodes)
        output_path = output_dir / f"cypher_nodes_{nodes}.json"
        command = [
            sys.executable,
            str(benchmark_file),
            "--nodes",
            str(nodes),
            "--fanout",
            str(config["fanout"]),
            "--tag-fanout",
            str(config["tag_fanout"]),
            "--index-set",
            index_set,
            "--warmup",
            str(warmup),
            "--repetitions",
            str(repetitions),
            "--batch-size",
            str(config["batch_size"]),
            "--output-json",
            str(output_path),
        ]
        _run_benchmark(command, env=env)
        run = _load_json(output_path)
        run["scale_key"] = "nodes"
        run["scale_value"] = nodes
        runs.append(run)
    summary = {
        "benchmark": "cypher_routing_sweep",
        "index_set": index_set,
        "scales": list(scales),
        "warmup": warmup,
        "repetitions": repetitions,
        "runs": runs,
    }
    summary_path = output_dir / "cypher_summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def _run_vector_sweep(
    *,
    scales: tuple[int, ...],
    top_k_grid: tuple[int, ...],
    queries: int,
    warmup: int,
    repetitions: int,
    dataset: str,
    filter_sources: str,
    sample_mode: str,
    output_dir: Path,
    env: dict[str, str],
) -> dict[str, object]:
    """Run the real-data vector sweep and persist its merged summary."""

    benchmark_file = Path(__file__).with_name("vector_search_real_sweep.py")
    intermediate_dir = output_dir / "vector_intermediate"
    summary_path = output_dir / "vector_summary.json"
    command = [
        sys.executable,
        str(benchmark_file),
        "--dataset",
        dataset,
        "--rows-grid",
        ",".join(str(scale) for scale in scales),
        "--top-k-grid",
        ",".join(str(value) for value in top_k_grid),
        "--queries",
        str(queries),
        "--warmup",
        str(warmup),
        "--repetitions",
        str(repetitions),
        "--sample-mode",
        sample_mode,
        "--filter-sources",
        filter_sources,
        "--output-json",
        str(summary_path),
        "--intermediate-dir",
        str(intermediate_dir),
        "--output",
        "json",
    ]
    print("[routing_sweep] running", " ".join(command))
    completed = subprocess.run(
        command,
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        text=True,
    )
    summary = json.loads(completed.stdout)
    summary["benchmark"] = "vector_real_routing_sweep"
    summary["dataset"] = dataset
    summary_path = output_dir / "vector_summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def main() -> None:
    """Execute the requested routing sweep families and write merged output."""

    args = _parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()

    sql_scales = _parse_scales(args.sql_scales)
    cypher_scales = _parse_scales(args.cypher_scales)
    vector_scales = _parse_scales(args.vector_scales)
    vector_top_k_grid = _parse_scales(args.vector_top_k_grid)
    merged: dict[str, object] = {
        "benchmark": "routing_sweep",
        "thread_limit": env.get("HUMEMDB_THREADS", "default"),
    }

    if args.benchmark in {"all", "sql"}:
        merged["sql"] = _run_sql_sweep(
            scales=sql_scales,
            warmup=args.warmup,
            repetitions=args.repetitions,
            output_dir=output_dir,
            env=env,
        )
    if args.benchmark in {"all", "cypher"}:
        merged["cypher"] = _run_cypher_sweep(
            scales=cypher_scales,
            index_set=args.cypher_index_set,
            warmup=args.warmup,
            repetitions=args.repetitions,
            output_dir=output_dir,
            env=env,
        )
    if args.benchmark in {"all", "vector"}:
        merged["vector"] = _run_vector_sweep(
            scales=vector_scales,
            top_k_grid=vector_top_k_grid,
            queries=args.vector_queries,
            warmup=args.warmup,
            repetitions=args.repetitions,
            dataset=args.vector_dataset,
            filter_sources=args.vector_filter_sources,
            sample_mode=args.vector_sample_mode,
            output_dir=output_dir,
            env=env,
        )

    merged_path = output_dir / "routing_sweep_summary.json"
    merged_path.write_text(
        json.dumps(merged, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"[routing_sweep] wrote merged summary to {merged_path}")


if __name__ == "__main__":
    main()
