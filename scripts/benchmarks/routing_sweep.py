from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Literal


BenchmarkName = Literal["sql", "cypher"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run scale sweeps for the relational and Cypher routing benchmarks and "
            "persist merged JSON summaries."
        )
    )
    parser.add_argument(
        "--benchmark",
        choices=("all", "sql", "cypher"),
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
    values = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise ValueError("At least one scale must be provided.")
    return values


def _sql_config(rows: int) -> dict[str, int]:
    if rows <= 10_000:
        return {"users": 2_000, "tags": 256, "batch_size": 2_000}
    if rows <= 100_000:
        return {"users": 10_000, "tags": 512, "batch_size": 5_000}
    if rows <= 1_000_000:
        return {"users": 50_000, "tags": 1_024, "batch_size": 20_000}
    return {"users": 200_000, "tags": 2_048, "batch_size": 50_000}


def _cypher_config(nodes: int) -> dict[str, int]:
    if nodes <= 100_000:
        return {"fanout": 4, "tag_fanout": 2, "batch_size": 5_000}
    return {"fanout": 4, "tag_fanout": 2, "batch_size": 20_000}


def _run_benchmark(command: list[str], *, env: dict[str, str]) -> None:
    print("[routing_sweep] running", " ".join(command))
    subprocess.run(command, check=True, env=env)


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _run_sql_sweep(
    *,
    scales: tuple[int, ...],
    warmup: int,
    repetitions: int,
    output_dir: Path,
    env: dict[str, str],
) -> dict[str, object]:
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
    warmup: int,
    repetitions: int,
    output_dir: Path,
    env: dict[str, str],
) -> dict[str, object]:
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


def main() -> None:
    args = _parse_args()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()

    sql_scales = _parse_scales(args.sql_scales)
    cypher_scales = _parse_scales(args.cypher_scales)
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
            warmup=args.warmup,
            repetitions=args.repetitions,
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
