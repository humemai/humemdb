#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
PYTHON_BIN=${PYTHON_BIN:-"$REPO_ROOT/.venv/bin/python"}
THREADS=${HUMEMDB_THREADS:-8}
QUERIES=${QUERIES:-100}
WARMUP=${WARMUP:-1}
REPETITIONS=${REPETITIONS:-3}
TOP_K_GRID=${TOP_K_GRID:-10,50}

DATASET_FILTER=${1:-all}
ROWS_FILTER=${2:-all}

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python interpreter not found or not executable: $PYTHON_BIN" >&2
  exit 1
fi

run_case() {
  local dataset=$1
  local rows=$2
  local sample_mode=$3
  local partitions=$4
  local sub_vectors=$5
  local nprobes=$6
  local refine_factor=$7
  local out_file=$8

  mkdir -p "$(dirname -- "$out_file")"

  echo
  echo "==> dataset=$dataset rows=$rows top_k_grid=$TOP_K_GRID partitions=$partitions sub_vectors=$sub_vectors nprobes=$nprobes refine_factor=$refine_factor"
  echo "    output=$out_file"

  HUMEMDB_THREADS=$THREADS "$PYTHON_BIN" "$SCRIPT_DIR/vector_search_real.py" \
    --dataset "$dataset" \
    --rows "$rows" \
    --queries "$QUERIES" \
    --top-k-grid "$TOP_K_GRID" \
    --warmup "$WARMUP" \
    --repetitions "$REPETITIONS" \
    --sample-mode "$sample_mode" \
    --lancedb-index-type IVF_PQ \
    --lancedb-num-partitions "$partitions" \
    --lancedb-num-sub-vectors "$sub_vectors" \
    --lancedb-nprobes "$nprobes" \
    --lancedb-refine-factor "$refine_factor" \
    --output json > "$out_file"
}

should_run_dataset() {
  local dataset=$1
  [[ "$DATASET_FILTER" == "all" || "$DATASET_FILTER" == "$dataset" ]]
}

should_run_rows() {
  local rows=$1
  [[ "$ROWS_FILTER" == "all" || "$ROWS_FILTER" == "$rows" ]]
}

should_run_case() {
  local dataset=$1
  local rows=$2
  should_run_dataset "$dataset" && should_run_rows "$rows"
}

if should_run_case "msmarco-10m" 1000000; then
  run_case \
    "msmarco-10m" \
    1000000 \
    auto \
    128 \
    128 \
    32 \
    4 \
    "$SCRIPT_DIR/results/real_ivf_pq_tuning/msmarco-10m/msmarco-10m_rows1000000_topk10_50_p128_sv128_np32_rf4.json"
fi

if should_run_case "stackoverflow-xlarge" 1000000; then
  run_case \
    "stackoverflow-xlarge" \
    1000000 \
    stratified \
    64 \
    128 \
    32 \
    4 \
    "$SCRIPT_DIR/results/real_ivf_pq_tuning/stackoverflow-xlarge/stackoverflow-xlarge_rows1000000_topk10_50_p64_sv128_np32_rf4.json"
fi

if should_run_case "msmarco-10m" 10000000; then
  run_case \
    "msmarco-10m" \
    10000000 \
    auto \
    128 \
    128 \
    32 \
    4 \
    "$SCRIPT_DIR/results/real_ivf_pq_tuning/msmarco-10m/msmarco-10m_rows10000000_topk10_50_p128_sv128_np32_rf4.json"
fi

if should_run_case "stackoverflow-xlarge" 10000000; then
  run_case \
    "stackoverflow-xlarge" \
    10000000 \
    stratified \
    64 \
    128 \
    32 \
    4 \
    "$SCRIPT_DIR/results/real_ivf_pq_tuning/stackoverflow-xlarge/stackoverflow-xlarge_rows10000000_topk10_50_p64_sv128_np32_rf4.json"
fi

if should_run_case "stackoverflow-xlarge" 25000000; then
  run_case \
    "stackoverflow-xlarge" \
    25000000 \
    stratified \
    64 \
    128 \
    32 \
    4 \
    "$SCRIPT_DIR/results/real_ivf_pq_tuning/stackoverflow-xlarge/stackoverflow-xlarge_rows25000000_topk10_50_p64_sv128_np32_rf4.json"
fi

echo
echo "Real-vector baseline runs completed."