# Benchmark Scripts

This directory contains benchmarking utilities for comparing query paths, storage
strategies, and execution backends.

Current scripts:

- `duckdb_direct_read.py`: compares SQLite and DuckDB read performance when
  DuckDB reads from the SQLite source of truth.
