"""HumemDB-wide runtime thread-budget helpers.

The top-level `HUMEMDB_THREADS` setting is intended to cap worker usage across the
embedded runtime instead of only a single backend. Today that means:

- DuckDB reads the setting directly for its own execution threads
- NumPy/BLAS/OpenMP libraries receive matching thread-limit env vars
- `threadpoolctl` applies a best-effort runtime cap for already loaded numeric pools
- Arrow-backed paths such as local LanceDB execution use `pyarrow.set_cpu_count()`

`LANCEDB_THREADS` remains available as a vector-only fallback for benchmark scripts, but
product runtime code should treat `HUMEMDB_THREADS` as the canonical knob.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Sequence

HUMEMDB_THREADS_ENV = "HUMEMDB_THREADS"
LANCEDB_THREADS_ENV = "LANCEDB_THREADS"

_NUMERIC_THREAD_ENV_VARS = (
    "OMP_THREAD_LIMIT",
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "BLIS_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)

_GENERAL_THREAD_ENV_VARS = (
    "RAYON_NUM_THREADS",
    "TOKIO_WORKER_THREADS",
    "POLARS_MAX_THREADS",
    "ARROW_NUM_THREADS",
)

_THREADPOOL_STATE: dict[str, Any] = {
    "limiter": None,
    "limit": None,
}


@dataclass(frozen=True, slots=True)
class RuntimeThreadBudget:
    """Resolved HumemDB runtime thread-budget details."""

    source_env: str | None
    thread_count: int | None
    arrow_cpu_count: int | None
    arrow_io_thread_count: int | None
    numpy_thread_limit: int | None


def configure_runtime_threads_from_env(
    *,
    fallback_env_names: Sequence[str] = (),
) -> RuntimeThreadBudget:
    """Apply the runtime thread budget from HumemDB environment variables."""

    source_env, thread_count = resolve_thread_budget_from_env(
        fallback_env_names=fallback_env_names,
    )
    return configure_runtime_threads(
        thread_count=thread_count,
        source_env=source_env,
    )


def resolve_thread_budget_from_env(
    *,
    fallback_env_names: Sequence[str] = (),
) -> tuple[str | None, int | None]:
    """Resolve the first configured HumemDB thread-budget env var."""

    for env_name in (HUMEMDB_THREADS_ENV, *fallback_env_names):
        raw_value = os.environ.get(env_name)
        if raw_value is None:
            continue

        try:
            thread_count = int(raw_value)
        except ValueError as exc:
            raise ValueError(
                f"{env_name} must be an integer; got {raw_value!r}."
            ) from exc

        if thread_count < 1:
            raise ValueError(f"{env_name} must be >= 1; got {thread_count}.")

        return env_name, thread_count

    return None, None


def configure_runtime_threads(
    *,
    thread_count: int | None,
    source_env: str | None = None,
) -> RuntimeThreadBudget:
    """Apply a HumemDB runtime thread budget to supported libraries."""

    if thread_count is None:
        return RuntimeThreadBudget(
            source_env=source_env,
            thread_count=None,
            arrow_cpu_count=_current_arrow_cpu_count(),
            arrow_io_thread_count=_current_arrow_io_thread_count(),
            numpy_thread_limit=_THREADPOOL_STATE["limit"],
        )

    _apply_numeric_thread_env(thread_count)
    _apply_general_thread_env(thread_count)
    arrow_cpu_count, arrow_io_thread_count = _apply_arrow_thread_budget(thread_count)
    numpy_thread_limit = _apply_numpy_thread_budget(thread_count)
    return RuntimeThreadBudget(
        source_env=source_env,
        thread_count=thread_count,
        arrow_cpu_count=arrow_cpu_count,
        arrow_io_thread_count=arrow_io_thread_count,
        numpy_thread_limit=numpy_thread_limit,
    )


def _apply_numeric_thread_env(thread_count: int) -> None:
    value = str(thread_count)
    for env_name in _NUMERIC_THREAD_ENV_VARS:
        os.environ[env_name] = value


def _apply_general_thread_env(thread_count: int) -> None:
    value = str(thread_count)
    for env_name in _GENERAL_THREAD_ENV_VARS:
        os.environ[env_name] = value


def _apply_arrow_thread_budget(thread_count: int) -> tuple[int | None, int | None]:
    try:
        import pyarrow as pa
    except ImportError:
        return None, None

    pa.set_cpu_count(thread_count)
    try:
        pa.set_io_thread_count(thread_count)
    except AttributeError:
        pass

    return int(pa.cpu_count()), _current_arrow_io_thread_count()


def _current_arrow_cpu_count() -> int | None:
    try:
        import pyarrow as pa
    except ImportError:
        return None

    return int(pa.cpu_count())


def _current_arrow_io_thread_count() -> int | None:
    try:
        import pyarrow as pa
    except ImportError:
        return None

    get_io_thread_count = getattr(pa, "io_thread_count", None)
    if get_io_thread_count is None:
        return None

    return int(get_io_thread_count())


def _apply_numpy_thread_budget(thread_count: int) -> int | None:
    current_limit = _THREADPOOL_STATE["limit"]
    current_limiter = _THREADPOOL_STATE["limiter"]

    if current_limit == thread_count:
        return thread_count

    if current_limiter is not None:
        current_limiter.restore_original_limits()
        current_limiter.unregister()
        _THREADPOOL_STATE["limiter"] = None
        _THREADPOOL_STATE["limit"] = None

    try:
        from threadpoolctl import threadpool_limits
    except ImportError:
        return None

    _THREADPOOL_STATE["limiter"] = threadpool_limits(limits=thread_count)
    _THREADPOOL_STATE["limit"] = thread_count
    return thread_count
