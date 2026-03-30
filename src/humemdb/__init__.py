"""Public package surface for HumemDB.

This module re-exports the small stable API that most callers should import from:

- `HumemDB` as the main embedded database entry point
- `translate_sql(...)` for backend-aware SQL translation
- `QueryResult` for normalized query results
- runtime thread-budget helpers and the installed package version

Importing `humemdb` also applies any configured runtime thread limits through
`configure_runtime_threads_from_env()` so SQLite, DuckDB, Arrow, and numeric
libraries can share one process-wide thread budget.
"""

from importlib.metadata import PackageNotFoundError, version

from .db import HumemDB
from .runtime import configure_runtime_threads_from_env
from .runtime import RuntimeThreadBudget
from .sql import translate_sql
from .types import QueryResult

configure_runtime_threads_from_env()

try:
    __version__ = version("humemdb")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "HumemDB",
    "QueryResult",
    "RuntimeThreadBudget",
    "configure_runtime_threads_from_env",
    "translate_sql",
    "__version__",
]
