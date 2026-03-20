"""HumemDB Python package."""

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version

from .runtime import configure_runtime_threads_from_env
from .runtime import RuntimeThreadBudget

configure_runtime_threads_from_env()

HumemDB = import_module("humemdb.db").HumemDB
translate_sql = import_module("humemdb.sql").translate_sql
QueryResult = import_module("humemdb.types").QueryResult

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
