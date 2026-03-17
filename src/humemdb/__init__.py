"""HumemDB Python package."""

from importlib.metadata import PackageNotFoundError, version

from .db import HumemDB
from .sql import translate_sql
from .types import QueryResult

try:
    __version__ = version("humemdb")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = ["HumemDB", "QueryResult", "translate_sql", "__version__"]
