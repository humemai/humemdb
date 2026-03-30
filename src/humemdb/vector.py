"""Exact vector-search helpers for the first HumemVector v0 slice.

This module intentionally starts with the simplest baseline that HumemDB needs to reason
about vector routing:

- canonical vectors can be stored in SQLite as float32 blobs
- exact in-memory search can run over NumPy arrays
- float16 and scalar-int8 variants can be benchmarked against the float32 baseline

These helpers back the first public `HumemVector v0` surface: SQLite is the canonical
store and exact NumPy search is the default execution path. The approximate and
accelerated variants remain benchmark tools until routing policy broadens.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence, TypeAlias, cast

import numpy as np

from .engines import _SQLiteEngine

VectorMetric: TypeAlias = Literal["cosine", "dot", "l2"]
_VectorMetadataValue: TypeAlias = str | int | float | bool | None
_VectorNamespaceKey: TypeAlias = tuple[str, str, int]

_GRAPH_NODE_VECTOR_DELETE_TRIGGER_SQL = (
    "CREATE TRIGGER IF NOT EXISTS trg_graph_nodes_delete_graph_vectors "
    "AFTER DELETE ON graph_nodes BEGIN "
    "DELETE FROM vector_entries "
    "WHERE target = 'graph_node' AND namespace = '' AND target_id = OLD.id; "
    "END"
)


@dataclass(frozen=True, slots=True)
class _VectorSearchMatch:
    """One nearest-neighbor result from an exact or quantized vector search.

    Attributes:
        target: Logical vector namespace such as `direct`, `sql_row`, or
            `graph_node`.
        namespace: Optional namespace within the target, such as a SQL table name.
        target_id: Logical identifier inside the target and namespace.
        score: Similarity score used for ranking.
    """

    target: str
    namespace: str
    target_id: int
    score: float


@dataclass(frozen=True, slots=True)
class _ColdVectorSnapshotMetadata:
    """Persisted description of one cold ANN snapshot table."""

    metric: VectorMetric
    table_name: str
    row_count: int
    generation: int


@dataclass(frozen=True, slots=True)
class _NamedVectorIndex:
    """Persisted public lifecycle row for one named vector index."""

    name: str
    metric: VectorMetric
    enabled: bool
    maintenance_paused: bool


def _default_public_vector_index_name(metric: VectorMetric) -> str:
    """Return the default public name for one metric-backed vector index."""

    return f"humemdb_vector_{metric}"


def _ensure_vector_schema(sqlite: _SQLiteEngine) -> None:
    """Create the initial SQLite-backed vector storage tables if needed.

    Args:
        sqlite: Canonical SQLite engine that owns vector storage.
    """

    for statement in (
        (
            "CREATE TABLE IF NOT EXISTS vector_entries ("
            "vector_id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "target TEXT NOT NULL, "
            "namespace TEXT NOT NULL DEFAULT '', "
            "target_id INTEGER NOT NULL, "
            "dimensions INTEGER NOT NULL, "
            "embedding BLOB NOT NULL, "
            "UNIQUE(target, namespace, target_id))"
        ),
        (
            "CREATE TABLE IF NOT EXISTS vector_entry_metadata ("
            "vector_id INTEGER NOT NULL, "
            "key TEXT NOT NULL, "
            "value TEXT, "
            "value_type TEXT NOT NULL, "
            "PRIMARY KEY (vector_id, key))"
        ),
        (
            "CREATE INDEX IF NOT EXISTS idx_vector_entries_target_lookup "
            "ON vector_entries(target, namespace, target_id)"
        ),
        (
            "CREATE INDEX IF NOT EXISTS idx_vector_entry_metadata_lookup "
            "ON vector_entry_metadata(key, value_type, value, vector_id)"
        ),
        (
            "CREATE TABLE IF NOT EXISTS vector_cold_snapshots ("
            "metric TEXT PRIMARY KEY, "
            "table_name TEXT NOT NULL, "
            "row_count INTEGER NOT NULL, "
            "generation INTEGER NOT NULL)"
        ),
        (
            "CREATE TABLE IF NOT EXISTS vector_cold_tombstones ("
            "metric TEXT NOT NULL, "
            "target TEXT NOT NULL, "
            "namespace TEXT NOT NULL DEFAULT '', "
            "target_id INTEGER NOT NULL, "
            "PRIMARY KEY(metric, target, namespace, target_id))"
        ),
        (
            "CREATE TABLE IF NOT EXISTS vector_index_policies ("
            "metric TEXT PRIMARY KEY, "
            "enabled INTEGER NOT NULL, "
            "maintenance_paused INTEGER NOT NULL DEFAULT 0)"
        ),
        (
            "CREATE TABLE IF NOT EXISTS vector_named_indexes ("
            "name TEXT PRIMARY KEY, "
            "metric TEXT NOT NULL UNIQUE, "
            "enabled INTEGER NOT NULL, "
            "maintenance_paused INTEGER NOT NULL DEFAULT 0)"
        ),
        (
            "CREATE TRIGGER IF NOT EXISTS trg_vector_entries_delete_metadata "
            "AFTER DELETE ON vector_entries BEGIN "
            "DELETE FROM vector_entry_metadata WHERE vector_id = OLD.vector_id; "
            "END"
        ),
        (
            "CREATE TRIGGER IF NOT EXISTS trg_vector_entries_record_tombstone "
            "BEFORE DELETE ON vector_entries BEGIN "
            "INSERT OR IGNORE INTO vector_cold_tombstones "
            "(metric, target, namespace, target_id) "
            "SELECT metric, OLD.target, OLD.namespace, OLD.target_id "
            "FROM vector_cold_snapshots "
            "; "
            "END"
        ),
    ):
        sqlite.execute(statement, query_type="vector")

    if not _table_column_exists(sqlite, "vector_index_policies", "maintenance_paused"):
        sqlite.execute(
            (
                "ALTER TABLE vector_index_policies "
                "ADD COLUMN maintenance_paused INTEGER NOT NULL DEFAULT 0"
            ),
            query_type="vector",
        )

    if _table_exists(sqlite, "vector_index_policies"):
        legacy_rows = sqlite.execute(
            (
                "SELECT metric, enabled, maintenance_paused "
                "FROM vector_index_policies ORDER BY metric"
            ),
            query_type="vector",
        ).rows
        for metric, enabled, maintenance_paused in legacy_rows:
            normalized_metric = cast(VectorMetric, str(metric))
            if _load_named_vector_index_for_metric(
                sqlite,
                metric=normalized_metric,
            ) is not None:
                continue
            _upsert_named_vector_index(
                sqlite,
                name=_default_public_vector_index_name(normalized_metric),
                metric=normalized_metric,
                enabled=bool(int(enabled)),
                maintenance_paused=bool(int(maintenance_paused)),
            )

    for metadata in _list_cold_vector_snapshot_metadata(sqlite):
        if (
            _load_named_vector_index_for_metric(sqlite, metric=metadata.metric)
            is not None
        ):
            continue
        _upsert_named_vector_index(
            sqlite,
            name=_default_public_vector_index_name(metadata.metric),
            metric=metadata.metric,
            enabled=True,
            maintenance_paused=False,
        )

    if _table_exists(sqlite, "graph_nodes"):
        sqlite.execute(_GRAPH_NODE_VECTOR_DELETE_TRIGGER_SQL, query_type="vector")


def _table_exists(sqlite: _SQLiteEngine, table_name: str) -> bool:
    """Return whether one SQLite table already exists."""

    return (
        sqlite.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
            query_type="vector",
        ).first()
        is not None
    )


def _table_column_exists(
    sqlite: _SQLiteEngine,
    table_name: str,
    column_name: str,
) -> bool:
    """Return whether one SQLite table already exposes the named column."""

    result = sqlite.execute(
        f"PRAGMA table_info({table_name})",
        query_type="vector",
    )
    return any(str(row[1]) == column_name for row in result.rows)


def _insert_vectors(
    sqlite: _SQLiteEngine,
    rows: Sequence[tuple[int, Sequence[float]]],
    *,
    target: str = "direct",
    namespace: str = "",
) -> None:
    """Insert vector rows into the SQLite canonical store.

    Args:
        sqlite: Canonical SQLite engine that owns vector storage.
        rows: `(target_id, embedding)` rows to insert.
        target: Logical target namespace for the rows.
        namespace: Optional namespace within the target.
    """

    if not rows:
        return

    _write_vector_rows(
        sqlite,
        rows,
        target=target,
        namespace=namespace,
        conflict_mode="insert",
    )
    _clear_vector_tombstones(
        sqlite,
        tuple((target, namespace, int(target_id)) for target_id, _ in rows),
    )


def _upsert_vectors(
    sqlite: _SQLiteEngine,
    rows: Sequence[tuple[int, Sequence[float]]],
    *,
    target: str = "direct",
    namespace: str = "",
) -> None:
    """Insert or replace vector rows in the SQLite canonical store.

    Args:
        sqlite: Canonical SQLite engine that owns vector storage.
        rows: `(target_id, embedding)` rows to insert or replace.
        target: Logical target namespace for the rows.
        namespace: Optional namespace within the target.
    """

    if not rows:
        return

    _write_vector_rows(
        sqlite,
        rows,
        target=target,
        namespace=namespace,
        conflict_mode="upsert",
    )
    _clear_vector_tombstones(
        sqlite,
        tuple((target, namespace, int(target_id)) for target_id, _ in rows),
    )


def _delete_target_namespaced_vectors(
    sqlite: _SQLiteEngine,
    item_ids: Sequence[_VectorNamespaceKey],
) -> int:
    """Delete canonical vector rows for the given logical ids."""

    normalized_item_ids = tuple(
        (str(target), str(namespace), int(target_id))
        for target, namespace, target_id in item_ids
    )
    if not normalized_item_ids:
        return 0

    existing_item_ids = _existing_vector_item_ids(sqlite, normalized_item_ids)
    if not existing_item_ids:
        return 0

    sqlite.executemany(
        (
            "DELETE FROM vector_entries "
            "WHERE target = ? AND namespace = ? AND target_id = ?"
        ),
        existing_item_ids,
        query_type="vector",
    )
    return len(existing_item_ids)


def _load_vector_matrix(
    sqlite: _SQLiteEngine,
) -> tuple[np.ndarray, np.ndarray]:
    """Load all SQLite-stored vectors into NumPy arrays.

    Args:
        sqlite: Canonical SQLite engine that owns vector storage.

    Returns:
        A tuple of logical identifier array and float32 embedding matrix.

    Raises:
        ValueError: If no vectors exist or the stored dimensions are inconsistent.
    """

    result = sqlite.execute(
        (
            "SELECT target, namespace, target_id, dimensions, embedding "
            "FROM vector_entries "
            "ORDER BY target, namespace, target_id"
        ),
        query_type="vector",
    )
    if not result.rows:
        return np.empty(0, dtype=object), np.empty((0, 0), dtype=np.float32)

    dimensions = int(result.rows[0][3])
    item_ids = np.empty(len(result.rows), dtype=object)
    matrix = np.empty((len(result.rows), dimensions), dtype=np.float32)

    for index, (target, namespace, target_id, row_dimensions, blob) in enumerate(
        result.rows
    ):
        if int(row_dimensions) != dimensions:
            raise ValueError(
                "HumemVector v0 requires one shared dimension per loaded vector "
                "set; got "
                f"{row_dimensions} and {dimensions}."
            )
        item_ids[index] = (str(target), str(namespace), int(target_id))
        matrix[index] = decode_vector_blob(blob, dimension=dimensions)

    return item_ids, matrix


def _upsert_vector_metadata(
    sqlite: _SQLiteEngine,
    rows: Sequence[tuple[int, Mapping[str, _VectorMetadataValue]]],
    *,
    target: str = "direct",
    namespace: str = "",
) -> None:
    """Insert or replace equality-filterable metadata for vector rows.

    Args:
        sqlite: Canonical SQLite engine that owns vector storage.
        rows: Sequence of `(target_id, metadata)` rows to write.
        target: Logical target namespace for the vectors.
        namespace: Optional namespace within the target.
    """

    encoded_rows: list[tuple[int, str, str | None, str]] = []
    for target_id, metadata in rows:
        vector_id = _load_vector_id(
            sqlite,
            target=target,
            namespace=namespace,
            target_id=target_id,
        )
        for key, value in metadata.items():
            encoded_value, value_type = _encode_metadata_value(value)
            encoded_rows.append((vector_id, key, encoded_value, value_type))

    if not encoded_rows:
        return

    sqlite.executemany(
        (
            "INSERT INTO vector_entry_metadata "
            "(vector_id, key, value, value_type) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(vector_id, key) DO UPDATE SET "
            "value = excluded.value, "
            "value_type = excluded.value_type"
        ),
        encoded_rows,
        query_type="vector",
    )


def _load_filtered_vector_target_keys(
    sqlite: _SQLiteEngine,
    filters: Mapping[str, _VectorMetadataValue],
    *,
    target: str = "direct",
    namespace: str = "",
) -> tuple[_VectorNamespaceKey, ...]:
    """Return logical vector identifiers whose metadata matches all filters.

    Args:
        sqlite: Canonical SQLite engine that owns vector storage.
        filters: Equality filters that must all match.
        target: Logical target namespace for the candidate vectors.
        namespace: Optional namespace within the target.

    Returns:
        A tuple of logical target keys that satisfy every filter.
    """

    if not filters:
        return ()

    matched_ids: set[_VectorNamespaceKey] | None = None
    for key, value in filters.items():
        encoded_value, value_type = _encode_metadata_value(value)
        if encoded_value is None:
            result = sqlite.execute(
                (
                    "SELECT e.target, e.namespace, e.target_id "
                    "FROM vector_entry_metadata AS m "
                    "JOIN vector_entries AS e ON e.vector_id = m.vector_id "
                    "WHERE e.target = ? AND e.namespace = ? "
                    "AND m.key = ? AND m.value_type = ? AND m.value IS NULL "
                    "ORDER BY e.target, e.namespace, e.target_id"
                ),
                params=(target, namespace, key, value_type),
                query_type="vector",
            )
        else:
            result = sqlite.execute(
                (
                    "SELECT e.target, e.namespace, e.target_id "
                    "FROM vector_entry_metadata AS m "
                    "JOIN vector_entries AS e ON e.vector_id = m.vector_id "
                    "WHERE e.target = ? AND e.namespace = ? "
                    "AND m.key = ? AND m.value_type = ? AND m.value = ? "
                    "ORDER BY e.target, e.namespace, e.target_id"
                ),
                params=(target, namespace, key, value_type, encoded_value),
                query_type="vector",
            )

        item_ids = {
            (str(row[0]), str(row[1]), int(row[2])) for row in result.rows
        }
        if matched_ids is None:
            matched_ids = item_ids
        else:
            matched_ids &= item_ids

        if not matched_ids:
            return ()

    return tuple(sorted(matched_ids or ()))


def _write_vector_rows(
    sqlite: _SQLiteEngine,
    rows: Sequence[tuple[int, Sequence[float]]],
    *,
    target: str,
    namespace: str,
    conflict_mode: Literal["insert", "upsert"],
) -> None:
    """Write one batch of target/namespace vector rows into SQLite."""

    encoded_rows = []
    for target_id, vector in rows:
        blob = encode_vector_blob(vector)
        encoded_rows.append((target, namespace, int(target_id), len(vector), blob))

    statement = (
        "INSERT INTO vector_entries "
        "(target, namespace, target_id, dimensions, embedding) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    if conflict_mode == "upsert":
        statement += (
            " ON CONFLICT(target, namespace, target_id) DO UPDATE SET "
            "dimensions = excluded.dimensions, "
            "embedding = excluded.embedding"
        )

    sqlite.executemany(
        statement,
        encoded_rows,
        query_type="vector",
    )


def _existing_vector_item_ids(
    sqlite: _SQLiteEngine,
    item_ids: Sequence[_VectorNamespaceKey],
) -> tuple[_VectorNamespaceKey, ...]:
    """Return the subset of logical vector ids that currently exist."""

    existing_item_ids: list[_VectorNamespaceKey] = []
    for target, namespace, target_id in item_ids:
        first_row = sqlite.execute(
            (
                "SELECT 1 FROM vector_entries "
                "WHERE target = ? AND namespace = ? AND target_id = ?"
            ),
            params=(str(target), str(namespace), int(target_id)),
            query_type="vector",
        ).first()
        if first_row is not None:
            existing_item_ids.append((str(target), str(namespace), int(target_id)))
    return tuple(existing_item_ids)


def _load_vector_tombstones(
    sqlite: _SQLiteEngine,
    *,
    metric: VectorMetric,
) -> tuple[_VectorNamespaceKey, ...]:
    """Return logical vector ids deleted since the live cold snapshot was built."""

    result = sqlite.execute(
        (
            "SELECT target, namespace, target_id FROM vector_cold_tombstones "
            "WHERE metric = ? "
            "ORDER BY target, namespace, target_id"
        ),
        params=(metric,),
        query_type="vector",
    )
    return tuple(
        (str(row[0]), str(row[1]), int(row[2]))
        for row in result.rows
    )


def _clear_vector_tombstones(
    sqlite: _SQLiteEngine,
    item_ids: Sequence[_VectorNamespaceKey] | None = None,
    *,
    metric: VectorMetric | None = None,
) -> None:
    """Delete tombstone records either for one subset or for all logical ids."""

    if item_ids is None:
        if metric is None:
            sqlite.execute("DELETE FROM vector_cold_tombstones", query_type="vector")
        else:
            sqlite.execute(
                "DELETE FROM vector_cold_tombstones WHERE metric = ?",
                params=(metric,),
                query_type="vector",
            )
        return

    normalized_item_ids = tuple(
        (str(target), str(namespace), int(target_id))
        for target, namespace, target_id in item_ids
    )
    if not normalized_item_ids:
        return
    if metric is None:
        sqlite.executemany(
            (
                "DELETE FROM vector_cold_tombstones "
                "WHERE target = ? AND namespace = ? AND target_id = ?"
            ),
            normalized_item_ids,
            query_type="vector",
        )
        return
    sqlite.executemany(
        (
            "DELETE FROM vector_cold_tombstones "
            "WHERE metric = ? AND target = ? AND namespace = ? AND target_id = ?"
        ),
        tuple(
            (metric, target, namespace, target_id)
            for target, namespace, target_id in normalized_item_ids
        ),
        query_type="vector",
    )


def _load_cold_vector_snapshot_metadata(
    sqlite: _SQLiteEngine,
    *,
    metric: VectorMetric,
) -> _ColdVectorSnapshotMetadata | None:
    """Return persisted metadata for one cold ANN snapshot, when present."""

    first_row = sqlite.execute(
        (
            "SELECT metric, table_name, row_count, generation "
            "FROM vector_cold_snapshots WHERE metric = ?"
        ),
        params=(metric,),
        query_type="vector",
    ).first()
    if first_row is None:
        return None
    return _ColdVectorSnapshotMetadata(
        metric=cast(VectorMetric, str(first_row[0])),
        table_name=str(first_row[1]),
        row_count=int(first_row[2]),
        generation=int(first_row[3]),
    )


def _list_cold_vector_snapshot_metadata(
    sqlite: _SQLiteEngine,
) -> tuple[_ColdVectorSnapshotMetadata, ...]:
    """Return persisted metadata for all known cold ANN snapshots."""

    result = sqlite.execute(
        (
            "SELECT metric, table_name, row_count, generation "
            "FROM vector_cold_snapshots ORDER BY metric"
        ),
        query_type="vector",
    )
    return tuple(
        _ColdVectorSnapshotMetadata(
            metric=cast(VectorMetric, str(row[0])),
            table_name=str(row[1]),
            row_count=int(row[2]),
            generation=int(row[3]),
        )
        for row in result.rows
    )


def _load_named_vector_index(
    sqlite: _SQLiteEngine,
    *,
    name: str,
) -> _NamedVectorIndex | None:
    """Return one persisted named vector index row, when present."""

    first_row = sqlite.execute(
        (
            "SELECT name, metric, enabled, maintenance_paused "
            "FROM vector_named_indexes WHERE name = ?"
        ),
        params=(str(name),),
        query_type="vector",
    ).first()
    if first_row is None:
        return None
    return _NamedVectorIndex(
        name=str(first_row[0]),
        metric=cast(VectorMetric, str(first_row[1])),
        enabled=bool(int(first_row[2])),
        maintenance_paused=bool(int(first_row[3])),
    )


def _load_named_vector_index_for_metric(
    sqlite: _SQLiteEngine,
    *,
    metric: VectorMetric,
) -> _NamedVectorIndex | None:
    """Return the persisted named vector index for one metric, when present."""

    first_row = sqlite.execute(
        (
            "SELECT name, metric, enabled, maintenance_paused "
            "FROM vector_named_indexes WHERE metric = ?"
        ),
        params=(metric,),
        query_type="vector",
    ).first()
    if first_row is None:
        return None
    return _NamedVectorIndex(
        name=str(first_row[0]),
        metric=cast(VectorMetric, str(first_row[1])),
        enabled=bool(int(first_row[2])),
        maintenance_paused=bool(int(first_row[3])),
    )


def _list_named_vector_indexes(
    sqlite: _SQLiteEngine,
) -> tuple[_NamedVectorIndex, ...]:
    """Return persisted named vector index rows."""

    result = sqlite.execute(
        (
            "SELECT name, metric, enabled, maintenance_paused "
            "FROM vector_named_indexes ORDER BY name"
        ),
        query_type="vector",
    )
    return tuple(
        _NamedVectorIndex(
            name=str(row[0]),
            metric=cast(VectorMetric, str(row[1])),
            enabled=bool(int(row[2])),
            maintenance_paused=bool(int(row[3])),
        )
        for row in result.rows
    )


def _delete_named_vector_index(
    sqlite: _SQLiteEngine,
    *,
    name: str,
) -> None:
    """Delete one persisted named vector index row."""

    sqlite.execute(
        "DELETE FROM vector_named_indexes WHERE name = ?",
        params=(str(name),),
        query_type="vector",
    )


def _upsert_named_vector_index(
    sqlite: _SQLiteEngine,
    *,
    name: str,
    metric: VectorMetric,
    enabled: bool,
    maintenance_paused: bool | None = None,
) -> None:
    """Persist one named vector index lifecycle row."""

    existing = _load_named_vector_index_for_metric(sqlite, metric=metric)
    resolved_maintenance_paused = maintenance_paused
    if resolved_maintenance_paused is None:
        resolved_maintenance_paused = (
            existing.maintenance_paused if existing is not None else False
        )

    sqlite.execute(
        (
            "INSERT INTO vector_named_indexes"
            "(name, metric, enabled, maintenance_paused) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(name) DO UPDATE SET metric = excluded.metric, "
            "enabled = excluded.enabled, "
            "maintenance_paused = excluded.maintenance_paused"
        ),
        params=(name, metric, int(enabled), int(resolved_maintenance_paused)),
        query_type="vector",
    )


def _upsert_cold_vector_snapshot_metadata(
    sqlite: _SQLiteEngine,
    *,
    metric: VectorMetric,
    table_name: str,
    row_count: int,
    generation: int,
) -> None:
    """Persist the current live cold ANN snapshot metadata for one metric."""

    sqlite.execute(
        (
            "INSERT INTO vector_cold_snapshots "
            "(metric, table_name, row_count, generation) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(metric) DO UPDATE SET "
            "table_name = excluded.table_name, "
            "row_count = excluded.row_count, "
            "generation = excluded.generation"
        ),
        params=(metric, table_name, int(row_count), int(generation)),
        query_type="vector",
    )


def _delete_cold_vector_snapshot_metadata(
    sqlite: _SQLiteEngine,
    *,
    metric: VectorMetric | None = None,
) -> None:
    """Delete persisted cold ANN snapshot metadata for one metric or all metrics."""

    if metric is None:
        sqlite.execute("DELETE FROM vector_cold_snapshots", query_type="vector")
        return
    sqlite.execute(
        "DELETE FROM vector_cold_snapshots WHERE metric = ?",
        params=(metric,),
        query_type="vector",
    )


def _load_vector_id(
    sqlite: _SQLiteEngine,
    *,
    target: str,
    namespace: str,
    target_id: int,
) -> int:
    """Resolve one logical target key to its internal vector row id."""

    result = sqlite.execute(
        (
            "SELECT vector_id FROM vector_entries "
            "WHERE target = ? AND namespace = ? AND target_id = ?"
        ),
        params=(target, namespace, int(target_id)),
        query_type="vector",
    )
    if not result.rows:
        raise ValueError(
            "HumemVector v0 metadata writes require an existing target/namespace "
            f"vector row; got {target!r}, {namespace!r}, {target_id!r}."
        )
    return int(result.rows[0][0])


def encode_vector_blob(vector: Sequence[float]) -> bytes:
    """Encode a vector as a float32 SQLite blob.

    Args:
        vector: Numeric vector to encode.

    Returns:
        SQLite-ready float32 bytes.
    """

    array = _coerce_query(vector)
    return array.astype(np.float32, copy=False).tobytes()


def decode_vector_blob(blob: bytes, *, dimension: int) -> np.ndarray:
    """Decode a float32 SQLite blob into a detached NumPy vector.

    Args:
        blob: SQLite BLOB containing float32 vector bytes.
        dimension: Expected dimensionality of the stored vector.

    Returns:
        A detached float32 NumPy vector.

    Raises:
        ValueError: If the blob size does not match `dimension`.
    """

    array = np.frombuffer(blob, dtype=np.float32)
    if array.size != dimension:
        raise ValueError(
            "HumemVector v0 expected a float32 blob with "
            f"dimension {dimension}, got {array.size}."
        )
    return np.array(array, dtype=np.float32, copy=True)


@dataclass(slots=True)
class _ExactVectorIndex:
    """Exact vector search over a contiguous NumPy matrix.

    Attributes:
        item_ids: One logical identifier per vector row.
        matrix: Backing float32 matrix used for exact search.
        metric: Similarity metric used when scoring queries.
    """

    item_ids: np.ndarray
    matrix: np.ndarray
    metric: VectorMetric = "cosine"

    def __post_init__(self) -> None:
        """Validate and normalize the backing matrix after dataclass construction.

        Raises:
            ValueError: If the identifier count does not match the matrix row count.
        """

        matrix = _coerce_matrix(self.matrix)
        item_ids = _coerce_identifier_array(self.item_ids)
        if item_ids.size != matrix.shape[0]:
            raise ValueError(
                "HumemVector v0 expected one item id per matrix row; got "
                f"{item_ids.size} ids and {matrix.shape[0]} rows."
            )

        object.__setattr__(self, "item_ids", item_ids)
        if self.metric == "cosine":
            object.__setattr__(self, "matrix", _normalize_rows(matrix))
        else:
            object.__setattr__(self, "matrix", matrix)

    @property
    def dimensions(self) -> int:
        """Return the shared dimensionality of the indexed vectors.

        Returns:
            Number of dimensions in each indexed vector.
        """

        return int(self.matrix.shape[1])

    def search(
        self,
        query: Sequence[float],
        *,
        top_k: int,
        candidate_indexes: Sequence[int] | np.ndarray | None = None,
    ) -> tuple[_VectorSearchMatch, ...]:
        """Return the nearest results for one query vector.

        Args:
            query: Query embedding to rank against the index.
            top_k: Maximum number of matches to return.
            candidate_indexes: Optional subset of row indexes to search.

        Returns:
            Ranked nearest-neighbor matches.
        """

        query_array = _coerce_query(query, dimension=self.dimensions)
        item_ids, matrix = self.item_ids, self.matrix
        if candidate_indexes is not None:
            indexes = _coerce_candidate_indexes(
                candidate_indexes,
                total=matrix.shape[0],
            )
            item_ids = item_ids[indexes]
            matrix = matrix[indexes]

        if self.metric == "cosine":
            query_array = _normalize_query(query_array)
            scores = np.atleast_1d(np.asarray(matrix @ query_array, dtype=np.float32))
        elif self.metric == "dot":
            scores = np.atleast_1d(np.asarray(matrix @ query_array, dtype=np.float32))
        else:
            diff = matrix - query_array
            scores = np.atleast_1d(
                np.asarray(-np.sum(diff * diff, axis=1, dtype=np.float32))
            )

        return _build_matches(item_ids, scores, top_k)


@dataclass(slots=True)
class ScalarQuantizedVectorIndex:
    """Simple scalar-int8 approximation for benchmark comparison.

    The quantization is per-dimension and symmetric around zero. This is not meant to be
    HumemDB's final vector format; it is a benchmarkable approximation layer that helps
    estimate the value of quantization before routing to LanceDB is finalized.

    Attributes:
        item_ids: One logical identifier per quantized row.
        quantized: Int8 matrix used for approximate search.
        scales: Per-dimension scale factors used to dequantize scores.
        metric: Similarity metric used when scoring queries.
    """

    item_ids: np.ndarray
    quantized: np.ndarray
    scales: np.ndarray
    metric: VectorMetric = "cosine"

    @classmethod
    def from_matrix(
        cls,
        item_ids: Sequence[Any],
        matrix: np.ndarray,
        *,
        metric: VectorMetric = "cosine",
    ) -> ScalarQuantizedVectorIndex:
        """Quantize a float32 matrix into the scalar-int8 benchmark format.

        Args:
            item_ids: One logical identifier per matrix row.
            matrix: Float32 matrix to quantize.
            metric: Similarity metric the quantized index should emulate.

        Returns:
            A `ScalarQuantizedVectorIndex` built from the input matrix.
        """

        base_matrix = _coerce_matrix(matrix)
        if metric == "cosine":
            base_matrix = _normalize_rows(base_matrix)

        max_abs = np.max(np.abs(base_matrix), axis=0)
        scales = np.where(max_abs > 0.0, max_abs / 127.0, 1.0).astype(np.float32)
        quantized = np.clip(
            np.rint(base_matrix / scales),
            -127,
            127,
        ).astype(np.int8)
        return cls(
            item_ids=_coerce_identifier_array(item_ids),
            quantized=quantized,
            scales=scales,
            metric=metric,
        )

    @property
    def dimensions(self) -> int:
        """Return the shared dimensionality of the quantized vectors.

        Returns:
            Number of dimensions in each quantized vector.
        """

        return int(self.quantized.shape[1])

    def search(
        self,
        query: Sequence[float],
        *,
        top_k: int,
        candidate_indexes: Sequence[int] | np.ndarray | None = None,
    ) -> tuple[_VectorSearchMatch, ...]:
        """Return approximate nearest results for one query vector.

        Args:
            query: Query embedding to rank against the quantized index.
            top_k: Maximum number of matches to return.
            candidate_indexes: Optional subset of row indexes to search.

        Returns:
            Ranked approximate nearest-neighbor matches.
        """

        query_array = _coerce_query(query, dimension=self.dimensions)
        if self.metric == "cosine":
            query_array = _normalize_query(query_array)

        item_ids, quantized = self.item_ids, self.quantized
        if candidate_indexes is not None:
            indexes = _coerce_candidate_indexes(
                candidate_indexes,
                total=quantized.shape[0],
            )
            item_ids = item_ids[indexes]
            quantized = quantized[indexes]

        if self.metric in {"cosine", "dot"}:
            scaled_query = query_array * self.scales
            scores = np.atleast_1d(
                np.asarray(quantized @ scaled_query, dtype=np.float32)
            )
        else:
            approx_matrix = quantized.astype(np.float32) * self.scales
            diff = approx_matrix - query_array
            scores = np.atleast_1d(
                np.asarray(-np.sum(diff * diff, axis=1, dtype=np.float32))
            )

        return _build_matches(item_ids, scores, top_k)


@dataclass(frozen=True, slots=True)
class LanceDBIndexConfig:
    """Configuration for one LanceDB-backed indexed vector path.

    These settings intentionally mirror the current benchmark knobs so the first
    indexed runtime can be tuned and compared against the benchmark evidence instead
    of introducing an unrelated configuration model.
    """

    index_type: str = "IVF_PQ"
    num_partitions: int | None = None
    num_sub_vectors: int | None = None
    num_bits: int = 8
    m: int = 20
    ef_construction: int = 300
    sample_rate: int = 256
    max_iterations: int = 50
    target_partition_size: int | None = None
    nprobes: int | None = None
    refine_factor: int | None = None
    ef: int | None = None
    table_name: str = "vectors"

    def with_table_name(self, table_name: str) -> LanceDBIndexConfig:
        """Return a copy of this config with a different table name."""

        return replace(self, table_name=table_name)

    def index_kwargs(self, *, metric: VectorMetric) -> dict[str, Any]:
        """Return LanceDB `create_index(...)` kwargs for this config."""

        kwargs: dict[str, Any] = {
            "metric": metric,
            "vector_column_name": "vector",
            "index_type": self.index_type,
            "num_bits": self.num_bits,
            "max_iterations": self.max_iterations,
            "sample_rate": self.sample_rate,
            "m": self.m,
            "ef_construction": self.ef_construction,
        }
        if self.num_partitions is not None:
            kwargs["num_partitions"] = self.num_partitions
        if self.num_sub_vectors is not None:
            kwargs["num_sub_vectors"] = self.num_sub_vectors
        if self.target_partition_size is not None:
            kwargs["target_partition_size"] = self.target_partition_size
        return kwargs

    def minimum_rows_for_training(self) -> int | None:
        """Return the smallest dataset size that can train this index family."""

        if self.index_type == "IVF_PQ":
            return 256
        return None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view of the current index config."""

        return {
            "index_type": self.index_type,
            "num_partitions": self.num_partitions,
            "num_sub_vectors": self.num_sub_vectors,
            "num_bits": self.num_bits,
            "m": self.m,
            "ef_construction": self.ef_construction,
            "sample_rate": self.sample_rate,
            "max_iterations": self.max_iterations,
            "target_partition_size": self.target_partition_size,
            "nprobes": self.nprobes,
            "refine_factor": self.refine_factor,
            "ef": self.ef,
            "table_name": self.table_name,
        }


@dataclass(slots=True)
class _LanceDBVectorIndex:
    """LanceDB-backed indexed vector search over the current vector set.

    This is the first large-scale indexed path for HumemVector. It keeps the same
    logical `(target, namespace, target_id)` identifiers as the exact NumPy path, but
    stores the backing vectors in a local LanceDB table and builds one ANN index over
    the `vector` column.
    """

    item_ids: np.ndarray
    metric: VectorMetric
    dimensions: int
    lance_path: Path
    config: LanceDBIndexConfig
    _table: Any

    @classmethod
    def from_matrix(
        cls,
        *,
        item_ids: Sequence[Any],
        matrix: np.ndarray,
        metric: VectorMetric = "cosine",
        lance_path: str | Path,
        config: LanceDBIndexConfig | None = None,
    ) -> _LanceDBVectorIndex:
        """Create one LanceDB table and build an index from a float32 matrix."""

        normalized_item_ids = _coerce_identifier_array(item_ids)
        normalized_matrix = _coerce_matrix(matrix)
        if normalized_item_ids.size != normalized_matrix.shape[0]:
            raise ValueError(
                "HumemVector v0 expected one item id per matrix row; got "
                f"{normalized_item_ids.size} ids and {normalized_matrix.shape[0]} rows."
            )

        resolved_config = config or LanceDBIndexConfig()
        table = _create_lancedb_table(
            item_ids=normalized_item_ids,
            matrix=normalized_matrix,
            lance_path=Path(lance_path),
            table_name=resolved_config.table_name,
        )
        table.create_index(**resolved_config.index_kwargs(metric=metric))
        return cls(
            item_ids=normalized_item_ids,
            metric=metric,
            dimensions=int(normalized_matrix.shape[1]),
            lance_path=Path(lance_path),
            config=resolved_config,
            _table=table,
        )

    @classmethod
    def from_existing(
        cls,
        *,
        metric: VectorMetric,
        lance_path: str | Path,
        config: LanceDBIndexConfig,
    ) -> _LanceDBVectorIndex:
        """Open one previously built LanceDB table from persisted metadata."""

        table = _open_lancedb_table(
            lance_path=Path(lance_path),
            table_name=config.table_name,
        )
        rows = cast(Any, table).to_arrow().to_pylist()
        if not rows:
            raise ValueError(
                "HumemVector cold LanceDB snapshot metadata pointed at an empty "
                f"table: {config.table_name!r}."
            )
        item_ids = np.empty(len(rows), dtype=object)
        for index, row in enumerate(rows):
            item_ids[index] = (
                str(row["target"]),
                str(row["namespace"]),
                int(row["target_id"]),
            )
        dimensions = len(cast(list[Any], rows[0]["vector"]))
        return cls(
            item_ids=item_ids,
            metric=metric,
            dimensions=dimensions,
            lance_path=Path(lance_path),
            config=config,
            _table=table,
        )

    def search(
        self,
        query: Sequence[float],
        *,
        top_k: int,
        candidate_indexes: Sequence[int] | np.ndarray | None = None,
    ) -> tuple[_VectorSearchMatch, ...]:
        """Return indexed nearest-neighbor matches for one query vector."""

        query_array = _coerce_query(query, dimension=self.dimensions)
        row_count = int(self.item_ids.size)
        if row_count == 0:
            return ()

        builder = (
            self._table.search(query_array.tolist())
            .distance_type(self.metric)
            .limit(_validated_top_k(top_k, total=row_count))
        )
        if self.config.nprobes is not None:
            builder = builder.nprobes(self.config.nprobes)
        if self.config.refine_factor is not None:
            builder = builder.refine_factor(self.config.refine_factor)
        if self.config.ef is not None:
            builder = builder.ef(self.config.ef)

        if candidate_indexes is not None:
            indexes = _coerce_candidate_indexes(candidate_indexes, total=row_count)
            if indexes.size == 0:
                return ()
            if indexes.size != row_count:
                builder = builder.where(
                    _lancedb_item_filter_expression(self.item_ids[indexes]),
                    prefilter=True,
                )

        rows = builder.select(
            ["target", "namespace", "target_id", "_distance"]
        ).to_list()
        return tuple(
            _VectorSearchMatch(
                target=str(row["target"]),
                namespace=str(row["namespace"]),
                target_id=int(row["target_id"]),
                score=_lancedb_score_to_similarity(
                    self.metric,
                    float(row["_distance"]),
                ),
            )
            for row in rows
        )

    def describe(self) -> dict[str, Any]:
        """Return a serializable summary of this indexed vector path."""

        return {
            "metric": self.metric,
            "dimensions": self.dimensions,
            "row_count": int(self.item_ids.size),
            "lance_path": str(self.lance_path),
            "config": self.config.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class IndexedVectorRuntimeConfig:
    """Private runtime policy for hot/cold vector execution."""

    hot_max_rows: int = 100_000
    merge_buffer_factor: int = 4
    cold_index_min_rows: int = 0
    cold_index_relative_to_hot_fraction: float = 0.10
    cold_refresh_min_rows: int = 0
    cold_refresh_relative_to_cold_fraction: float = 0.10
    lancedb: LanceDBIndexConfig = field(
        default_factory=lambda: LanceDBIndexConfig(
            nprobes=32,
            refine_factor=4,
        )
    )

    def __post_init__(self) -> None:
        """Validate runtime policy values after dataclass construction."""

        if self.hot_max_rows < 1:
            raise ValueError("HumemVector hot_max_rows must be at least 1.")
        if self.merge_buffer_factor < 1:
            raise ValueError(
                "HumemVector merge_buffer_factor must be at least 1."
            )
        if self.cold_index_min_rows < 0:
            raise ValueError("HumemVector cold_index_min_rows cannot be negative.")
        if self.cold_index_relative_to_hot_fraction < 0.0:
            raise ValueError(
                "HumemVector cold_index_relative_to_hot_fraction cannot be "
                "negative."
            )
        if self.cold_refresh_min_rows < 0:
            raise ValueError("HumemVector cold_refresh_min_rows cannot be negative.")
        if self.cold_refresh_relative_to_cold_fraction < 0.0:
            raise ValueError(
                "HumemVector cold_refresh_relative_to_cold_fraction cannot be "
                "negative."
            )

    def buffered_top_k(self, top_k: int) -> int:
        """Return the first-pass per-tier top-k buffer for merge-and-rerank."""

        if top_k < 1:
            raise ValueError("HumemVector v0 top_k must be at least 1.")
        return max(top_k, top_k * self.merge_buffer_factor)

    def cold_index_required_rows(
        self,
        *,
        hot_rows: int,
        minimum_training_rows: int | None = None,
    ) -> int:
        """Return the minimum cold-row count required before building cold ANN.

        The effective threshold is the maximum of the explicit absolute floor, the
        relative spill-over threshold against the hot tier, and the selected
        LanceDB index family's training minimum.
        """

        threshold = self.cold_index_min_rows
        if self.cold_index_relative_to_hot_fraction > 0.0:
            threshold = max(
                threshold,
                int(math.ceil(hot_rows * self.cold_index_relative_to_hot_fraction)),
            )
        if minimum_training_rows is not None:
            threshold = max(threshold, minimum_training_rows)
        return threshold

    def cold_refresh_required_rows(
        self,
        *,
        cold_rows: int,
    ) -> int:
        """Return the pending cold-spill size that should trigger one refresh."""

        threshold = self.cold_refresh_min_rows
        if self.cold_refresh_relative_to_cold_fraction > 0.0:
            threshold = max(
                threshold,
                int(
                    math.ceil(
                        cold_rows * self.cold_refresh_relative_to_cold_fraction
                    )
                ),
            )
        return threshold


def _merge_vector_search_matches(
    *groups: Sequence[_VectorSearchMatch],
    top_k: int,
) -> tuple[_VectorSearchMatch, ...]:
    """Merge tier-local results by logical id, keep best score, and trim to top-k."""

    _validated_top_k(top_k, total=max(top_k, 1))
    merged: dict[_VectorNamespaceKey, _VectorSearchMatch] = {}
    first_seen: dict[_VectorNamespaceKey, int] = {}
    ordinal = 0
    for group in groups:
        for match in group:
            key = (match.target, match.namespace, match.target_id)
            existing = merged.get(key)
            if existing is None or match.score > existing.score:
                merged[key] = match
                first_seen.setdefault(key, ordinal)
            ordinal += 1

    ranked = sorted(
        merged.values(),
        key=lambda match: (
            -match.score,
            first_seen[(match.target, match.namespace, match.target_id)],
        ),
    )
    return tuple(ranked[:top_k])


def _build_matches(
    item_ids: np.ndarray,
    scores: np.ndarray,
    top_k: int,
) -> tuple[_VectorSearchMatch, ...]:
    """Convert raw similarity scores into a sorted top-k match tuple."""

    count = _validated_top_k(top_k, total=scores.size)
    candidate_indexes = np.argpartition(-scores, count - 1)[:count]
    sorted_indexes = candidate_indexes[
        np.argsort(-scores[candidate_indexes], kind="stable")
    ]
    return tuple(
        _VectorSearchMatch(
            target=match_id[0],
            namespace=match_id[1],
            target_id=match_id[2],
            score=float(scores[index]),
        )
        for index in sorted_indexes
        for match_id in (_coerce_match_id(item_ids[index]),)
    )


def _coerce_match_id(identifier: Any) -> _VectorNamespaceKey:
    """Normalize one stored identifier into the public target/namespace/id shape."""

    if isinstance(identifier, tuple) and len(identifier) == 3:
        return (str(identifier[0]), str(identifier[1]), int(identifier[2]))
    if isinstance(identifier, tuple):
        raise ValueError(
            "HumemVector v0 tuple-backed match identifiers must have exactly three "
            "elements."
        )
    if isinstance(identifier, (str, bytes, bytearray)):
        raise ValueError(
            "HumemVector v0 direct match identifiers must be integer-like, got a "
            "string-like value."
        )
    return ("direct", "", int(identifier))


def _coerce_matrix(matrix: np.ndarray) -> np.ndarray:
    """Validate one matrix input and return it as contiguous float32."""

    array = np.asarray(matrix, dtype=np.float32)
    if array.ndim != 2:
        raise ValueError("HumemVector v0 matrices must be two-dimensional.")
    if array.shape[0] == 0 or array.shape[1] == 0:
        raise ValueError("HumemVector v0 matrices cannot be empty.")
    return np.ascontiguousarray(array)


def _coerce_identifier_array(item_ids: Any) -> np.ndarray:
    """Normalize one identifier sequence into a one-dimensional object array."""

    raw = np.asarray(item_ids, dtype=object)
    if raw.ndim == 1:
        values = raw.tolist()
    elif raw.ndim == 2 and raw.shape[1] == 3:
        values = [tuple(row.tolist()) for row in raw]
    else:
        raise ValueError("HumemVector v0 item ids must be one-dimensional.")

    normalized = np.empty(len(values), dtype=object)
    for index, value in enumerate(values):
        normalized[index] = _coerce_match_id(value)
    return normalized


def _coerce_query(
    query: Sequence[float],
    *,
    dimension: int | None = None,
) -> np.ndarray:
    """Validate one query vector and return it as contiguous float32."""

    array = np.asarray(query, dtype=np.float32)
    if array.ndim != 1:
        raise ValueError("HumemVector v0 query vectors must be one-dimensional.")
    if dimension is not None and array.size != dimension:
        raise ValueError(
            "HumemVector v0 query dimension mismatch: expected "
            f"{dimension}, got {array.size}."
        )
    if array.size == 0:
        raise ValueError("HumemVector v0 query vectors cannot be empty.")
    return np.ascontiguousarray(array)


def _normalize_rows(matrix: np.ndarray) -> np.ndarray:
    """Normalize each row vector to unit length for cosine search."""

    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    safe_norms = np.where(norms > 0.0, norms, 1.0)
    return np.ascontiguousarray(matrix / safe_norms, dtype=np.float32)


def _normalize_query(query: np.ndarray) -> np.ndarray:
    """Normalize one query vector to unit length for cosine search."""

    norm = float(np.linalg.norm(query))
    if norm == 0.0:
        raise ValueError("HumemVector v0 cosine queries cannot be zero vectors.")
    return np.ascontiguousarray(query / norm, dtype=np.float32)


def _validated_top_k(top_k: int, *, total: int) -> int:
    """Clamp one requested top-k value to the available score count."""

    if top_k < 1:
        raise ValueError("HumemVector v0 top_k must be at least 1.")
    return min(top_k, total)


def _encode_metadata_value(value: _VectorMetadataValue) -> tuple[str | None, str]:
    """Encode one metadata scalar into the SQLite filter storage format."""

    if value is None:
        return None, "null"
    if isinstance(value, bool):
        return ("1" if value else "0"), "boolean"
    if isinstance(value, int):
        return str(value), "integer"
    if isinstance(value, float):
        return repr(value), "real"
    if isinstance(value, str):
        return value, "string"
    raise ValueError(
        "HumemVector v0 metadata filters support only str, int, float, bool, or None."
    )


def _coerce_candidate_indexes(
    candidate_indexes: Sequence[int] | np.ndarray,
    *,
    total: int,
) -> np.ndarray:
    """Validate one candidate-index list against the current matrix size."""

    indexes = np.asarray(candidate_indexes, dtype=np.int64)
    if indexes.ndim != 1:
        raise ValueError("HumemVector v0 candidate indexes must be one-dimensional.")
    if indexes.size == 0:
        raise ValueError("HumemVector v0 candidate indexes cannot be empty.")
    if np.any(indexes < 0) or np.any(indexes >= total):
        raise ValueError("HumemVector v0 candidate indexes are out of range.")
    return indexes


def _create_lancedb_table(
    *,
    item_ids: np.ndarray,
    matrix: np.ndarray,
    lance_path: Path,
    table_name: str,
):
    """Create one local LanceDB table from the current logical vector set."""

    lancedb, pa = _load_lancedb_dependencies()
    lance_path.parent.mkdir(parents=True, exist_ok=True)
    db = lancedb.connect(str(lance_path))
    schema = pa.schema(
        [
            pa.field("target", pa.string()),
            pa.field("namespace", pa.string()),
            pa.field("target_id", pa.int64()),
            pa.field("vector", pa.list_(pa.float32(), matrix.shape[1])),
        ]
    )
    table = pa.table(
        {
            "target": pa.array(
                [item_id[0] for item_id in item_ids.tolist()],
                type=pa.string(),
            ),
            "namespace": pa.array(
                [item_id[1] for item_id in item_ids.tolist()],
                type=pa.string(),
            ),
            "target_id": pa.array(
                [int(item_id[2]) for item_id in item_ids.tolist()],
                type=pa.int64(),
            ),
            "vector": pa.array(
                matrix.tolist(),
                type=pa.list_(pa.float32(), matrix.shape[1]),
            ),
        },
        schema=schema,
    )
    return db.create_table(table_name, data=table, mode="overwrite")


def _open_lancedb_table(
    *,
    lance_path: Path,
    table_name: str,
):
    """Open one existing local LanceDB table."""

    lancedb, _ = _load_lancedb_dependencies()
    db = lancedb.connect(str(lance_path))
    return db.open_table(table_name)


def _drop_lancedb_table(*, lance_path: str | Path, table_name: str) -> None:
    """Best-effort removal of one local LanceDB table."""

    lancedb, _ = _load_lancedb_dependencies()
    db = lancedb.connect(str(Path(lance_path)))
    drop_table = getattr(db, "drop_table", None)
    if not callable(drop_table):
        return
    try:
        drop_table(table_name)
    except TypeError:
        return


def _load_lancedb_dependencies() -> tuple[Any, Any]:
    """Import LanceDB and PyArrow lazily for indexed-vector execution."""

    try:
        import lancedb
        import pyarrow as pa
    except ImportError as exc:
        raise RuntimeError(
            "HumemVector indexed search requires the optional LanceDB runtime "
            "dependencies to be installed."
        ) from exc
    return lancedb, pa


def _lancedb_item_filter_expression(item_ids: np.ndarray) -> str:
    """Build one LanceDB prefilter expression for a subset of logical keys."""

    grouped: dict[tuple[str, str], list[int]] = {}
    for target, namespace, target_id in item_ids.tolist():
        grouped.setdefault((str(target), str(namespace)), []).append(int(target_id))

    clauses = []
    for (target, namespace), target_ids in grouped.items():
        sorted_ids = sorted(set(target_ids))
        if len(sorted_ids) == 1:
            target_id_clause = f"target_id = {sorted_ids[0]}"
        else:
            joined_ids = ", ".join(str(value) for value in sorted_ids)
            target_id_clause = f"target_id IN ({joined_ids})"
        clauses.append(
            "("
            f"target = {_sql_quote_string(target)} AND "
            f"namespace = {_sql_quote_string(namespace)} AND "
            f"{target_id_clause}"
            ")"
        )
    return " OR ".join(clauses)


def _sql_quote_string(value: str) -> str:
    """Quote one string literal for the current LanceDB SQL filter syntax."""

    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _lancedb_score_to_similarity(metric: VectorMetric, distance: float) -> float:
    """Convert LanceDB distance outputs into HumemDB score semantics."""

    if metric == "cosine":
        return 1.0 - distance
    return -distance
