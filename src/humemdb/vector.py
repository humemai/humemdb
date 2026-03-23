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

from dataclasses import dataclass
from typing import Any, Literal, Mapping, Sequence, TypeAlias

import numpy as np

from .engines import SQLiteEngine

VectorMetric: TypeAlias = Literal["cosine", "dot", "l2"]
VectorMetadataValue: TypeAlias = str | int | float | bool | None
VectorNamespaceKey: TypeAlias = tuple[str, str, int]


@dataclass(frozen=True, slots=True)
class VectorSearchMatch:
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


def ensure_vector_schema(sqlite: SQLiteEngine) -> None:
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
    ):
        sqlite.execute(statement, query_type="vector")


def insert_vectors(
    sqlite: SQLiteEngine,
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


def upsert_vectors(
    sqlite: SQLiteEngine,
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


def load_vector_matrix(
    sqlite: SQLiteEngine,
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
        raise ValueError("HumemVector v0 could not load vectors: no rows found.")

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


def upsert_vector_metadata(
    sqlite: SQLiteEngine,
    rows: Sequence[tuple[int, Mapping[str, VectorMetadataValue]]],
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


def load_filtered_vector_target_keys(
    sqlite: SQLiteEngine,
    filters: Mapping[str, VectorMetadataValue],
    *,
    target: str = "direct",
    namespace: str = "",
) -> tuple[VectorNamespaceKey, ...]:
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

    matched_ids: set[VectorNamespaceKey] | None = None
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
    sqlite: SQLiteEngine,
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


def _load_vector_id(
    sqlite: SQLiteEngine,
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
class ExactVectorIndex:
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
    ) -> tuple[VectorSearchMatch, ...]:
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
    ) -> tuple[VectorSearchMatch, ...]:
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


def _build_matches(
    item_ids: np.ndarray,
    scores: np.ndarray,
    top_k: int,
) -> tuple[VectorSearchMatch, ...]:
    """Convert raw similarity scores into a sorted top-k match tuple."""

    count = _validated_top_k(top_k, total=scores.size)
    candidate_indexes = np.argpartition(-scores, count - 1)[:count]
    sorted_indexes = candidate_indexes[
        np.argsort(-scores[candidate_indexes], kind="stable")
    ]
    return tuple(
        VectorSearchMatch(
            target=match_id[0],
            namespace=match_id[1],
            target_id=match_id[2],
            score=float(scores[index]),
        )
        for index in sorted_indexes
        for match_id in (_coerce_match_id(item_ids[index]),)
    )


def _coerce_match_id(identifier: Any) -> VectorNamespaceKey:
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


def _encode_metadata_value(value: VectorMetadataValue) -> tuple[str | None, str]:
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
