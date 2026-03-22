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


@dataclass(frozen=True, slots=True)
class VectorSearchMatch:
    """One nearest-neighbor result from an exact or quantized vector search."""

    item_id: Any
    score: float


def ensure_vector_schema(sqlite: SQLiteEngine) -> None:
    """Create the initial SQLite-backed vector storage tables if needed."""

    for statement in (
        (
            "CREATE TABLE IF NOT EXISTS vector_entries ("
            "item_id INTEGER PRIMARY KEY, "
            "dimensions INTEGER NOT NULL, "
            "embedding BLOB NOT NULL)"
        ),
        (
            "CREATE TABLE IF NOT EXISTS vector_entry_metadata ("
            "item_id INTEGER NOT NULL, "
            "key TEXT NOT NULL, "
            "value TEXT, "
            "value_type TEXT NOT NULL, "
            "PRIMARY KEY (item_id, key))"
        ),
        (
            "CREATE INDEX IF NOT EXISTS idx_vector_entry_metadata_lookup "
            "ON vector_entry_metadata(key, value_type, value, item_id)"
        ),
    ):
        sqlite.execute(statement, query_type="vector")


def insert_vectors(
    sqlite: SQLiteEngine,
    rows: Sequence[tuple[int, Sequence[float]]],
) -> None:
    """Insert vector rows into the SQLite canonical store."""

    if not rows:
        return

    encoded_rows = []
    for item_id, vector in rows:
        blob = encode_vector_blob(vector)
        encoded_rows.append((item_id, len(vector), blob))

    sqlite.executemany(
        (
            "INSERT INTO vector_entries "
            "(item_id, dimensions, embedding) "
            "VALUES (?, ?, ?)"
        ),
        encoded_rows,
        query_type="vector",
    )


def upsert_vectors(
    sqlite: SQLiteEngine,
    rows: Sequence[tuple[int, Sequence[float]]],
) -> None:
    """Insert or replace vector rows in the SQLite canonical store."""

    if not rows:
        return

    encoded_rows = []
    for item_id, vector in rows:
        blob = encode_vector_blob(vector)
        encoded_rows.append((item_id, len(vector), blob))

    sqlite.executemany(
        (
            "INSERT INTO vector_entries "
            "(item_id, dimensions, embedding) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(item_id) DO UPDATE SET "
            "dimensions = excluded.dimensions, "
            "embedding = excluded.embedding"
        ),
        encoded_rows,
        query_type="vector",
    )


def load_vector_matrix(
    sqlite: SQLiteEngine,
) -> tuple[np.ndarray, np.ndarray]:
    """Load all SQLite-stored vectors into NumPy arrays."""

    result = sqlite.execute(
        (
            "SELECT item_id, dimensions, embedding "
            "FROM vector_entries "
            "ORDER BY item_id"
        ),
        query_type="vector",
    )
    if not result.rows:
        raise ValueError("HumemVector v0 could not load vectors: no rows found.")

    dimensions = int(result.rows[0][1])
    item_ids = np.empty(len(result.rows), dtype=np.int64)
    matrix = np.empty((len(result.rows), dimensions), dtype=np.float32)

    for index, (item_id, row_dimensions, blob) in enumerate(result.rows):
        if int(row_dimensions) != dimensions:
            raise ValueError(
                "HumemVector v0 requires one shared dimension per loaded vector "
                "set; got "
                f"{row_dimensions} and {dimensions}."
            )
        item_ids[index] = int(item_id)
        matrix[index] = decode_vector_blob(blob, dimension=dimensions)

    return item_ids, matrix


def upsert_vector_metadata(
    sqlite: SQLiteEngine,
    rows: Sequence[tuple[int, Mapping[str, VectorMetadataValue]]],
) -> None:
    """Insert or replace equality-filterable metadata for vector rows."""

    encoded_rows: list[tuple[int, str, str | None, str]] = []
    for item_id, metadata in rows:
        for key, value in metadata.items():
            encoded_value, value_type = _encode_metadata_value(value)
            encoded_rows.append((item_id, key, encoded_value, value_type))

    if not encoded_rows:
        return

    sqlite.executemany(
        (
            "INSERT INTO vector_entry_metadata "
            "(item_id, key, value, value_type) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(item_id, key) DO UPDATE SET "
            "value = excluded.value, "
            "value_type = excluded.value_type"
        ),
        encoded_rows,
        query_type="vector",
    )


def load_filtered_vector_item_ids(
    sqlite: SQLiteEngine,
    filters: Mapping[str, VectorMetadataValue],
) -> tuple[int, ...]:
    """Return item ids whose metadata matches all equality filters."""

    if not filters:
        return ()

    matched_ids: set[int] | None = None
    for key, value in filters.items():
        encoded_value, value_type = _encode_metadata_value(value)
        if encoded_value is None:
            result = sqlite.execute(
                (
                    "SELECT item_id FROM vector_entry_metadata "
                    "WHERE key = ? AND value_type = ? AND value IS NULL "
                    "ORDER BY item_id"
                ),
                params=(key, value_type),
                query_type="vector",
            )
        else:
            result = sqlite.execute(
                (
                    "SELECT item_id FROM vector_entry_metadata "
                    "WHERE key = ? AND value_type = ? AND value = ? "
                    "ORDER BY item_id"
                ),
                params=(key, value_type, encoded_value),
                query_type="vector",
            )

        item_ids = {int(row[0]) for row in result.rows}
        if matched_ids is None:
            matched_ids = item_ids
        else:
            matched_ids &= item_ids

        if not matched_ids:
            return ()

    return tuple(sorted(matched_ids or ()))


def encode_vector_blob(vector: Sequence[float]) -> bytes:
    """Encode a vector as a float32 SQLite blob."""

    array = _coerce_query(vector)
    return array.astype(np.float32, copy=False).tobytes()


def decode_vector_blob(blob: bytes, *, dimension: int) -> np.ndarray:
    """Decode a float32 SQLite blob into a detached NumPy vector."""

    array = np.frombuffer(blob, dtype=np.float32)
    if array.size != dimension:
        raise ValueError(
            "HumemVector v0 expected a float32 blob with "
            f"dimension {dimension}, got {array.size}."
        )
    return np.array(array, dtype=np.float32, copy=True)


@dataclass(slots=True)
class ExactVectorIndex:
    """Exact vector search over a contiguous NumPy matrix."""

    item_ids: np.ndarray
    matrix: np.ndarray
    metric: VectorMetric = "cosine"

    def __post_init__(self) -> None:
        """Validate and normalize the backing matrix after dataclass construction."""

        matrix = _coerce_matrix(self.matrix)
        item_ids = np.asarray(self.item_ids, dtype=object)
        if item_ids.ndim != 1:
            raise ValueError("HumemVector v0 item ids must be one-dimensional.")
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
        """Return the shared dimensionality of the indexed vectors."""

        return int(self.matrix.shape[1])

    def search(
        self,
        query: Sequence[float],
        *,
        top_k: int,
        candidate_indexes: Sequence[int] | np.ndarray | None = None,
    ) -> tuple[VectorSearchMatch, ...]:
        """Return the nearest results for one query vector."""

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
        """Quantize a float32 matrix into the scalar-int8 benchmark format."""

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
            item_ids=np.asarray(item_ids, dtype=object),
            quantized=quantized,
            scales=scales,
            metric=metric,
        )

    @property
    def dimensions(self) -> int:
        """Return the shared dimensionality of the quantized vectors."""

        return int(self.quantized.shape[1])

    def search(
        self,
        query: Sequence[float],
        *,
        top_k: int,
        candidate_indexes: Sequence[int] | np.ndarray | None = None,
    ) -> tuple[VectorSearchMatch, ...]:
        """Return approximate nearest results for one query vector."""

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
        VectorSearchMatch(item_id=item_ids[index], score=float(scores[index]))
        for index in sorted_indexes
    )


def _coerce_matrix(matrix: np.ndarray) -> np.ndarray:
    """Validate one matrix input and return it as contiguous float32."""

    array = np.asarray(matrix, dtype=np.float32)
    if array.ndim != 2:
        raise ValueError("HumemVector v0 matrices must be two-dimensional.")
    if array.shape[0] == 0 or array.shape[1] == 0:
        raise ValueError("HumemVector v0 matrices cannot be empty.")
    return np.ascontiguousarray(array)


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
