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
from typing import Any, Literal, Sequence, TypeAlias

import numpy as np

from .engines import SQLiteEngine

VectorMetric: TypeAlias = Literal["cosine", "dot", "l2"]


@dataclass(frozen=True, slots=True)
class VectorSearchMatch:
    """One nearest-neighbor result from an exact or quantized vector search."""

    item_id: Any
    score: float


def ensure_vector_schema(sqlite: SQLiteEngine) -> None:
    """Create the initial SQLite-backed vector storage tables if needed."""

    sqlite.execute(
        (
            "CREATE TABLE IF NOT EXISTS vector_entries ("
            "item_id INTEGER PRIMARY KEY, "
            "collection TEXT NOT NULL, "
            "bucket INTEGER NOT NULL, "
            "dimensions INTEGER NOT NULL, "
            "embedding BLOB NOT NULL)"
        ),
        query_type="vector",
    )
    sqlite.execute(
        (
            "CREATE INDEX IF NOT EXISTS idx_vector_entries_collection_bucket "
            "ON vector_entries (collection, bucket, item_id)"
        ),
        query_type="vector",
    )


def insert_vectors(
    sqlite: SQLiteEngine,
    rows: Sequence[tuple[int, str, int, Sequence[float]]],
) -> None:
    """Insert vector rows into the SQLite canonical store."""

    if not rows:
        return

    encoded_rows = []
    for item_id, collection, bucket, vector in rows:
        blob = encode_vector_blob(vector)
        encoded_rows.append((item_id, collection, bucket, len(vector), blob))

    sqlite.executemany(
        (
            "INSERT INTO vector_entries "
            "(item_id, collection, bucket, dimensions, embedding) "
            "VALUES (?, ?, ?, ?, ?)"
        ),
        encoded_rows,
        query_type="vector",
    )


def load_vector_matrix(
    sqlite: SQLiteEngine,
    *,
    collection: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Load one collection of SQLite-stored vectors into NumPy arrays."""

    result = sqlite.execute(
        (
            "SELECT item_id, bucket, dimensions, embedding "
            "FROM vector_entries "
            "WHERE collection = ? "
            "ORDER BY item_id"
        ),
        params=(collection,),
        query_type="vector",
    )
    if not result.rows:
        raise ValueError(
            f"HumemVector v0 could not load collection {collection!r}: no rows found."
        )

    dimensions = int(result.rows[0][2])
    item_ids = np.empty(len(result.rows), dtype=np.int64)
    buckets = np.empty(len(result.rows), dtype=np.int32)
    matrix = np.empty((len(result.rows), dimensions), dtype=np.float32)

    for index, (item_id, bucket, row_dimensions, blob) in enumerate(result.rows):
        if int(row_dimensions) != dimensions:
            raise ValueError(
                "HumemVector v0 requires one dimension per collection; got "
                f"{row_dimensions} and {dimensions}."
            )
        item_ids[index] = int(item_id)
        buckets[index] = int(bucket)
        matrix[index] = decode_vector_blob(blob, dimension=dimensions)

    return item_ids, buckets, matrix


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
            scores = np.asarray(matrix @ query_array, dtype=np.float32)
        elif self.metric == "dot":
            scores = np.asarray(matrix @ query_array, dtype=np.float32)
        else:
            diff = matrix - query_array
            scores = -np.sum(diff * diff, axis=1, dtype=np.float32)

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
            scores = np.asarray(quantized @ scaled_query, dtype=np.float32)
        else:
            approx_matrix = quantized.astype(np.float32) * self.scales
            diff = approx_matrix - query_array
            scores = -np.sum(diff * diff, axis=1, dtype=np.float32)

        return _build_matches(item_ids, scores, top_k)


def _build_matches(
    item_ids: np.ndarray,
    scores: np.ndarray,
    top_k: int,
) -> tuple[VectorSearchMatch, ...]:
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
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    safe_norms = np.where(norms > 0.0, norms, 1.0)
    return np.ascontiguousarray(matrix / safe_norms, dtype=np.float32)


def _normalize_query(query: np.ndarray) -> np.ndarray:
    norm = float(np.linalg.norm(query))
    if norm == 0.0:
        raise ValueError("HumemVector v0 cosine queries cannot be zero vectors.")
    return np.ascontiguousarray(query / norm, dtype=np.float32)


def _validated_top_k(top_k: int, *, total: int) -> int:
    if top_k < 1:
        raise ValueError("HumemVector v0 top_k must be at least 1.")
    return min(top_k, total)


def _coerce_candidate_indexes(
    candidate_indexes: Sequence[int] | np.ndarray,
    *,
    total: int,
) -> np.ndarray:
    indexes = np.asarray(candidate_indexes, dtype=np.int64)
    if indexes.ndim != 1:
        raise ValueError("HumemVector v0 candidate indexes must be one-dimensional.")
    if indexes.size == 0:
        raise ValueError("HumemVector v0 candidate indexes cannot be empty.")
    if np.any(indexes < 0) or np.any(indexes >= total):
        raise ValueError("HumemVector v0 candidate indexes are out of range.")
    return indexes
