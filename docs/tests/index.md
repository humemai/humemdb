# Tests

HumemDB keeps its shipped test suite in the repository `tests/` directory, and the
pages in this section describe those exact Python files.

Like the examples section, each page is a companion to real source in the repository
rather than a disconnected hand-written snippet.

Current test files documented here:

- [test_db.py](test_db.md): end-to-end coverage for routing, SQL translation,
  transactions, Cypher, and vector integration through the public `HumemDB` API.
- [test_vector.py](test_vector.md): focused coverage for vector encoding, index
  behavior, SQLite vector storage helpers, and SQL/Cypher-owned vector flows.

Run the tests locally from the repository root:

```bash
uv run python -m unittest tests.test_db tests.test_vector
```

Or run a single test module directly:

```bash
uv run python -m unittest tests.test_db
uv run python -m unittest tests.test_vector
```