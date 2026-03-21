# Licensing

## HumemDB license

HumemDB's own source code is licensed under MIT.

- Repository license file:
  [LICENSE]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/LICENSE)
- Package metadata:
  [pyproject.toml]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/pyproject.toml)

That MIT grant applies to HumemDB's code and documentation in this repository. It does
not automatically relicense third-party dependencies.

## Runtime libraries HumemDB relies on

The current runtime depends on a small set of explicit Python libraries and embedded
engines:

- `sqlite3` from the Python standard library for the canonical local write path
- `duckdb` for analytical reads
- `numpy` for the exact in-memory vector search baseline
- `sqlglot[c]` for the SQL translation layer
- `lancedb` for benchmark work and future indexed ANN paths
- `threadpoolctl` for thread-pool coordination

HumemDB intentionally presents those libraries as part of the implementation story
instead of pretending everything comes from one hidden backend.

## Third-party licenses

Third-party dependencies keep their own licenses and distribution terms.

- Installing HumemDB may download third-party wheels or source distributions.
- Some of those dependencies may include native code or platform-specific wheels.
- Those components remain under their upstream licenses.

For the concrete dependency set used by this project, see:

- [pyproject.toml]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/pyproject.toml)
- [uv.lock]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/uv.lock)

If you need a formal license review for a deployment, review the resolved dependency
set from the lockfile rather than only the top-level MIT license.
