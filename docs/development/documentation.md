# Documentation

HumemDB documentation uses MkDocs Material and mike versioning.

## Local preview

```bash
uv sync --group docs
uv run mkdocs serve
```

## Local build

```bash
uv run mkdocs build --strict
```

## Versioned publishing

The docs site is published into the shared `humemai-docs` repository under the
`/humemdb/` prefix so it can live beside the ArcadeDB Python docs on the same domain.

`mike` manages versioned documentation aliases such as `latest`.

## Writing rules

- keep the docs aligned with the actual `v0` runtime surface
- prefer clear boundaries over aspirational claims
- add examples only for tested behaviors

## Post-v0.1.0 docs backlog

Do not treat this as part of the minimum pre-release bar. It is the reminder list for
the first serious documentation pass after `v0.1.0` ships.

- add a real Python API reference for the stable package surface and the main `HumemDB`
  methods
- reorganize the guides so SQL, Cypher, vector, ingest, and transactions each have a
  clearer single home
- add a compact capability matrix for what `HumemSQL v0`, `HumemCypher v0`, and the
  direct-vector surface do and do not support
- tighten the README and quickstart into one coherent product narrative instead of
  release-hardening notes spread across pages
- expand examples only where the behavior is tested and likely to remain stable after
  `v0.1.0`
