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

## Writing rules for this phase

- keep the docs aligned with the actual `v0` runtime surface
- prefer clear boundaries over aspirational claims
- add examples only for tested behaviors