# humemdb

HumemDB

## Python package

This repository is configured as a Python package named `humemdb`.

### Local build

```bash
source .venv/bin/activate
uv pip install build
python -m build
```

The built artifacts will be written to `dist/`.

### PyPI publishing

Trusted publishing is configured through GitHub Actions in [.github/workflows/publish-pypi.yml](.github/workflows/publish-pypi.yml).

Use these values when adding the pending publisher on PyPI:

- PyPI project name: `humemdb`
- Owner: `humemai`
- Repository name: `humemdb`
- Workflow name: `publish-pypi.yml`
- Environment name: `pypi`

The publish workflow builds the package on every release tag that matches `v*` and publishes it to PyPI through GitHub OIDC.

Example release tags:

- `v0.1.0`
- `v0.1.1`
- `v0.2.0rc1`

The package version is derived from the Git tag during the build.
