# Scripts

This directory holds repository utilities that are useful during development,
benchmarking, and release work.

Current layout:

- `benchmarks/`: performance and workload comparison scripts.
- `dev/`: local development helpers.
- `release/`: release preparation and packaging helpers.
- `fix_markdown.py`: normalize Markdown structure in the docs tree for MkDocs.

Current development helpers include the Docker-backed Cypher frontend regeneration
workflow in `dev/regenerate_cypher_frontend_docker.sh`.

These scripts are not part of the public `humemdb` runtime API.
