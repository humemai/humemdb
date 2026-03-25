# Generated Parser Artifacts

This directory holds the checked-in ANTLR Python artifacts for HumemDB's internal
Cypher frontend.

These files should be treated as owned build artifacts for HumemDB, not as the public
API boundary.

They are intended to be checked in and shipped in the Python wheel so runtime parsing
does not require Java or ANTLR code generation on the target machine.

Preferred regeneration path:

`scripts/dev/regenerate_cypher_frontend_docker.sh`

That wrapper builds a small dev-only container, mounts the repository, and runs the
existing Python regeneration helper inside it. The container currently pins
`eclipse-temurin:25-jdk`, so local Java is not required.

CI also verifies that these generated files are current by running the Docker-backed
`--check` path.
