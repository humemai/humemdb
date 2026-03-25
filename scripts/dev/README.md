# Dev Scripts

This directory is reserved for local development helpers such as data seeding,
inspection tools, migration helpers, or one-off workflow automation.

Add scripts here when they support day-to-day engineering work but are not part of the
shipped library.

Current helpers:

- `regenerate_cypher_frontend_docker.sh`: run the same regeneration workflow inside a
  Docker container so local Java is not required.

Internal support files:

- `regenerate_cypher_frontend.py`: internal Python helper invoked by the Docker
  wrapper to regenerate or verify the checked-in ANTLR Python artifacts.

Docker regeneration uses the checked-in Dockerfile in this directory and currently pins
`eclipse-temurin:25-jdk`, which is the newest stable Java image tag verified from this
environment.
