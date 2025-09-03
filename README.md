# Ampelios

_Generate meaningful clusters from browser session data as paths_

Service with the following endpoints to load a retail dataset from source and apply clustering

## About

### Pipeline

The dag for doing all clustering steps from source data

- **load_journeys** loads user session paths from the *events* table in Postgres
- **cluster_journeys** performs clustering and stores labels

### Server

Microservice for setting up cluster runs and viewing output data

- `/trigger` start pipeline run
- `/view` shows output clusters

## Docker setup

```bash
cd infra
docker compose up --build
```

## Local setup

1. Setup the virtual environment with up to date dependencies

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install .
pip-compile --output-file=requirements.txt pyproject.toml
```

2. Run pipeline locally

```bash
cp .env.example .env
python3.12 -m pipeline.dag
```

Note: The `./models` directory is only used for local development and is persisted on your host machine.

In production, models are stored in a **named Docker volume** mounted at `/app/models`.
This ensures that each site’s model (e.g. `k-means-{site-id}-{cluster_count}.pkl`) is persisted across flows and container restarts, while still isolated from the host filesystem.

## Tooling

Unit tests and linting can now be run locally as follows;

```bash
pip install .[dev]
pytest
ruff check
```
