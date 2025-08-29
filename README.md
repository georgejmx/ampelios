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

## Tooling

Unit tests and linting can now be run locally as follows;

```bash
pip install .[dev]
pytest
ruff check
```
