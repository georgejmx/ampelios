# Local setup

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
