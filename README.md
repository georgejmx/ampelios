# Ampelios

_Generate meaningful clusters from browser session data represented as paths_

As an e-commerce site, have you ever wanted automatic, behavior-based segmentation of your customers without manually defining segments?

Ampelios is a data pipeline and microservice that takes raw browser event data and produces clusters useful for:
- Identifying high-value users
- Finding moderately likely buyers
- Filtering out users with no purchase potential
- Tracking behavioural change over time

---

## Sample Input / Output

### Input (CSV)

```csv
timestamp,visitorid,event,transactionid
433221332117,257598,view,
433221078505,158091,addtocart,
433221999827,111014,view,
433193500981,122686,transaction,11
```

### Output (JSON)

```json
[
  {
    "id": 0,
    "users": [0, 6, 7, 13, 22],
    "centroid": [
      1.5968122,
      0.03344515,
      0.008624283,
      1.1687442,
      1.3817313,
      0.0044482
    ]
  },
  {
    "id": 1,
    "users": [302, 588, 904, 914, 159],
    "centroid": [
      0.9970998,
      0.012757817,
      0.0033784304,
      1.0007713,
      0.9967424,
      0.0033321506
    ]
  },
  {
    "id": 2,
    "users": [1722, 1879, 2019, 2114, 2194],
    "centroid": [
      13.875828,
      0.6650781,
      0.22663489,
      2.8512046,
      7.307564,
      0.012281195
    ]
  },
  {
    "id": 3,
    "users": [2, 37, 51, 54, 64],
    "centroid": [
      5.205463,
      0.10490605,
      0.029401531,
      1.4163188,
      4.3710628,
      0.0054033287
    ]
  },
  {
    "id": 4,
    "users": [1, 3, 4, 5, 8],
    "centroid": [
      1,
      0,
      0,
      1,
      1,
      0
    ]
  }
]
```

_Note that the users field has been truncated for easy viewing_

---

## Quickstart

This project is designed to work with a Kafka consumer for incremental real-time data, but for initial development and proof-of-concept you can load from a CSV dataset.

### Sample Data

Use the [Retailrocket recommender dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset).
The dataset is licensed under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).

Please comply with the dataset license; it is only intended as a demo for Ampelios.
**Note:** The dataset is not included in this repository.

### Steps

1. Download the dataset and place `events.csv` in `init-data/events.csv` relative to the project root.

2. Start Ampelios with Docker:

cd infra
docker compose up

3. Call the trigger endpoint as defined in the [Bruno collection](./docs/bruno) to start a pipeline run.
_Note: With the full dataset (~2.7M rows), it currently takes ~10 minutes. In production with incremental data, the pipeline could be polled every minute for smooth flow 🌊._

4. Call the view clusters endpoint (also in the Bruno collection) to see results.

---

## How It Works

### Pipeline

The DAG handles all clustering steps from source data;

- **save_raw_events**: loads raw CSV events into the pipeline
- **save_raw_events_sessions**: annotates events with session numbers for analysis
- **load_journeys**: loads user session paths from the `events` table in Postgres
- **cluster_journeys**: performs clustering storing updated labels and centroids

### Server

A microservice wraps the pipeline as a Python module:

- `/trigger` → start a pipeline run
- `/view` → view output clusters

### Features Used for Clustering

Each user journey is represented with:

- Final state vector: page views, add-to-carts, transactions
- Path length (number of sessions)
- Views per session
- Purchase ratio (buys per session / views per session)

Clustering is performed using K-Means (MiniBatchKMeans) over these features.

---

## Documentation

- **[Endpoints & Pipeline Jobs](./docs/units.md)** – API and job reference
- **[Architecture](./docs/architecture.md)** – System design and data flow
- **[Local setup](./docs/local-setup.md)** - How to run stuff locally

---

## License & Attribution

- **Code:** [Apache 2.0](./LICENSE)
- **Sample Dataset:** CC BY-NC-SA 4.0 (attribution required, non-commercial, share-alike)

Please respect dataset licensing when using it for examples or testing.
