# Endpoints and Pipeline Jobs

## Endpoints

### `POST /trigger`

A way of starting the Python DAG pipeline from a single control point. This sets off the pipeline as a background task

#### Body Parameters

- _source_id (int)_; a unique number to identify this site within **ampelios**
- _events_path (string)_; path to the source dataset to use as input. In future events could be sent in the body or passed directly as a file attachment
- _is_initial_flow (boolean)_; wether this is the first run for this site/dataset. Used to include warming up of the machine learning model for better cluster convergence in the inital run

### `GET /view`

Retrieve the output of the pipeline

#### Query Parameters

- _source_id (boolean)_; which source to view clusters for
- _verbose (boolean)_; when false as by default, just show then number of users in each cluster. When set to true, show all the user ids in each cluster

## Jobs

### Save Raw Events CSV

Loads a CSV file of events into the **events** table in PostgreSQL

### Save Raw Events Sessions

In batches, annotate each raw event with a session number. A new session number is generated when it has been over 20 minutes since the first interaction

### Load journeys

First parse a batch of events from the **events** table

Convert each session into a list of 3D movement vectors
	- Starts at `(0, 0, 0)`
	- Page view `(+1, 0, 0)`
	- Add to cart `(0, +1, 0)`
	- Transaction `(0, 0, +1)` _in production to get better results this will be `(0, 0, +value)` once the say kafka consumer has been setup to have transaction values inline_

⚡ _Has a single conjoined path per session and then a total path for the entire user journey_

Store this in the **user_journey** PostgreSQL table with each row being the total user journey across all sessions for each user represented as
- State; the final (X,Y,Z) coordinates for the journey
- Paths; the total (X,Y,Z) for each user's session

### Cluster journeys

Incrementally cluster user journeys using MiniBatch K-Means.

- Loads an existing model for the site/cluster count or initializes a new one with k-means++.
- Partially fits the model on each batch of journeys, predicts cluster labels, and assigns them to users.
- Persists centroids in the **cluster** PostgreSQL table and saves the updated model for reuse.

⚡ _Designed for incremental updates so new journeys can be clustered without retraining from scratch_
