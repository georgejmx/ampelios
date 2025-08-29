import numpy as np

from os import path
from sklearn.cluster import MiniBatchKMeans
from numpy.typing import NDArray
from joblib import load, dump

from pipeline.types import TaskSignature, UserJourneyRow

from .postgres import assign_clusters, get_user_journeys, write_centroids


def extract_features(journey: UserJourneyRow) -> NDArray:
    session_count = len(journey['steps'])
    views_per_session = np.mean([ vec[0] for vec in journey['steps'] ])
    buys_per_session = np.mean([ vec[2] for vec in journey['steps'] ])
    purchase_ratio = buys_per_session / views_per_session if views_per_session else buys_per_session

    return np.concatenate([
        journey['state'],
        [float(session_count), views_per_session, purchase_ratio]
    ])


def parse_prediction_source(journeys: list[UserJourneyRow]) -> list[NDArray]:
    return [extract_features(j) for j in journeys]


async def main(model_path: str, num_clusters: int, fetch_batch_size: int) -> TaskSignature:
    journeys = await get_user_journeys(fetch_batch_size)
    journey_count = len(journeys)

    if journey_count == 0:
        return {
            'status': 'skipped',
            'message': 'Ran out of unclustered user journeys',
            'count': 0
        }

    model = None
    if path.exists(model_path):
        model = load(model_path)
    else:
        model = MiniBatchKMeans(
            n_clusters=num_clusters,
            batch_size=journey_count
        )

    user_ids = [j['user_id'] for j in journeys]
    prediction_source = parse_prediction_source(journeys)

    model.partial_fit(prediction_source)
    batch_labels = model.predict(prediction_source)
    centroids: NDArray = model.cluster_centers_ # type: ignore

    await write_centroids(centroids)

    # ensure clusters already set up for foreign key reference
    await assign_clusters(zip(user_ids, batch_labels))

    dump(model, model_path)
    return {
        'status': 'success',
        'message': f"{len(user_ids)} user journeys clustered",
        'count': len(user_ids)
    }
