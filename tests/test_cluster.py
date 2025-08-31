import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
import numpy as np

from pipeline.types import UserJourneyRow
from pipeline.cluster import main as cluster


TEST_MODEL_PATH = "./tests/tmp/test-model.pkl"


@pytest_asyncio.fixture
async def blank_user_journeys_fixture() -> list[UserJourneyRow]:
    return []


@pytest.mark.asyncio
async def test_empty_data(blank_user_journeys_fixture):
    """Checks clustering endpoint when there are no processed rows"""

    with patch("pipeline.cluster.main.get_user_journeys", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = blank_user_journeys_fixture

        result = await cluster(TEST_MODEL_PATH, 5, 100)
        assert result['status'] == 'skipped'
        assert result['message'] == 'Ran out of unclustered user journeys'


@pytest_asyncio.fixture
async def user_journeys_fixture() -> list[UserJourneyRow]:
    return [
        {
            'steps': [np.array([3.0,1.0,0.0]), np.array([1.0,0.0,1.0])],
            'state': np.array([4.0, 1.0, 1.0]),
            'event_id': 99,
            'user_id': 91
        },
        {
            'steps': [np.array([4.0,0.0,1.0]), np.array([0.0,0.0,1.0])],
            'state': np.array([4.0, 0.0, 2.0]),
            'event_id': 199,
            'user_id': 92
        },
        {
            'steps': [np.array([1.0,0.0,0.0]), np.array([12.0,1.0,0.0]), np.array([0.0,1.0,0.0])],
            'state': np.array([13.0, 2.0, 0.0]),
            'event_id': 990,
            'user_id': 93
        }
    ]


@pytest.mark.asyncio
async def test_cluster(user_journeys_fixture):
    """Checks clustering logic"""
    NUM_CLUSTERS = 2

    with patch("pipeline.cluster.main.get_user_journeys", new_callable=AsyncMock) as mock_get, \
    patch("pipeline.cluster.main.assign_clusters", new_callable=AsyncMock) as mock_assign, \
    patch("pipeline.cluster.main.write_centroids", new_callable=AsyncMock) as mock_write:
        mock_get.return_value = user_journeys_fixture
        mock_assign.return_value = None
        mock_write.return_value = None

        result = await cluster(TEST_MODEL_PATH, NUM_CLUSTERS, 100)
        assert result['status'] == 'success'
        assert result['count'] == 3, "All three journeys should be clustered"

        assert mock_write.call_count == 1
        assign_args, _ = mock_write.call_args
        centroids_arg = assign_args[0]
        assert isinstance(centroids_arg, np.ndarray)
        assert centroids_arg.shape[0] == NUM_CLUSTERS, "There is a centroid per cluster"

        assert mock_assign.call_count == 1
        assign_args, _ = mock_assign.call_args
        for user_id, cluster_id in assign_args[0]:
            assert 90 < user_id < 100, "Original user ids are preserved"
            assert 0 <= cluster_id < 3, "Cluster ids are natural"


@pytest.mark.asyncio
async def test_cluster_initial_flow(user_journeys_fixture):
    """
    Checks clustering logic when `is_initial_flow=True`, meaning the model is nudged away from random clusters faster
    """
    NUM_CLUSTERS = 2

    with patch("pipeline.cluster.main.get_user_journeys", new_callable=AsyncMock) as mock_get, \
    patch("pipeline.cluster.main.assign_clusters", new_callable=AsyncMock) as mock_assign, \
    patch("pipeline.cluster.main.write_centroids", new_callable=AsyncMock) as mock_write:
        mock_get.return_value = user_journeys_fixture
        mock_assign.return_value = None
        mock_write.return_value = None

        result = await cluster(TEST_MODEL_PATH, NUM_CLUSTERS, 100, True)
        assert result['status'] == 'success'
        assign_args, _ = mock_assign.call_args
        for user_id, cluster_id in assign_args[0]:
            assert 90 < user_id < 100, "Original user ids are preserved"
            assert 0 <= cluster_id < 3, "Cluster ids are natural"
