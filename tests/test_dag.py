import pytest
from unittest.mock import AsyncMock, patch

from pipeline.dag import bulk_pipeline


@pytest.mark.asyncio
async def test_bulk_pipeline_success():
    """Check full pipeline; save, save sessions, load, cluster"""

    with patch("pipeline.dag.save_raw_events_csv", new_callable=AsyncMock) as mock_save, \
         patch("pipeline.dag.save_raw_events_sessions", new_callable=AsyncMock) as mock_save_sessions, \
         patch("pipeline.dag.load_journeys", new_callable=AsyncMock) as mock_load, \
         patch("pipeline.dag.cluster_journeys", new_callable=AsyncMock) as mock_cluster, \
         patch("pipeline.dag.logger") as mock_log:

        mock_save.return_value = {"status": "success", "message": "Events saved", "count": 50004}

        mock_save_sessions.side_effect = [
            {"status": "success", "message": "Sessions saved to event data", "count": 19000},
            {"status": "success", "message": "Sessions saved to event data", "count": 0}
        ]

        mock_load.side_effect = [
            {"status": "success", "message": "23452 user journeys written", "count": 23452},
            {"status": "success", "message": "4 user journeys written", "count": 4},
        ]

        mock_cluster.return_value = {"status": "success", "message": "23456 user journeys clustered", "count": 23456}

        await bulk_pipeline.fn(42, "./init-data/events.csv", False)

        mock_log.info.assert_called()
        assert mock_save.await_count == 1
        assert mock_save_sessions.await_count == 2
        assert mock_load.await_count == 2
        assert mock_cluster.await_count == 1


@pytest.mark.asyncio
async def test_bulk_pipeline_save_failure():
    """Check that the pipeline does not run when saving events fails"""

    with patch("pipeline.dag.save_raw_events_csv", new_callable=AsyncMock) as mock_save, \
         patch("pipeline.dag.save_raw_events_sessions", new_callable=AsyncMock) as mock_save_sessions, \
         patch("pipeline.dag.load_journeys", new_callable=AsyncMock) as mock_load, \
         patch("pipeline.dag.cluster_journeys", new_callable=AsyncMock) as mock_cluster:

        mock_save.return_value = {"status": "error", "message": "Unable to parse results from database", "count": 0}

        await bulk_pipeline.fn(66, "./init-data/events.csv", False)

        mock_save_sessions.assert_not_awaited()
        mock_load.assert_not_awaited()
        mock_cluster.assert_not_awaited()
