import pytest
import pytest_asyncio
import numpy as np
from unittest.mock import AsyncMock, patch, MagicMock

from pipeline.load.postgres import batch_update_user_journeys


@pytest_asyncio.fixture
async def journeys_fixture():
    return [
        (1, {
            'steps': [np.array([3.0,1.0,0.0]), np.array([1.0,0.0,1.0])],
            'state': np.array([4.0, 1.0, 1.0]),
            'event_id': 1,
            'user_id': 1
        }),
        (3, {
            'steps': [np.array([6.0,0.0,2.0]), np.array([0.0,1.0,0.0])],
            'state': np.array([6.0, 1.0, 2.0]),
            'event_id': 2,
            'user_id': 2
        }),
    ]


@pytest.mark.asyncio
async def test_batch_update_inserts(journeys_fixture):
    """Check INSERTs are correctly applied when updating user state"""
    mock_cursor_cm = MagicMock()
    mock_cursor = AsyncMock()
    mock_cursor_cm.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor_cm.__aexit__ = AsyncMock(return_value=None)

    mock_transaction_cm = MagicMock()
    mock_transaction_cm.__aenter__ = AsyncMock(return_value=None)
    mock_transaction_cm.__aexit__ = AsyncMock(return_value=None)

    mock_conn = MagicMock()
    mock_conn.transaction.return_value = mock_transaction_cm
    mock_conn.cursor.return_value = mock_cursor_cm

    mock_conn_cm = MagicMock()
    mock_conn_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn_cm.__aexit__ = AsyncMock(return_value=None)

    with patch("pipeline.load.postgres.get_connection", return_value=mock_conn_cm):
        await batch_update_user_journeys(journeys_fixture)

    insert_calls = [c for c in mock_cursor.execute.call_args_list if c[0][0].strip().startswith("INSERT")]

    assert len(insert_calls) == 1, "INSERT is batched"
    insert_sql = insert_calls[0][0][0]
    assert "1" in insert_sql, "INSERT should include new user 1"
    assert "2" in insert_sql, "INSERT should include new user 3"
