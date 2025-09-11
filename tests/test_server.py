from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient

from server.api import api

client = TestClient(api)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.text == "Healthy"


def test_trigger_handler():
    with patch("server.api.bulk_pipeline", new_callable=AsyncMock):
        payload = {
            "source_id": 1,
            "events_path": "./init-data/events.csv",
            "is_initial_flow": True
        }
        response = client.post("/trigger", json=payload)

    assert response.status_code == 200
    assert response.json() == {"detail": "started"}


def test_view_handler():
    mock_clusters = [{"cluster_id": 0, "users": [1, 2], "centroid": [0.1, 0.2, 0.3]}]

    with patch("server.api.get_clusters", new_callable=AsyncMock) as mock_get_clusters:
        mock_get_clusters.return_value = mock_clusters
        response = client.get("/view?source_id=42&verbose=true")

    assert response.status_code == 200
    assert response.json() == mock_clusters
    mock_get_clusters.assert_called_once_with(42, True)
