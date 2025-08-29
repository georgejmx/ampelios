import numpy as np
import pytest
from pipeline.load.transforms import process_rows


@pytest.fixture
def events_fixture() -> list[tuple[int, int, str, int]]:
    return [
        (1, 1, "view", 1),
        (3, 1, "addtocart", 1),
        (7, 1, "view", 2),
        (5, 1, "transaction", 2),
        (4, 11, "view", 1),
        (6, 11, "view", 2),
        (2, 11, "addtocart", 2),
    ]


def test_process_rows_natural_sessions(events_fixture):
    journeys = process_rows(events_fixture)

    assert np.all(journeys[1]["steps"][0] == np.array([1.0, 1.0, 0.0]))
    assert np.all(journeys[1]["steps"][1] == np.array([1.0, 0.0, 1.0]))
    assert np.all(journeys[1]["state"] == np.array([2.0, 1.0, 1.0]))

    assert np.all(journeys[11]["steps"][0] == np.array([1.0, 0.0, 0.0]))
    assert np.all(journeys[11]["steps"][1] == np.array([1.0, 1.0, 0.0]))
    assert np.all(journeys[11]["state"] == np.array([2.0, 1.0, 0.0]))

    assert len(journeys[21]) == 0


@pytest.fixture
def events_fixture_no_session() -> list[tuple[int, int, str, int]]:
    return [
        (1, 1, "view", 1),
        (3, 1, "addtocart", 2),
        (5, 1, "transaction", 3),
        (2, 2, "addtocart", 1),
        (4, 3, "view", 1),
        (6, 3, "view", 2),
    ]


def test_process_rows_with_separate_sessions(events_fixture_no_session):
    journeys = process_rows(events_fixture_no_session)

    assert np.all(journeys[1]["steps"][0] == np.array([1.0, 0.0, 0.0]))
    assert np.all(journeys[2]["steps"][0] == np.array([0.0, 1.0, 0.0]))
    assert np.all(journeys[1]["steps"][-1] == np.array([0.0, 0.0, 1.0]))

    assert np.all(journeys[1]["state"] == np.array([1.0, 1.0, 1.0]))
    assert np.all(journeys[2]["state"] == np.array([0.0, 1.0, 0.0]))
    assert np.all(journeys[3]["state"] == np.array([2.0, 0.0, 0.0]))
