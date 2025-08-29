from collections import defaultdict

import numpy as np
from typing import DefaultDict

from pipeline.types import UserJourney


def process_row(
    event: tuple[int, int, str, int],
    journeys: dict[int, UserJourney],
    user_sessions: dict[int, int]
) -> None:
    _, user_id, name, session_num = event

    next_step = False
    if user_sessions[user_id]:
        if session_num > user_sessions[user_id]:
            next_step = True
            user_sessions[user_id] = session_num
    else:
        next_step = True
        user_sessions[user_id] = session_num

    increments = {
        "view": np.array([1.0, 0.0, 0.0]),
        "addtocart": np.array([0.0, 1.0, 0.0]),
        "transaction": np.array([0.0, 0.0, 1.0]),
    }

    if len(journeys[user_id]):
        if next_step:
            journeys[user_id]["steps"].append(increments[name])
        else:
            current_session_steps = journeys[user_id]["steps"][-1]
            journeys[user_id]["steps"][-1] = current_session_steps + increments[name]

        existing_state = journeys[user_id]["state"]
        journeys[user_id]["state"] = existing_state + increments[name]
    else:
        journeys[user_id] = {
            "steps": [increments[name]],
            "state": np.array(increments[name])
        }


def process_rows(
    events: list[tuple[int, int, str, int]],
) -> dict[int, UserJourney]:
    journeys: DefaultDict[int, UserJourney] = defaultdict(dict) # type: ignore
    user_session: DefaultDict[int, int] = defaultdict(int)

    [process_row(event, journeys, user_session) for event in events]
    return journeys
