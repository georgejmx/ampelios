from numpy.typing import NDArray
from typing import TypedDict, Literal

TaskStatus = Literal["success", "error", "skipped"]

class TaskSignature(TypedDict):
    status: TaskStatus
    message: str
    count: int

class UserJourney(TypedDict):
    steps: list[NDArray]
    state: NDArray

class UserJourneyRow(UserJourney):
    event_id: int
    user_id: int

class ClusterRow(TypedDict):
    id: int
    users: list[int]
    centroid: list[int]
