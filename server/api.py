import logging
import sys
import asyncio
from dataclasses import dataclass
from typing_extensions import Any, Optional
from fastapi import FastAPI
from fastapi.background import BackgroundTasks
from fastapi.responses import PlainTextResponse

from pipeline.dag import bulk_pipeline

from .view import get_clusters


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

api = FastAPI(
    title="ampelios-server",
    summary="Control point for the ampelios-pipeline",
)


@api.get("/health", response_class=PlainTextResponse)
def health_handler() -> str:
    return "Healthy"


@dataclass
class TriggerRequest:
    source_id: int
    events_path: str
    is_initial_flow: Optional[bool] = None


@api.post("/trigger")
async def trigger_handler(
    req: TriggerRequest,
    background_tasks: BackgroundTasks
) -> dict:
    background_tasks.add_task(lambda: asyncio.run(bulk_pipeline(
        req.source_id,
        req.events_path,
        req.is_initial_flow or False,
    )))

    return {"status": "started"}


@api.get("/view")
async def view_handler(verbose: bool = False) -> Any:
    clusters = await get_clusters(verbose)
    logging.info(f"{len(clusters)} retrieved")

    return clusters
