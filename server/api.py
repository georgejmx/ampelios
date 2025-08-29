import logging
import asyncio
from typing_extensions import Any
from fastapi import FastAPI
from fastapi.background import BackgroundTasks
from fastapi.responses import PlainTextResponse

from pipeline.dag import bulk_pipeline

from .view import get_clusters


api = FastAPI(
    title="ampelios-server",
    summary="Control point for the ampelios-pipeline",
)


@api.get("/health", response_class=PlainTextResponse)
def health_handler() -> str:
    return "Healthy"


@api.post("/trigger")
async def trigger_pipeline(background_tasks: BackgroundTasks) -> dict:
    background_tasks.add_task(lambda: asyncio.run(bulk_pipeline()))

    return {"status": "started"}


@api.get("/view")
async def view_handler() -> Any:
    clusters = await get_clusters()
    logging.info(f"{len(clusters)} retrieved")

    return clusters
