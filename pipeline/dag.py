from prefect import flow, task

from pipeline.types import TaskSignature
from pipeline.logging import logger

from .save import main as save
from .save_sessions import main as save_sessions
from .load import main as load
from .cluster import main as cluster


USER_BATCH_SIZE = 20000
BATCH_SIZE = 50000
CLUSTERING_MODEL_DIR = "./models"
CLUSTER_COUNT = 5


@task(retries=1, retry_delay_seconds=2)
async def save_raw_events_csv(source_filepath: str, source_id: int) -> TaskSignature:
    return await save(source_filepath, source_id)


@task(retries=1, retry_delay_seconds=2)
async def save_raw_events_sessions(batch_size: int, source_id: int) -> TaskSignature:
    return await save_sessions(batch_size, source_id)


@task(
    retries=3,
    retry_delay_seconds=5,
    retry_jitter_factor=0.2,
    timeout_seconds=90,
)
async def load_journeys(batch_size: int, source_id: int) -> TaskSignature:
    return await load(batch_size, source_id)


@task(
    retries=3,
    retry_delay_seconds=5,
    retry_jitter_factor=0.2,
    timeout_seconds=90,
)
async def cluster_journeys(
    batch_size: int,
    source_id: int,
    is_initial_flow: bool
) -> TaskSignature:
    return await cluster(
        source_id,
        CLUSTERING_MODEL_DIR,
        CLUSTER_COUNT,
        batch_size,
        is_initial_flow
    )


@flow
async def bulk_pipeline(source_id: int, events_path: str, is_initial_flow: bool) -> None:
    save_result = await save_raw_events_csv(events_path, source_id)
    logger.info(save_result["message"])
    if save_result["status"] != 'success':
        return

    while True:
        annotated_users = await save_raw_events_sessions(USER_BATCH_SIZE, source_id)
        if annotated_users["count"] == 0:
            break
    logger.info("Sessions annotated")

    # run all loading and clustering
    clustered_events = 0
    load_count = 0
    while clustered_events < save_result["count"]:
        load_result = await load_journeys(BATCH_SIZE, source_id)
        logger.info(load_result["message"])
        if load_result["status"] != 'success':
            break
        load_count += 1

        if load_count % 2 == 0:
            cluster_result = await cluster_journeys(
                BATCH_SIZE * 2,
                source_id,
                is_initial_flow
            )
            logger.info(cluster_result["message"])
            if cluster_result["status"] == 'error':
                break

        clustered_events += BATCH_SIZE

    logger.info("Bulk pipeline complete")


if __name__ == "__main__":
    import asyncio

    asyncio.run(bulk_pipeline(1, "./init-data/events.csv", True))
