from prefect import flow, task

from pipeline.types import TaskSignature
from pipeline.logging import logger

from .save import main as save
from .save_sessions import main as save_sessions
from .load import main as load
from .cluster import main as cluster


SOURCE_FILEPATH = "./init-data/events.csv"
USER_BATCH_SIZE = 20000
BATCH_SIZE = 50000
CLUSTERING_MODEL_PATH = "./models/mini-batch-k-means.pkl"
CLUSTER_COUNT = 5


@task(retries=1, retry_delay_seconds=2)
async def save_raw_events() -> TaskSignature:
    return await save(SOURCE_FILEPATH)


@task(retries=1, retry_delay_seconds=2)
async def save_raw_events_sessions(batch_size: int) -> TaskSignature:
    return await save_sessions(batch_size)


@task(
    retries=3,
    retry_delay_seconds=5,
    retry_jitter_factor=0.2,
    timeout_seconds=90,
)
async def load_journeys(batch_size: int) -> TaskSignature:
    return await load(batch_size)


@task(
    retries=3,
    retry_delay_seconds=5,
    retry_jitter_factor=0.2,
    timeout_seconds=90,
)
async def cluster_journeys(batch_size: int) -> TaskSignature:
    return await cluster(CLUSTERING_MODEL_PATH, CLUSTER_COUNT, batch_size)


@flow
async def bulk_pipeline() -> None:
    save_result = await save_raw_events()
    logger.info(save_result["message"])
    if save_result["status"] != 'success':
        return

    while True:
        annotated_users = await save_raw_events_sessions(USER_BATCH_SIZE)
        if annotated_users["count"] == 0:
            break
    logger.info("Sessions annotated")

    # run all loading and clustering
    clustered_events = 0
    load_count = 0
    while clustered_events < save_result["count"]:
        load_result = await load_journeys(BATCH_SIZE)
        logger.info(load_result["message"])
        if load_result["status"] != 'success':
            break
        load_count += 1

        if load_count % 2 == 0:
            cluster_result = await cluster_journeys(BATCH_SIZE * 2)
            logger.info(cluster_result["message"])
            if cluster_result["status"] == 'error':
                break

        clustered_events += BATCH_SIZE

    logger.info("Bulk pipeline complete")


if __name__ == "__main__":
    import asyncio

    asyncio.run(bulk_pipeline())
