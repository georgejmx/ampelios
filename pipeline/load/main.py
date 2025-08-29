from pipeline.logging import logger as logging
from pipeline.types import TaskSignature

from .postgres import (
    batch_update_user_journeys,
    fetch_user_journey_batch,
    mark_events_processed,
)
from .transforms import process_rows


async def main(batch_size: int) -> TaskSignature:
    raw_journeys_batch = await fetch_user_journey_batch(batch_size)
    load_count = len(raw_journeys_batch)

    if load_count == 0:
        logging.warning("No events available for processing")
        return {
            'status': 'skipped',
            'message': 'Ran out of unprocessed events',
            'count': 0
        }
    else:
        logging.info(f"{load_count} user journeys extracted")

    user_journeys = process_rows(raw_journeys_batch)
    await batch_update_user_journeys(user_journeys.items())

    await mark_events_processed([e[0] for e in raw_journeys_batch])
    logging.info(f"{load_count} events marked as processed")

    return {
        'status': 'success',
        'message': f"{len(user_journeys)} user journeys written",
        'count': len(user_journeys)
    }
