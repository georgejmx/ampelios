from pipeline.db_utils import get_connection
from pipeline.types import TaskSignature


async def main(source_id: int) -> TaskSignature:
    query = "DELETE FROM events WHERE source_id = %(source_id)s;"

    async with get_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(query, { "source_id": source_id })

    return {
        'status': 'success',
        'message': 'Truncated events for cleanup',
        'count': 0
    }
