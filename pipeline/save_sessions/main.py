from pipeline.db_utils import get_connection
from pipeline.types import TaskSignature

async def main() -> TaskSignature:
    query = """
    WITH diffs AS (
        SELECT
            _id,
            visitorid,
            timestamp,
            LAG(timestamp) OVER (PARTITION BY visitorid ORDER BY timestamp) AS prev_time
        FROM events
    ),
    session_flags AS (
        SELECT
            _id,
            visitorid,
            timestamp,
            CASE
                WHEN prev_time IS NULL THEN 1
                WHEN timestamp - prev_time <= 20 * 60 * 1000 THEN 0
                ELSE 1
            END AS new_session_flag
        FROM diffs
    ),
    sessions AS (
        SELECT
            _id,
            visitorid,
            SUM(new_session_flag) OVER (PARTITION BY visitorid ORDER BY timestamp ROWS UNBOUNDED PRECEDING) AS session_number
        FROM session_flags
    )
    UPDATE events e
    SET sessionnumber = s.session_number
    FROM sessions s
    WHERE e._id = s._id AND e.processed IS NOT TRUE;
    """

    async with get_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(query)

    return {
        'status': 'success',
        'message': 'Sessions saved to event data',
        'count': 0
    }
