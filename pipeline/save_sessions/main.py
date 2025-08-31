from pipeline.db_utils import get_connection
from pipeline.types import TaskSignature


SESSION_DURATION_IN_MINUTES = 20


async def main(batch_size: int) -> TaskSignature:
    query = """
    WITH visitor_batch AS (
        SELECT visitorid
        FROM events
        WHERE processed IS NOT TRUE
            AND sessionnumber IS NULL
        GROUP BY visitorid
        ORDER BY visitorid
        LIMIT %s
    ),
    diffs AS (
        SELECT
            e._id,
            e.visitorid,
            e.timestamp,
            LAG(e.timestamp) OVER (PARTITION BY e.visitorid ORDER BY e.timestamp) AS prev_time
        FROM events e
        JOIN visitor_batch vb ON e.visitorid = vb.visitorid
    ),
    session_flags AS (
        SELECT
            _id,
            visitorid,
            timestamp,
            CASE
                WHEN prev_time IS NULL THEN 1
                WHEN timestamp - prev_time <= %s * 60 * 1000 THEN 0
                ELSE 1
            END AS new_session_flag
        FROM diffs
    ),
    sessions AS (
        SELECT
            _id,
            visitorid,
            SUM(new_session_flag) OVER (
                PARTITION BY visitorid ORDER BY timestamp ROWS UNBOUNDED PRECEDING
            ) AS session_number
        FROM session_flags
    )
    UPDATE events e
    SET sessionnumber = s.session_number
    FROM sessions s
    WHERE e._id = s._id;
    """
    count = 0

    async with get_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    query,
                    (
                        batch_size,
                        SESSION_DURATION_IN_MINUTES,
                    )
                )
                count = cur.rowcount

    return {
        'status': 'success',
        'message': 'Sessions saved to event data',
        'count': count
    }
