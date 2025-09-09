from pipeline.logging import logger
from typing import ItemsView
from psycopg.rows import TupleRow

from pipeline.types import UserJourney
from pipeline.db_utils import (
    get_connection,
    postgres_vector_array_string_to_array,
    postgres_vector_string_to_array,
    vector_array_to_postgres_string,
)


async def fetch_user_journey_batch(batch_size: int, site_id: int) -> list[TupleRow]:
    query = """
    WITH unprocessed_sessions AS (
        SELECT MIN(timestamp) AS first_event_id,
            visitorid,
            sessionnumber
        FROM events
        WHERE processed IS NOT TRUE
            AND site_id = %(site_id)s
        GROUP BY visitorid, sessionnumber
        ORDER BY MIN(timestamp)
        LIMIT %(batch_size)s
    )
    SELECT e._id, e.visitorid, e.event, e.sessionnumber
    FROM events e
    JOIN unprocessed_sessions u
        ON e.visitorid = u.visitorid
        AND e.sessionnumber = u.sessionnumber
    WHERE e.processed IS NOT TRUE
        AND e.site_id = %(site_id)s
    ORDER BY e.visitorid, e.sessionnumber, e._id;
    """
    results = None

    async with get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, {
                "batch_size": batch_size,
                "site_id": site_id
            })
            results = await cur.fetchall()

    return results


async def batch_update_user_journeys(
    journeys: ItemsView[int, UserJourney],
    site_id: int
):
    """Batch update multiple user journeys for better performance"""
    if not journeys:
        return

    async with get_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                user_ids = [user_id for user_id, _ in journeys]
                existing_journeys: dict[str, UserJourney] = {}

                if user_ids:
                    placeholders = ",".join(["%s"] * len(user_ids))
                    query = f"""
                    SELECT user_id, state, paths
                    FROM user_journey
                    WHERE user_id IN ({placeholders}) AND site_id = %s
                    """

                    params = user_ids + [site_id]
                    await cur.execute(query, params)
                    results = await cur.fetchall()

                    for row in results:
                        existing_journeys[row[0]] = {
                            "state": postgres_vector_string_to_array(row[1]),
                            "steps": postgres_vector_array_string_to_array(row[2]),
                        }

                inserts = []

                for user_id, journey in journeys:
                    if user_id in existing_journeys:
                        existing = existing_journeys[user_id]
                        combined_paths = existing["steps"] + journey["steps"]
                        new_state = (
                            existing["state"] + journey["state"]
                        ).tolist()

                        paths_array = vector_array_to_postgres_string(combined_paths)

                        # set cluster_id to null as needs to be re-clustered
                        update_query = f"""
                        UPDATE user_journey
                        SET state = %s, paths = {paths_array}, cluster_id = null
                        WHERE user_id = %s AND site_id = %s
                        """
                        await cur.execute(
                            update_query,
                            (
                                new_state,
                                user_id,
                                site_id,
                            ),
                        )
                        logger.info(f"User with id {user_id} journey updated")
                    else:
                        state_list = (
                            journey["state"].tolist()
                            if hasattr(journey["state"], "tolist")
                            else journey["state"]
                        )
                        inserts.append((user_id, state_list, journey["steps"]))

                if len(inserts):
                    values_parts = []
                    for user_id, state, steps in inserts:
                        state_vector = f"'[{','.join([str(x) for x in state])}]'::vector"
                        paths_array = vector_array_to_postgres_string(steps)
                        values_parts.append(f"({user_id}, {site_id}, {state_vector}, {paths_array})")

                    insert_query = f"""
                    INSERT INTO user_journey (user_id, site_id, state, paths)
                    VALUES {",".join(values_parts)}
                    """
                    await cur.execute(insert_query)
                    logger.info(f"{len(inserts)} new user journeys created")


async def mark_events_processed(ids: list[int]):
    async with get_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute("CREATE TEMP TABLE tmp_ids (id BIGINT)")
                async with cur.copy("COPY tmp_ids (id) FROM STDIN") as copy:
                    for id in ids:
                        await copy.write_row([id])

                await cur.execute("""
                    UPDATE events e
                    SET processed = TRUE
                    FROM tmp_ids i
                    WHERE e._id = i.id
                """)
