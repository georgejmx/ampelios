from numpy.typing import NDArray
from typing_extensions import Iterator
from psycopg.rows import TupleRow

from pipeline.db_utils import (
    get_connection,
    postgres_vector_array_string_to_array,
    postgres_vector_string_to_array,
)
from pipeline.types import UserJourneyRow


def _parse_user_journey_row(result: TupleRow) -> UserJourneyRow:
    return {
        'event_id': result[0],
        'user_id': result[1],
        'state': postgres_vector_string_to_array(result[2]),
        'steps': postgres_vector_array_string_to_array(result[3]),
    }


async def get_user_journeys(batch_size: int, source_id: int) -> list[UserJourneyRow]:
    results: list[TupleRow] = []

    async with get_connection() as conn:
        async with conn.cursor() as cur:
            query = """
            SELECT j._id, j.user_id, j.state, j.paths
            FROM user_journey j
            WHERE j.cluster_id IS NULL AND j.source_id = %s
            ORDER BY _id
            LIMIT %s;
            """
            await cur.execute(query, (source_id, batch_size,))
            results = await cur.fetchall()

    return [ _parse_user_journey_row(result) for result in results]


async def assign_clusters(user_clusters: Iterator[tuple[int, int]], source_id: int):
    async with get_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute("CREATE TEMP TABLE tmp_clusters (user_id INT, cluster_id INT)")
                async with cur.copy("COPY tmp_clusters (user_id, cluster_id) FROM STDIN") as copy:
                    for user_id, cluster_id in user_clusters:
                        await copy.write_row([user_id, cluster_id])

                await cur.execute("""
                UPDATE user_journey j
                SET cluster_id = t.cluster_id
                FROM tmp_clusters t
                WHERE j.user_id = t.user_id AND j.source_id = %s;
                """, (source_id,))


async def write_centroids(centroids: NDArray, source_id: int):
    async with get_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                for i, centroid in enumerate(centroids):
                    await cur.execute("""
                    INSERT INTO cluster (source_id, cluster_id, centroid)
                    SELECT 1, %(cluster_id)s, %(centroid)s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM cluster
                        WHERE source_id = %(source_id)s
                            AND cluster_id = %(cluster_id)s
                    );
                    """, {
                        "source_id": source_id,
                        "cluster_id": i,
                        "centroid": centroid.tolist()
                    })
