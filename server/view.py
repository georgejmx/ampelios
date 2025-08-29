from psycopg.rows import TupleRow
from pipeline.db_utils import get_connection, postgres_vector_string_to_list
from pipeline.types import ClusterRow


def _parse_cluster_row(result: TupleRow) -> ClusterRow:
    return {
        'id': result[0],
        'users': result[1],
        'centroid': postgres_vector_string_to_list(result[2])
    }


async def get_clusters() -> list[ClusterRow]:
    cluster_data: list[TupleRow] = []

    async with get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
            SELECT
                j.cluster_id,
                json_agg(j.user_id ORDER BY j.user_id) as users,
                c.centroid
            FROM user_journey j
            JOIN cluster c
                ON j.site_id = c.site_id
                AND j.cluster_id = c.cluster_id
            WHERE j.site_id = 1
            GROUP BY j.cluster_id, c.centroid
            ORDER BY j.cluster_id;
            """)
            cluster_data = await cur.fetchall()

    return [ _parse_cluster_row(item) for item in cluster_data]
