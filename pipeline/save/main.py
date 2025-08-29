import csv
from os import path

from pipeline.db_utils import get_connection
from pipeline.types import TaskSignature


async def main(source_filepath: str) -> TaskSignature:
    count = 0

    if not path.exists(source_filepath):
        return {
            'status': 'error',
            'message': f"{source_filepath} cannot be parsed",
            'count': 0
        }

    async with get_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM events;")
                result = await cur.fetchone()
                if not result:
                    return {
                        'status': 'error',
                        'message': 'Unable to parse results from database',
                        'count': 0
                    }

                if result[0] >= 1:
                    return {
                        'status': 'skipped',
                        'message': 'Already parsed results',
                        'count': result[0]
                    }

                with open(source_filepath, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    next(reader)

                    async with cur.copy(
                        "COPY events(timestamp, visitorid, event, itemid, transactionid) FROM STDIN WITH CSV HEADER"
                    ) as copy:
                        for row in reader:
                            line = ",".join(row) + "\n"
                            await copy.write(line)
                            count += 1

    return {
        'status': 'success',
        'message': 'Events saved',
        'count': count
    }
