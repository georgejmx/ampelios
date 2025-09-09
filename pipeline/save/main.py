import csv
from os import path
from io import StringIO

from pipeline.db_utils import get_connection
from pipeline.types import TaskSignature


CSV_COLUMN_COUNT = 5


def _write_line(row: list[str], site_id: int) -> str:
    row = row[:CSV_COLUMN_COUNT]
    while len(row) < CSV_COLUMN_COUNT:
        row.append("")
    row.append(str(site_id))

    output = StringIO()
    csv.writer(output).writerow(row)
    return output.getvalue()


async def main(source_filepath: str, site_id: int) -> TaskSignature:
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
                await cur.execute(
                    "SELECT COUNT(*) FROM events WHERE site_id = %s;",
                    (site_id,)
                )
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
                        "COPY events(timestamp, visitorid, event, itemid, transactionid, site_id) FROM STDIN WITH CSV"
                    ) as copy:
                        for row in reader:
                            line = _write_line(row, site_id)
                            await copy.write(line)
                            count += 1

    return {
        'status': 'success',
        'message': f"{count} events loaded",
        'count': count
    }
