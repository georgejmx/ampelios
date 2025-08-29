from ast import literal_eval
from contextlib import asynccontextmanager
from os import getenv

import numpy as np
from numpy.typing import NDArray
from psycopg import AsyncConnection


def _get_connection_string() -> str:
    return f"dbname={getenv('POSTGRES_DB')} user={getenv('POSTGRES_USER')} password={getenv('POSTGRES_PASSWORD')} host={getenv('POSTGRES_HOST')}"


@asynccontextmanager
async def get_connection():
    conn = await AsyncConnection.connect(_get_connection_string())
    try:
        yield conn
    finally:
        await conn.close()


def vector_array_to_postgres_string(arr_of_vectors: list[NDArray]) -> str:
    """Convert array of vectors to SQL literal for pgvector array"""
    if not arr_of_vectors or len(arr_of_vectors) == 0:
        return "ARRAY[]::vector[]"
    vectors = [f"'[{','.join([
        str(x) for x in vec.tolist()
    ])}]'::vector" for vec in arr_of_vectors]
    return f"ARRAY[{','.join(vectors)}]" """""" """"""


def postgres_vector_string_to_list(vector_str: str) -> list:
    return literal_eval(vector_str)


def postgres_vector_string_to_array(vector_str: str) -> NDArray:
    return np.array(postgres_vector_string_to_list(vector_str), dtype=float)


def postgres_vector_array_string_to_array(vector_array_str: str) -> list[NDArray]:
    inner = vector_array_str.strip("{}")
    parts = inner.split('","')

    parts = [p.strip('"') for p in parts]
    return [np.array(literal_eval(p)) for p in parts]
