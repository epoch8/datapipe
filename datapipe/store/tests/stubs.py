import pandas as pd
import pytest
from sqlalchemy import Column, Integer, String

DATA_PARAMS = [
    pytest.param(
        pd.DataFrame(
            {
                "id": range(100),
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        [
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            Column("price", Integer),
        ],
        id="int_id",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id": [f"id_{i}" for i in range(100)],
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        [
            Column("id", String(100), primary_key=True),
            Column("name", String(100)),
            Column("price", Integer),
        ],
        id="str_id",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id_int": range(100),
                "id_str": [f"id_{i}" for i in range(100)],
                "name": [f"Product {i}" for i in range(100)],
                "price": [1000 + i for i in range(100)],
            }
        ),
        [
            Column("id_int", Integer, primary_key=True),
            Column("id_str", String(100), primary_key=True),
            Column("name", String(100)),
            Column("price", Integer),
        ],
        id="multi_id",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id1": [f"id_{i}" for i in range(1000)],
                "id2": [f"id_{i}" for i in range(1000)],
                "name": [f"Product {i}" for i in range(1000)],
                "price": [1000 + i for i in range(1000)],
            }
        ),
        [
            Column("id1", String(100), primary_key=True),
            Column("id2", String(100), primary_key=True),
            Column("name", String(100)),
            Column("price", Integer),
        ],
        id="double_id_1000_records",
    ),
    pytest.param(
        pd.DataFrame(
            {
                "id1": [f"id_{i}" for i in range(1000)],
                "id2": [f"id_{i}" for i in range(1000)],
                "id3": [f"id_{i}" for i in range(1000)],
                "name": [f"Product {i}" for i in range(1000)],
                "price": [1000 + i for i in range(1000)],
            }
        ),
        [
            Column("id1", String(100), primary_key=True),
            Column("id2", String(100), primary_key=True),
            Column("id3", String(100), primary_key=True),
            Column("name", String(100)),
            Column("price", Integer),
        ],
        id="triple_id_1000_records",
    ),
]
