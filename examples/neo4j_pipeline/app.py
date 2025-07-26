from __future__ import annotations

import random
from pathlib import Path
from typing import Generator, List, cast

import pandas as pd
from sqlalchemy import JSON, Column, String

from datapipe.compute import Catalog, DatapipeApp, Pipeline, Table
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.store.database import DBConn
from datapipe.store.neo4j import Neo4JStore
from datapipe.types import IndexDF

bolt_kwargs = {
    "uri": "bolt://localhost:7687",  # neo4j:7687 if run in container
    "auth": ("neo4j", "password"),
}

# Store
node_schema = [
    Column("node_id", String, primary_key=True),
    Column("node_type", String, primary_key=True),
    Column("attributes", JSON),
]

node_store = Neo4JStore(connection_kwargs=bolt_kwargs, data_sql_schema=node_schema)
node_table = Table(name="node_data", store=node_store)

edge_schema = [
    Column("from_node_id", String, primary_key=True),
    Column("to_node_id", String, primary_key=True),
    Column("from_node_type", String, primary_key=True),
    Column("to_node_type", String, primary_key=True),
    Column("edge_label", String, primary_key=True),
    Column("attributes", JSON),
]

edge_store = Neo4JStore(connection_kwargs=bolt_kwargs, data_sql_schema=edge_schema)
edge_table = Table(name="edge_data", store=edge_store)


# Steps


def gen_nodes() -> Generator[pd.DataFrame, None, None]:
    rows: List[dict] = [
        {"node_id": "N0", "node_type": "person", "attributes": {"name": "Bob"}},
        {"node_id": "N1", "node_type": "person", "attributes": {"name": "Alice"}},
        {"node_id": "N2", "node_type": "person", "attributes": {"name": "TSheyd"}},
        {"node_id": "N3", "node_type": "company", "attributes": {"name": "Epoch8"}},
    ]
    yield pd.DataFrame(rows)


def gen_edges() -> Generator[pd.DataFrame, None, None]:
    rows: List[dict] = [
        {
            "from_node_id": "N0",
            "to_node_id": "N3",
            "from_node_type": "person",
            "to_node_type": "company",
            "edge_label": "PERSON_works_at_COMPANY",
            "attributes": {"weight": random.random()},
        },
        {
            "from_node_id": "N1",
            "to_node_id": "N3",
            "from_node_type": "person",
            "to_node_type": "company",
            "edge_label": "PERSON_works_at_COMPANY",
            "attributes": {"weight": random.random()},
        },
        {
            "from_node_id": "N2",
            "to_node_id": "N3",
            "from_node_type": "person",
            "to_node_type": "company",
            "edge_label": "PERSON_works_at_COMPANY",
            "attributes": {"weight": random.random()},
        },
        {
            "from_node_id": "N3",
            "to_node_id": "N0",
            "from_node_type": "company",
            "to_node_type": "person",
            "edge_label": "COMPANY_EMPLOYS_PERSON",
            "attributes": {"weight": random.random()},
        },
        {
            "from_node_id": "N3",
            "to_node_id": "N1",
            "from_node_type": "company",
            "to_node_type": "person",
            "edge_label": "COMPANY_EMPLOYS_PERSON",
            "attributes": {"weight": random.random()},
        },
        {
            "from_node_id": "N3",
            "to_node_id": "N2",
            "from_node_type": "company",
            "to_node_type": "person",
            "edge_label": "COMPANY_EMPLOYS_PERSON",
            "attributes": {"weight": random.random()},
        },
        {
            "from_node_id": "N0",
            "to_node_id": "N1",
            "from_node_type": "person",
            "to_node_type": "person",
            "edge_label": "PERSON_follows_PERSON",
            "attributes": {"weight": random.random()},
        },
    ]
    yield pd.DataFrame(rows)


pipeline = Pipeline(
    [
        BatchGenerate(gen_nodes, outputs=[node_table]),
        BatchGenerate(gen_edges, outputs=[edge_table]),
    ]
)


def main() -> None:
    meta_path = Path(__file__).parent / "db.sqlite"
    ds = DataStore(DBConn(f"sqlite:///{meta_path}"), create_meta_table=True)

    app = DatapipeApp(ds=ds, catalog=Catalog({}), pipeline=pipeline)

    from datapipe.compute import run_steps

    run_steps(app.ds, app.steps)

    # simple read
    idx: IndexDF = cast(IndexDF, pd.DataFrame({"node_id": ["N0"], "node_type": ["person"]}))
    print(node_table.store.read_rows(idx))


if __name__ == "__main__":
    main()
