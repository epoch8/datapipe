from dataclasses import dataclass
from typing import Any, Dict, List, Optional, cast

import pandas as pd
from neo4j import Driver, GraphDatabase
from sqlalchemy import Column

from datapipe.store.table_store import TableStore, TableStoreCaps
from datapipe.types import DataDF, DataSchema, IndexDF, MetaSchema


@dataclass(frozen=True)
class _NodePK:
    node_id: str
    node_type: str


@dataclass(frozen=True)
class _EdgePK:
    from_node_id: str
    to_node_id: str
    edge_label: str


class Neo4JStore(TableStore):
    """Neo4J graph database interface

    Tested on Memgraph databases

    Requires two calls (two tables) - nodes and edges. Store mode is determined by edges pkeys:
    1. Node mode – primary keys (node_id, node_type).
    2. Edge mode – primary keys (from_node_id, to_node_id, from_node_type, to_node_type, edge_label).
    """

    caps = TableStoreCaps(
        supports_delete=True,
        supports_get_schema=True,
        supports_read_all_rows=False,
        supports_read_nonexistent_rows=False,
        supports_read_meta_pseudo_df=False,
    )

    def __init__(
        self,
        connection_kwargs: Dict[str, Any],
        data_sql_schema: List[Column],
    ) -> None:
        super().__init__()

        self.connection_kwargs = connection_kwargs
        self.data_sql_schema = data_sql_schema

        self._pk_columns: List[str] = [c.name for c in self.data_sql_schema if c.primary_key]
        self._non_pk_columns: List[str] = [c.name for c in self.data_sql_schema if not c.primary_key]

        # Detect mode from PK set
        node_pk = {"node_id", "node_type"}
        edge_pk = {"from_node_id", "to_node_id", "from_node_type", "to_node_type", "edge_label"}

        if set(self._pk_columns) == node_pk:
            self._mode: str = "node"
        elif set(self._pk_columns) == edge_pk:
            self._mode = "edge"
        else:
            raise ValueError(
                "Unsupported primary-key configuration for Neo4JStore. "
                "Expected either (node_id, node_type) or "
                "(from_node_id, to_node_id, from_node_type, to_node_type, edge_label)."
            )

        self._driver: Driver = GraphDatabase.driver(**self.connection_kwargs)

    def __getstate__(self) -> Dict[str, Any]:
        return {
            "connection_kwargs": self.connection_kwargs,
            "data_sql_schema": self.data_sql_schema,
        }

    def __setstate__(self, state: Dict[str, Any]) -> None:
        Neo4JStore.__init__(
            self,
            connection_kwargs=state["connection_kwargs"],
            data_sql_schema=state["data_sql_schema"],
        )

    # TableStore interface

    def get_schema(self) -> DataSchema:
        return self.data_sql_schema

    def get_primary_schema(self) -> DataSchema:
        return [c for c in self.data_sql_schema if c.primary_key]

    def get_meta_schema(self) -> MetaSchema:
        # todo
        return []

    def _run_query(self, cypher: str, params: Dict[str, Any]):
        with self._driver.session() as session:
            result = session.run(cypher, **params)
            return [tuple(rec.values()) for rec in result]

    # CRUD operations

    def insert_rows(self, df: DataDF) -> None:
        if df.empty:
            return

        if self._mode == "node":
            for node_type, gdf in df.groupby("node_type"):  # group by label for single-label bulk queries
                rows = [
                    {"id": r["node_id"], "props": r.get("attributes", {}) or {}} for r in gdf.to_dict(orient="records")
                ]
                cypher = f"UNWIND $rows AS row\nMERGE (n:`{node_type}` {{id: row.id}})\nSET   n += row.props"
                self._run_query(cypher, {"rows": rows})

        else:  # unwind edges in the same fashion
            for keys, gdf in df.groupby(["from_node_type", "to_node_type", "edge_label"]):
                from_type, to_type, edge_label = keys
                rows = [
                    {
                        "from_id": r["from_node_id"],
                        "to_id": r["to_node_id"],
                        "props": r.get("attributes", {}) or {},
                    }
                    for r in gdf.to_dict(orient="records")
                ]
                cypher = (
                    f"UNWIND $rows AS row\n"
                    f"MERGE (from:`{from_type}` {{id: row.from_id}})\n"
                    f"MERGE (to:`{to_type}` {{id: row.to_id}})\n"
                    f"MERGE (from)-[r:`{edge_label}`]->(to)\n"
                    f"SET r += row.props"
                )
                self._run_query(cypher, {"rows": rows})

    def update_rows(self, df: DataDF) -> None:
        self.insert_rows(df)

    def delete_rows(self, idx: IndexDF) -> None:
        if idx.empty:
            return

        if self._mode == "node":
            for node_type, gdf in idx.groupby("node_type"):
                ids = gdf["node_id"].tolist()
                cypher = f"UNWIND $ids AS id\nMATCH (n:`{node_type}` {{id: id}}) DETACH DELETE n"
                self._run_query(cypher, {"ids": ids})
        else:
            for keys, gdf in idx.groupby(["from_node_type", "to_node_type", "edge_label"]):
                from_type, to_type, edge_label = keys
                rows = [{"from_id": r["from_node_id"], "to_id": r["to_node_id"]} for r in gdf.to_dict(orient="records")]
                cypher = (
                    f"UNWIND $rows AS row\n"
                    f"MATCH (from:`{from_type}` {{id: row.from_id}})-[r:`{edge_label}`]->"
                    f"(to:`{to_type}` {{id: row.to_id}}) DELETE r"
                )
                self._run_query(cypher, {"rows": rows})

    # todo: read_rows is oversimplified for now, since Neo4JStore is intended as a sink, rather than a source.
    def read_rows(self, idx: Optional[IndexDF] = None) -> DataDF:
        if idx is None:
            raise NotImplementedError("Neo4JStore does not support reading full table")
        if idx.empty:
            return pd.DataFrame(columns=self._pk_columns)

        records: List[Dict[str, Any]] = []
        session = self._driver.session()
        if self._mode == "node":
            for row in idx[self._pk_columns].to_dict(orient="records"):
                node_id = row["node_id"]
                node_type = row["node_type"]

                cypher = (
                    f"UNWIND $ids AS id "
                    f"MATCH (n:`{node_type}` {{id: id}}) "
                    f"RETURN n.id AS node_id, '{node_type}' AS node_type, properties(n) AS attributes"
                )
                recs = session.run(cypher, ids=[node_id])
                for rec in recs:
                    records.append(
                        {
                            "node_id": rec[0],
                            "node_type": rec[1],
                            "attributes": cast(Dict[str, Any], rec[2]),
                        }
                    )

        else:  # edges
            required_cols = self._pk_columns

            for row in idx[required_cols].to_dict(orient="records"):
                cypher = (
                    f"UNWIND $rows AS row "
                    f"MATCH (from:`{row['from_node_type']}` {{id: row.from_id}})-"
                    f"[r:`{row['edge_label']}`]->(to:`{row['to_node_type']}` {{id: row.to_id}}) "
                    f"RETURN from.id AS from_node_id, to.id AS to_node_id, '{row['from_node_type']}' AS from_node_type, "
                    f"'{row['to_node_type']}' AS to_node_type, '{row['edge_label']}' AS edge_label, properties(r) AS attributes"
                )
                recs = session.run(
                    cypher,
                    rows=[{"from_id": row["from_node_id"], "to_id": row["to_node_id"]}],
                )
                for rec in recs:
                    records.append(
                        {
                            "from_node_id": rec[0],
                            "to_node_id": rec[1],
                            "from_node_type": rec[2],
                            "to_node_type": rec[3],
                            "edge_label": rec[4],
                            "attributes": cast(Dict[str, Any], rec[5]),
                        }
                    )

        if records:
            return pd.DataFrame.from_records(records)
        else:
            return pd.DataFrame(columns=[c.name for c in self.data_sql_schema])
