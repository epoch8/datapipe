import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, cast

import pandas as pd
from sqlalchemy import Column
import mgclient

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


class MemgraphStore(TableStore):
    """Memgraph graph database interface

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
                "Unsupported primary-key configuration for MemgraphStore. "
                "Expected either (node_id, node_type) or "
                "(from_node_id, to_node_id, edge_label)."
            )

        self._con = mgclient.connect(**self.connection_kwargs)
        self._con.autocommit = True

    def __getstate__(self) -> Dict[str, Any]:
        return {
            "connection_kwargs": self.connection_kwargs,
            "data_sql_schema": self.data_sql_schema,
        }

    def __setstate__(self, state: Dict[str, Any]) -> None:
        MemgraphStore.__init__(
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

    # Data manipulation helpers

    def _execute_many(self, queries: List[Tuple[str, Dict[str, Any]]]) -> None:
        """Execute a batch of Cypher queries in a single transaction."""
        if not queries:
            return

        cur = self._con.cursor()
        try:
            for q, p in queries:
                cur.execute(q, p)
            # Commit only if autocommit is disabled
            if not getattr(self._con, "autocommit", True):
                self._con.commit()
        finally:
            cur.close()

    # CRUD operations

    def insert_rows(self, df: DataDF) -> None:
        if df.empty:
            return

        queries: List[Tuple[str, Dict[str, Any]]] = []

        if self._mode == "node":  # nodes
            for row in df.to_dict(orient="records"):
                node_id = row["node_id"]
                node_type = row["node_type"]
                attrs = row.get("attributes", {}) or {}

                cypher = f"MERGE (n:`{node_type}` {{id: $node_id}}) SET n += $props"
                queries.append((cypher, {"node_id": node_id, "props": attrs}))

        else:  # edges
            for row in df.to_dict(orient="records"):
                from_id = row["from_node_id"]
                to_id = row["to_node_id"]
                from_type = row["from_node_type"]
                to_type = row["to_node_type"]
                edge_label = row["edge_label"]
                attrs = row.get("attributes", {}) or {}

                cypher = (
                    f"MERGE (from:`{from_type}` {{id: $from_id}}) "
                    f"MERGE (to:`{to_type}` {{id: $to_id}}) "
                    f"MERGE (from)-[r:`{edge_label}`]->(to) "
                    f"SET r += $props"
                )

                queries.append(
                    (
                        cypher,
                        {
                            "from_id": from_id,
                            "to_id": to_id,
                            "props": attrs,
                        },
                    )
                )

        self._execute_many(queries)

    def update_rows(self, df: DataDF) -> None:
        self.insert_rows(df)

    def delete_rows(self, idx: IndexDF) -> None:
        if idx.empty:
            return

        queries: List[Tuple[str, Dict[str, Any]]] = []

        if self._mode == "node":  # nodes
            for row in idx[self._pk_columns].to_dict(orient="records"):
                node_id = row["node_id"]
                node_type = row["node_type"]

                cypher = f"MATCH (n:`{node_type}` {{id: $node_id}}) DETACH DELETE n"
                queries.append((cypher, {"node_id": node_id}))

        else:  # edges
            for row in idx[self._pk_columns].to_dict(orient="records"):
                from_id = row["from_node_id"]
                to_id = row["to_node_id"]
                edge_label = row["edge_label"]
                from_type = row["from_node_type"]
                to_type = row["to_node_type"]

                cypher = (
                    f"MATCH (from:`{from_type}` {{id: $from_id}})-[r:`{edge_label}`]->"
                    f"(to:`{to_type}` {{id: $to_id}}) DELETE r"
                )
                queries.append((cypher, {"from_id": from_id, "to_id": to_id}))

        self._execute_many(queries)

    # todo: read_rows is oversimplified for now, since MemgraphStore is intended as a sink, rather than a source.
    def read_rows(self, idx: Optional[IndexDF] = None) -> DataDF:
        if idx is None:
            raise NotImplementedError("MemgraphStore does not support reading full table yet. Provide an IndexDF.")
        if idx.empty:
            return pd.DataFrame(columns=self._pk_columns)

        records: List[Dict[str, Any]] = []
        cur = self._con.cursor()
        try:
            if self._mode == "node":
                for row in idx[self._pk_columns].to_dict(orient="records"):
                    node_id = row["node_id"]
                    node_type = row["node_type"]

                    cypher = (
                        f"MATCH (n:`{node_type}` {{id: $node_id}}) "
                        f"RETURN n.id AS node_id, '{node_type}' AS node_type, properties(n) AS attributes"
                    )
                    cur.execute(cypher, {"node_id": node_id})
                    for rec in cur.fetchall():
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
                    q = (
                        f"MATCH (from:`{row['from_node_type']}` {{id: $from_id}})-"
                        f"[r:`{row['edge_label']}`]->(to:`{row['to_node_type']}` {{id: $to_id}}) "
                        f"RETURN from.id AS from_node_id, to.id AS to_node_id, '{row['from_node_type']}' AS from_node_type, "
                        f"'{row['to_node_type']}' AS to_node_type, '{row['edge_label']}' AS edge_label, properties(r) AS attributes"
                    )
                    cur.execute(q, {"from_id": row["from_node_id"], "to_id": row["to_node_id"]})
                    for rec in cur.fetchall():
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
        finally:
            cur.close()

        if records:
            return pd.DataFrame.from_records(records)
        else:
            return pd.DataFrame(columns=[c.name for c in self.data_sql_schema])
