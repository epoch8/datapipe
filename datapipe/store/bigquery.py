from typing import Iterator, List, Optional, Union

from datapipe.store.table_store import TableStore
from datapipe.run_config import RunConfig
from datapipe.types import DataDF, DataSchema, IndexDF, MetaSchema, data_to_index

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

from sqlalchemy import Column


# sqlalchemy types to GoogleSQL data types
SCHEMA_MAPPING = {
    "ARRAY": "ARRAY",
    "BIGINT": "INT64",
    "BINARY": "BYTES",
    "BLOB": "STRING",
    "BOOLEAN": "BOOL",
    "CHAR": "STRING",
    "CLOB": "STRING",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "DECIMAL": "FLOAT64",
    "DOUBLE": "FLOAT64",
    "DOUBLE_PRECISION": "FLOAT64",
    "FLOAT": "FLOAT64",
    "INT": "INT64",
    "JSON": "JSON",
    "INTEGER": "INT64",
    "NCHAR": "STRING",
    "NVARCHAR": "STRING",
    "NUMERIC": "NUMERIC",
    "REAL": "FLOAT64",
    "SMALLINT": "INT64",
    "TEXT": "STRING",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "UUID": "STRING",
    "VARBINARY": "BYTES",
    "VARCHAR": "STRING",
}


def is_table_exists(client: bigquery.Client, table) -> bool:
    try:
        client.get_table(table)
        return True
    except NotFound:
        return False


def is_dataset_exists(
    client: bigquery.Client, dataset: bigquery.Client.dataset
) -> bool:
    try:
        client.get_dataset(dataset)
        return True
    except NotFound:
        return False


class BQClient:
    def __init__(self, service_account_file):
        self.bq_client = bigquery.Client(
            credentials=service_account.Credentials.from_service_account_file(
                service_account_file
            )
        )

    def __call__(self, *args, **kwds):
        return self.bq_client(*args, **kwds)


class TableStoreBQ(TableStore):
    def __init__(
        self,
        bq_client: bigquery.Client,
        name: str,
        data_sql_schema: List[Column],
        dataset_id: str,
        table_id: str,
    ) -> None:
        self.bq_client = bq_client
        self.name = name
        self.data_sql_schema = data_sql_schema

        dataset_ref = self.bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(str(table_id))

        if not is_dataset_exists(self.bq_client, dataset_ref):
            self.bq_client.create_dataset(dataset_ref)

        prim_keys = [
            column for column in self.data_sql_schema if column.primary_key
        ]
        value_cols = [
            column for column in self.data_sql_schema if not column.primary_key
        ]

        schema_prim_keys = [bigquery.SchemaField(column.name, SCHEMA_MAPPING.get(f"{column.type}", "STRING"), mode="REQUIRED") for column in prim_keys]
        schema_value_cols = [bigquery.SchemaField(column.name, SCHEMA_MAPPING.get(f"{column.type}", "STRING"), mode="NULLABLE") for column in value_cols]

        self.table = bigquery.Table(
            table_ref=table_ref,
            schema=schema_prim_keys+schema_value_cols
        )
        
        self.table.clustering_fields = [column.name for column in self.data_sql_schema if column.primary_key][0:4]
        self.table = self.bq_client.create_table(self.table, exists_ok=True)

        self.job_config = bigquery.LoadJobConfig()
        self.job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND


    def get_primary_schema(self) -> DataSchema:
        raise NotImplementedError


    def get_meta_schema(self) -> MetaSchema:
        raise NotImplementedError


    def get_schema(self) -> DataSchema:
        raise NotImplementedError


    @property
    def primary_keys(self) -> List[str]:
        return [i.name for i in self.get_primary_schema()]


    def delete_rows(self, idx: IndexDF) -> None:
        dml = f"DELETE FROM `{self.table.project}`.`{self.table.dataset_id}`.`{self.table.table_id}` WHERE TRUE;"
        self.bq_client.query(dml)


    def insert_rows(self, df: DataDF) -> None:
        self.bq_client.load_table_from_dataframe(
            df,
            self.table,
            job_config=self.job_config,
        ).result()


    def update_rows(self, df: DataDF) -> None:
        if df.empty:
            return

        self.delete_rows(data_to_index(df, self.primary_keys))
        self.insert_rows(df)


    def read_rows(self, idx: Optional[IndexDF] = None) -> DataDF:
        sql = f"SELECT * FROM `{self.table.project}`.`{self.table.dataset_id}`.`{self.table.table_id}`;"
        result = self.bq_client.query_and_wait(sql)
        df = result.to_dataframe()

        return df


    def read_rows_meta_pseudo_df(
        self, chunksize: int = 1000, run_config: Optional[RunConfig] = None
    ) -> Iterator[DataDF]:
        # FIXME сделать честную чанкированную реализацию во всех сторах
        yield self.read_rows()
