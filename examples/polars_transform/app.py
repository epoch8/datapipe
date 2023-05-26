import logging
from dataclasses import dataclass
from io import BytesIO
from typing import IO, Any, Dict, List, Optional

import polars as pl

from datapipe.compute import (
    Catalog,
    ComputeStep,
    DatapipeApp,
    Pipeline,
    PipelineStep,
    Table,
)
from datapipe.core_steps import (
    BaseBatchTransformStep,
    UpdateExternalTable,
    safe_func_name,
)
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.store.database import DBConn
from datapipe.store.filedir import ItemStoreFileAdapter, TableStoreFiledir
from datapipe.types import DataDF, IndexDF, Labels, TransformResult

logger = logging.getLogger("app")


class PolarsParquet(ItemStoreFileAdapter):
    mode = "b"

    def load(self, f: IO) -> Dict[str, Any]:
        assert isinstance(f, BytesIO)
        return {"data": pl.read_parquet(f)}

    def dump(self, obj: Dict[str, Any], f: IO) -> None:
        data = obj["data"]

        assert isinstance(data, pl.DataFrame)
        # assert isinstance(f, BytesIO)
        data.write_parquet(f)  # type: ignore


@dataclass
class OneToOneTransform(PipelineStep):
    func: Any
    input: str
    output: str

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        return [
            OneToOneTransformStep(
                ds=ds,
                name=safe_func_name(self.func),
                func=self.func,
                input_dt=catalog.get_datatable(ds, self.input),
                output_dt=catalog.get_datatable(ds, self.output),
            )
        ]


class OneToOneTransformStep(BaseBatchTransformStep):
    def __init__(
        self,
        ds: DataStore,
        name: str,
        func: Any,
        input_dt: DataTable,
        output_dt: DataTable,
        transform_keys: Optional[List[str]] = None,
        chunk_size: int = 1000,
        labels: Optional[Labels] = None,
    ) -> None:
        super().__init__(
            ds=ds,
            name=name,
            input_dts=[input_dt],
            output_dts=[output_dt],
            transform_keys=transform_keys,
            chunk_size=chunk_size,
            labels=labels,
        )

        self.func = func

    def process_batch_dfs(
        self,
        ds: DataStore,
        idx: IndexDF,
        input_dfs: List[DataDF],
        run_config: Optional[RunConfig] = None,
    ) -> TransformResult:
        logger.debug(f"Idx to process: {idx.to_records()}")

        assert len(input_dfs) == 1
        input_df = input_dfs[0]

        output_df = input_df.assign(
            data=input_df["filepath"].apply(lambda x: self.func(pl.read_parquet(x)))
        )

        return output_df


ds = DataStore(DBConn("sqlite+pysqlite3:///db.sqlite"))


catalog = Catalog(
    {
        "input": Table(
            store=TableStoreFiledir(
                "input/{raw_input_chunk}.parquet",
                adapter=PolarsParquet(),
                add_filepath_column=True,
                read_data=False,
            )
        ),
        "output": Table(
            store=TableStoreFiledir(
                "output/{raw_input_chunk}.parquet",
                adapter=PolarsParquet(),
                add_filepath_column=True,
                read_data=False,
            )
        ),
    }
)

pipeline = Pipeline(
    [
        UpdateExternalTable(
            output="input",
            labels=[("stage", "update_external_table")],
        ),
        OneToOneTransform(
            func=lambda df: df,
            input="input",
            output="output",
        ),
    ]
)


app = DatapipeApp(ds=ds, catalog=catalog, pipeline=pipeline)
