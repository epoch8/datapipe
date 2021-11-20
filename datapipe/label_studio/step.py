from typing import List

from sqlalchemy import Integer
from sqlalchemy.sql.schema import Column

from datapipe.dsl import Catalog, PipelineStep, Table
from datapipe.compute import BatchTransformStep, ComputeStep
from datapipe.datatable import DataStore
from datapipe.store.database import DBConn, TableStoreDB


class LSModeration(PipelineStep):
    def __init__(
        self,
        ls_url,
        auth,
        project_identifier,
        project_label_config_at_create,
        annotations_column,

        temp_data_dbconn: DBConn,

        inputs,
        outputs,
        chunk_size,
    ):
        # TODO дописать

        assert len(inputs) == 1
        assert len(outputs) == 1

        self.input = inputs[0]
        self.output = outputs[0]

        self.temp_data_dbconn = temp_data_dbconn

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        # TODO дописать

        input_tbl = catalog.get_datatable(ds, self.input)
        output_tbl = catalog.get_datatable(ds, self.output)

        assert input_tbl.table_store.get_primary_schema() == output_tbl.table_store.get_primary_schema()

        publish_result_tbl = catalog.add_table(
            ds,
            # FIXME сделать автоименование
            'ls_publish_result',
            Table(
                TableStoreDB(
                    self.temp_data_dbconn,
                    name='ls_publish_result',
                    data_sql_schema=input_tbl.table_store.get_primary_schema() +
                    [
                        Column('task_id', Integer),
                    ]
                )
            )
        )

        return [
            BatchTransformStep(
                # FIXME сделать автоименование
                'publish_to_ls',
                input_dts=[input_tbl],
                output_dts=[publish_result_tbl],
                func=...
            ),
            BatchTransformStep(
                # FIXME сделать автоименование
                'update_ls_results',
                input_dts=[publish_result_tbl],
                output_dts=[output_tbl],
                func=...
            )
        ]