from typing import Callable, Union, Tuple, List, Optional
from dataclasses import dataclass

from sqlalchemy import Column

from datapipe.types import DataDF
from datapipe.compute import PipelineStep, DataStore, Catalog, DatatableTransformStep
from datapipe.core_steps import update_external_table, batch_transform_wrapper
from datapipe.label_studio.store import TableStoreLabelStudio


# TODO autonaming
@dataclass
class LabelStudioStep(PipelineStep):
    input: str
    output: str

    ls_url: str
    auth: Union[Tuple[str, str], str]
    project_identifier: Union[str, int]  # project_title or id
    data_sql_schema: List[Column]
    tasks_id_column: Optional[str] = 'tasks_id'
    annotations_column: Optional[str] = 'annotations'
    predictions_column: Optional[str] = None
    preannotations_column: Optional[str] = None
    project_label_config_at_create: str = ''
    project_description_at_create: str = ""

    input_convert_func: Callable[[DataDF], DataDF] = lambda df: df
    output_convert_func: Callable[[DataDF], DataDF] = lambda df: df

    update_chunk_size: int = 1000

    page_chunk_size: int = 100
    tqdm_disable: bool = True

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[DatatableTransformStep]:
        input_dt = catalog.get_datatable(ds, self.input)

        output_dt = ds.get_or_create_table(
            self.output,
            TableStoreLabelStudio(
                ls_url=self.ls_url,
                auth=self.auth,
                project_identifier=self.project_identifier,
                data_sql_schema=self.data_sql_schema,
                tasks_id_column=self.tasks_id_column,
                annotations_column=self.annotations_column,
                preannotations_column=self.preannotations_column,
                predictions_column=self.preannotations_column,
                project_label_config_at_create=self.project_label_config_at_create,
                project_description_at_create=self.project_description_at_create,
                page_chunk_size=self.page_chunk_size,
                tqdm_disable=self.tqdm_disable,
            )
        )

        def load_data_to_ls_func(ds, input_dts, output_dts, run_config):
            return batch_transform_wrapper(
                func=self.input_convert_func,
                ds=ds,
                input_dts=input_dts,
                output_dts=output_dts,
                chunksize=self.update_chunk_size,
                run_config=run_config,
            )

        def update_ls_dt_meta_func(ds, input_dts, output_dts, run_config):
            return update_external_table(
                ds=ds,
                table=output_dts[0],
                run_config=run_config,
            )

        return [
            DatatableTransformStep(
                name='load_data_to_ls',
                input_dts=[input_dt],
                output_dts=[output_dt],
                func=load_data_to_ls_func,
            ),
            DatatableTransformStep(
                name='update_ls_dt_meta',
                input_dts=[],
                output_dts=[output_dt],
                func=update_ls_dt_meta_func,
            ),
        ]
