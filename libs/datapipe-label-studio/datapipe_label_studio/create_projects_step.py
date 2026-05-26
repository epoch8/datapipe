import logging
from dataclasses import dataclass
from typing import List, Optional, Union

import pandas as pd
from datapipe.compute import (
    Catalog,
    ComputeStep,
    DataStore,
    Pipeline,
    PipelineStep,
    Table,
    build_compute,
)
from datapipe.executor import ExecutorConfig
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.types import PipelineInput, PipelineOutput, get_pipeline_input_name, get_pipeline_output_name, Labels
from label_studio_sdk import LabelStudio
from sqlalchemy import Column, Integer

from datapipe_label_studio.sdk_utils import (
    ensure_project,
    ensure_project_storages,
    get_ls_client,
)
from datapipe_label_studio.types import Buckets, GCSBucket, ProjectDict, S3Bucket
from datapipe_label_studio.utils import check_columns_are_in_table

logger = logging.getLogger("dataipipe_label_studio_lite")

__all__ = ["Buckets", "CreateLabelStudioProjects", "GCSBucket", "S3Bucket"]


@dataclass
class CreateLabelStudioProjects(PipelineStep):
    input__label_studio_project_setting: PipelineInput  # Input with project setting columns.
    output__label_studio_project: PipelineOutput

    ls_url: str
    api_key: str

    storages: Optional[List[Union[GCSBucket, S3Bucket]]] = None
    create_table: bool = False
    labels: Optional[Labels] = None
    executor_config: Optional[ExecutorConfig] = None

    def __post_init__(self):
        # lazy initialization
        self._ls_client: Optional[LabelStudio] = None
        self.labels = self.labels or []
        self.storages = self.storages or []

    @property
    def ls_client(self) -> LabelStudio:
        if self._ls_client is None:
            self._ls_client = get_ls_client(self.ls_url, self.api_key)
        return self._ls_client

    def create_project(
        self,
        project_identifier: Union[str, int],  # project_title or id
        project_label_config_at_create: str,
        project_description_at_create: str,
    ) -> ProjectDict:
        """
        При первом использовании ищет проект в LS по индентификатору,
        если его нет -- автоматически создаётся проект с нуля.
        """
        if isinstance(project_identifier, str):
            assert len(project_identifier) <= 50
        project = ensure_project(
            self.ls_client,
            project_identifier,
            project_label_config_at_create,
            project_description_at_create,
        )
        ensure_project_storages(
            self.ls_client,
            project["id"],
            self.storages,
        )
        return project

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        input_label_studio_project_setting_name = get_pipeline_input_name(self.input__label_studio_project_setting)
        output_label_studio_project_name = get_pipeline_output_name(self.output__label_studio_project)
        dt__input__label_studio_project_setting = ds.get_table(input_label_studio_project_setting_name)
        assert isinstance(dt__input__label_studio_project_setting.table_store, TableStoreDB)
        check_columns_are_in_table(
            ds,
            self.input__label_studio_project_setting,
            ["project_identifier", "project_label_config_at_create", "project_description_at_create"],
        )
        catalog.add_datatable(
            output_label_studio_project_name,
            Table(
                ds.get_or_create_table(
                    output_label_studio_project_name,
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=output_label_studio_project_name,
                        data_sql_schema=[
                            column
                            for column in dt__input__label_studio_project_setting.table_store.get_primary_schema()
                            if column.name in ["project_identifier"]
                        ]
                        + [
                            Column("project_id", Integer, primary_key=True),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )

        def create_projects(df__input__label_studio_project_setting: pd.DataFrame) -> pd.DataFrame:
            """
            Добавляет в LS новые задачи с заданными ключами.
            """
            project_identifier = df__input__label_studio_project_setting.iloc[0]["project_identifier"]
            project_label_config_at_create = df__input__label_studio_project_setting.iloc[0][
                "project_label_config_at_create"
            ]
            project_description_at_create = df__input__label_studio_project_setting.iloc[0][
                "project_description_at_create"
            ]
            project = self.create_project(
                project_identifier, project_label_config_at_create, project_description_at_create
            )
            df__input__label_studio_project_setting["project_id"] = project["id"]
            return df__input__label_studio_project_setting[["project_identifier", "project_id"]]

        pipeline = Pipeline(
            [
                BatchTransform(
                    func=create_projects,
                    inputs=[self.input__label_studio_project_setting],
                    outputs=[self.output__label_studio_project],
                    chunk_size=1,
                    labels=self.labels,
                    executor_config=self.executor_config,
                ),
            ]
        )
        return build_compute(ds, catalog, pipeline)
