from dataclasses import dataclass
from typing import Iterator, List, Tuple, Union, Optional, cast
from datapipe.label_studio.session import LabelStudioSession
from datapipe.store.table_store import TableStore
from datapipe.types import (
    DataDF, DataSchema, IndexDF, data_to_index, index_difference, index_intersection, index_to_data
)

import pandas as pd

from sqlalchemy.sql.sqltypes import String

from tqdm import tqdm

from sqlalchemy import Column


@dataclass
class TableStoreLabelStudio(TableStore):
    def __init__(
        self,
        ls_url: str,
        auth: Union[Tuple[str, str], str],
        project_title: str,
        project_label_config: str,
        data_sql_schema: List[Column],
        preannotations: Optional[str] = None,
        predictions: Optional[str] = None,
        project_description: str = "",
        page_chunk_size: int = 100
    ) -> None:
        self.ls_url = ls_url
        self.auth = auth
        self.project_title = project_title
        self.project_description = project_description
        self.project_label_config = project_label_config
        assert [column not in ["tasks_id", "annotations"] for column in data_sql_schema], (
            "Columns 'tasks_id' and 'annotations' are reserved."
        )
        self.data_sql_schema = data_sql_schema + [Column("tasks_id", String()), Column("annotations", String())]
        self.preannotations = preannotations
        self.predictions = predictions
        self.label_studio_session = LabelStudioSession(
            ls_url=ls_url,
            auth=auth
        )
        self.page_chunk_size = page_chunk_size
        self.get_or_create_project(raise_exception=False)

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self.data_sql_schema if column.primary_key]

    @property
    def data_columns(self) -> DataSchema:
        return [
            column.name for column in self.data_sql_schema
            if not column.primary_key and column.name not in ["tasks_id", "annotations"]
        ]

    def get_or_create_project(self, raise_exception: bool = True) -> str:
        if not self.label_studio_session.is_service_up(raise_exception=raise_exception):
            return '-1'

        # Authorize or sign up
        if not self.label_studio_session.login():
            self.label_studio_session.sign_up()
            self.label_studio_session.is_auth_ok(raise_exception=True)

        project_id = self.label_studio_session.get_project_id_by_title(self.project_title)
        if project_id is None:
            project = self.label_studio_session.create_project(
                project_setting={
                    "title": self.project_title,
                    "description": self.project_description,
                    "label_config": self.project_label_config,
                    "expert_instruction": "",
                    "show_instruction": False,
                    "show_skip_button": False,
                    "enable_empty_annotation": True,
                    "show_annotation_history": False,
                    "organization": 1,
                    "color": "#FFFFFF",
                    "maximum_annotations": 1,
                    "is_published": False,
                    "model_version": "",
                    "is_draft": False,
                    "min_annotations_to_start_training": 10,
                    "show_collab_predictions": True,
                    "sampling": "Sequential sampling",
                    "show_ground_truth_first": True,
                    "show_overlap_first": True,
                    "overlap_cohort_percentage": 100,
                    "task_data_login": None,
                    "task_data_password": None,
                    "control_weights": {}
                }
            )
            project_id = project['id']

        return project_id

    def get_current_tasks_from_LS(self, include_annotations: bool) -> Iterator[DataDF]:
        """
            Возвращает все задачи из сервера LS
        """
        self.label_studio_session.is_service_up(raise_exception=True)
        project_summary = self.label_studio_session.get_project_summary(self.get_or_create_project())
        if 'all_data_columns' not in project_summary:
            total_tasks_count = 0
        else:
            keys = [key for key in self.data_columns if key in project_summary['all_data_columns']]
            total_tasks_count = project_summary['all_data_columns'][keys[0]] if len(keys) > 0 else 0

        total_pages = total_tasks_count // self.page_chunk_size + 1

        # created_ago - очень плохой параметр, он меняется каждый раз, когда происходит запрос
        def _cleanup_annotations(annotations):
            for ann in annotations:
                if 'created_ago' in ann:
                    del ann['created_ago']
            return annotations

        for page in tqdm(range(1, total_pages + 1), desc='Getting tasks from Label Studio Projects...', disable=True):
            tasks_page = self.label_studio_session.get_tasks(
                project_id=self.get_or_create_project(),
                page=page,
                page_size=self.page_chunk_size
            )

            output_df = pd.DataFrame.from_records(
                {
                    **{
                        primary_key: [task['data'][primary_key] for task in tasks_page]
                        for primary_key in self.primary_keys + self.data_columns
                    },
                    'tasks_id': [str(task['id']) for task in tasks_page]
                }
            )
            if include_annotations:
                output_df['annotations'] = [_cleanup_annotations(task['annotations']) for task in tasks_page]

            yield output_df

    def get_current_indexes_from_LS(self) -> DataDF:
        """
            Возвращает все текущие в LS колонки .primary_keys плюс айдишники задач в LS tasks_id
        """
        self.label_studio_session.is_service_up(raise_exception=True)
        self.get_or_create_project()

        ls_indexes_df: List[pd.DataFrame] = []
        for current_tasks_as_df in self.get_current_tasks_from_LS(include_annotations=False):
            ls_indexes_df.append(current_tasks_as_df[self.primary_keys + ['tasks_id']])
        current_indexes_df = cast(DataDF, pd.concat(ls_indexes_df, ignore_index=True))
        return current_indexes_df

    def delete_rows(self, idx: IndexDF, tasks_ids: Optional[List[str]] = None) -> None:
        """
            Удаляет из LS задачи с заданными строками.
            tasks_ids -- кидаем сюда список номеров задач, если они известны (в целях ускорения)
        """
        self.label_studio_session.is_service_up(raise_exception=True)
        if tasks_ids is not None:
            assert len(idx) == len(tasks_ids)
        else:
            ls_indexes_df = self.get_current_indexes_from_LS()
            ls_indexes = data_to_index(ls_indexes_df, self.primary_keys)
            ls_indexes_intersection = index_intersection(idx, ls_indexes)
            tasks_ids = list(index_to_data(ls_indexes_df, ls_indexes_intersection)['tasks_id'])
        self.label_studio_session.delete_tasks(project_id=self.get_or_create_project(), tasks_ids=tasks_ids)

    def insert_rows(self, df: DataDF) -> None:
        self.label_studio_session.is_service_up(raise_exception=True)
        data = [
            {
                'data': {
                    **{
                        primary_key: df.loc[idx, primary_key]
                        for primary_key in self.primary_keys
                    },
                    **{
                        column: df.loc[idx, column]
                        for column in self.data_columns
                    }
                },
                'annotations': df.loc[idx, self.preannotations] if self.preannotations is not None else [],
                'predictions': df.loc[idx, self.predictions] if self.predictions is not None else [],
            }
            for idx in df.index
        ]
        self.label_studio_session.upload_tasks(data=data, project_id=self.get_or_create_project())

    def update_rows(self, df: DataDF) -> None:
        self.label_studio_session.is_service_up(raise_exception=True)
        ls_indexes_df = self.get_current_indexes_from_LS()
        ls_indexes = data_to_index(ls_indexes_df, self.primary_keys)
        df_idxs = data_to_index(df, self.primary_keys)

        new_idx = index_difference(df_idxs, ls_indexes)  # To be added to LS
        to_be_updated_idx = index_intersection(df_idxs, ls_indexes)  # To be deleted from LS and updated as new to LS
        deleted_idx = index_difference(df_idxs, ls_indexes)  # To be deleted from LS

        tasks_ids_to_be_deleted: List[str] = index_to_data(ls_indexes_df, deleted_idx)['tasks_id'].tolist()
        tasks_ids_to_be_updated: List[str] = index_to_data(ls_indexes_df, to_be_updated_idx)['tasks_id'].tolist()

        self.delete_rows(
            cast(IndexDF, pd.concat([to_be_updated_idx, deleted_idx], ignore_index=True)),
            tasks_ids=tasks_ids_to_be_updated + tasks_ids_to_be_deleted
        )
        self.insert_rows(index_to_data(df, cast(IndexDF, pd.concat([new_idx, to_be_updated_idx], ignore_index=True))))

    def read_rows(self, idx: IndexDF = None) -> DataDF:
        output_df = []
        for output_df_chunk in self.get_current_tasks_from_LS(include_annotations=True):
            if idx is not None:
                data_idx = data_to_index(output_df_chunk, self.primary_keys)
                intersection_idx = index_intersection(data_idx, idx)
                output_df_chunk = index_to_data(output_df_chunk, intersection_idx)
            output_df.append(output_df_chunk)

        return pd.concat(output_df, ignore_index=True)

    def read_rows_meta_pseudo_df(self, idx: Optional[IndexDF] = None) -> DataDF:
        raise NotImplementedError
