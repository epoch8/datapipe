from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Tuple, Union, Optional, cast
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
        self._project_id: Optional[str] = None
        self.view_data = {
            "title": "datapipe_view [DO NOT CHANGE OR DELETE IT]",
            "type": "list",
            "target": "tasks",
            "hiddenColumns": {
                "explore": [
                    "tasks:completed_at",
                    "tasks:cancelled_annotations",
                    "tasks:total_predictions",
                    "tasks:annotators",
                    "tasks:total_annotations",
                    "tasks:annotations_results",
                    "tasks:annotations_ids",
                    "tasks:predictions_score",
                    "tasks:predictions_results",
                    "tasks:file_upload",
                    "tasks:created_at"
                ],
                "labeling": [
                    "tasks:id"
                ]
            },
            "columnsWidth": {},
            "columnsDisplayType": {},
            "gridWidth": 4,
            "filters": {
                "conjunction": "and",
                "items": []
            }
        }
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
        if self._project_id is not None:
            return self._project_id

        if not self.label_studio_session.is_service_up(raise_exception=raise_exception):
            return '-1'

        # Authorize or sign up
        if not self.label_studio_session.login():
            self.label_studio_session.sign_up()
            self.label_studio_session.is_auth_ok(raise_exception=True)

        self._project_id = self.label_studio_session.get_project_id_by_title(self.project_title)
        if self._project_id is None:
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
            self._project_id = project['id']

        return self._project_id

    def get_or_create_view(
        self,
    ) -> str:
        views = self.label_studio_session.get_all_views()
        project_id = self.get_or_create_project()
        views_found: List[Dict[str, Any]] = [
            view
            for view in views
            if view['project'] == project_id and view['data'] == self.view_data
        ]
        if len(views_found) == 0:
            view = self.label_studio_session.create_view(self.get_or_create_project(), data=self.view_data)
            views_found = [view]
        elif len(views_found) >= 2:
            # Удаляем какие-то лишние странные views
            for view in views_found[1:]:
                self.label_studio_session.delete_view(view_id=view['id'])
            views_found = [views_found[0]]

        view_id = views_found[0]['id']
        return view_id

    def get_total_tasks_count(self) -> int:
        project_id = self.get_or_create_project()
        project_summary = self.label_studio_session.get_project_summary(project_id)
        if 'all_data_columns' not in project_summary:
            total_tasks_count = 0
        else:
            keys = [key for key in self.data_columns if key in project_summary['all_data_columns']]
            total_tasks_count = project_summary['all_data_columns'][keys[0]] if len(keys) > 0 else 0
        return total_tasks_count

    def get_current_tasks_from_LS_with_annotations(self) -> Iterator[DataDF]:
        """
            Возвращает все задачи из сервера LS вместе с разметкой
        """
        self.label_studio_session.is_service_up(raise_exception=True)
        total_tasks_count = self.get_total_tasks_count()
        total_pages = total_tasks_count // self.page_chunk_size + 1

        # created_ago - очень плохой параметр, он меняется каждый раз, когда происходит запрос
        def _cleanup_annotations(annotations):
            for ann in annotations:
                if 'created_ago' in ann:
                    del ann['created_ago']
            return annotations

        for page in tqdm(range(1, total_pages + 1), desc='Getting tasks from Label Studio Projects...'):
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
                    'tasks_id': [str(task['id']) for task in tasks_page],
                    'annotations': [_cleanup_annotations(task['annotations']) for task in tasks_page]
                }
            )
            yield output_df

    def get_current_tasks_from_LS_without_annotations(self) -> DataDF:
        """
            Возвращает все задачи из сервера LS без разметки, с primary ключами плюс tasks_id
        """
        tasks = self.label_studio_session.get_all_tasks_from_view(
            self.get_or_create_view(), page=1, page_size=100000000
        )
        ls_indexes_df = pd.DataFrame({
            **{
                primary_key: [task['data'][primary_key] for task in tasks]
                for primary_key in self.primary_keys + self.data_columns
            },
            'tasks_id': [str(task['id']) for task in tasks]
        })
        return ls_indexes_df

    def delete_rows(self, idx: IndexDF) -> None:
        """
            Удаляет из LS задачи с заданными индексами
        """
        self.label_studio_session.is_service_up(raise_exception=True)
        ls_indexes_df = self.get_current_tasks_from_LS_without_annotations()
        ls_indexes = data_to_index(ls_indexes_df, self.primary_keys)
        ls_indexes_intersection = index_intersection(idx, ls_indexes)
        tasks_ids = list(index_to_data(ls_indexes_df, ls_indexes_intersection)['tasks_id'])
        self.label_studio_session.delete_tasks(project_id=self.get_or_create_project(), tasks_ids=tasks_ids)

    def insert_rows(self, df: DataDF) -> None:
        """
            Добавляет в LS новые задачи с заданными ключами
        """
        self.label_studio_session.is_service_up(raise_exception=True)
        data = [
            {
                'data': {
                    **{
                        primary_key: df.loc[idx, primary_key]
                        for primary_key in self.primary_keys + self.data_columns
                    }
                },
                'annotations': df.loc[idx, self.preannotations] if self.preannotations is not None else [],
                'predictions': df.loc[idx, self.predictions] if self.predictions is not None else [],
            }
            for idx in df.index
        ]
        self.label_studio_session.upload_tasks(data=data, project_id=self.get_or_create_project())

    def update_rows(self, df: DataDF) -> None:
        """
            Изменяет в LS задачи следующим образом:
            1) добавляет новые задачи с новыми ключами;
            2) удаляет и пересоздает те же задачи с измененными ключами;
            3) удаляет задачи на удаленных ключах
        """
        self.label_studio_session.is_service_up(raise_exception=True)
        ls_indexes_df = self.get_current_tasks_from_LS_without_annotations()
        ls_indexes = data_to_index(ls_indexes_df, self.primary_keys)
        df_idxs = data_to_index(df, self.primary_keys)

        new_idx = index_difference(df_idxs, ls_indexes)  # To be added to LS
        to_be_updated_idx = index_intersection(df_idxs, ls_indexes)  # To be deleted from LS and uploadedd as new to LS
        deleted_idx = index_difference(df_idxs, ls_indexes)  # To be deleted from LS

        self.delete_rows(cast(IndexDF, pd.concat([to_be_updated_idx, deleted_idx], ignore_index=True)))
        self.insert_rows(index_to_data(df, cast(IndexDF, pd.concat([new_idx, to_be_updated_idx], ignore_index=True))))

    def read_rows(self, idx: IndexDF = None) -> DataDF:
        output_df = []
        for output_df_chunk in self.get_current_tasks_from_LS_with_annotations():
            if idx is not None:
                data_idx = data_to_index(output_df_chunk, self.primary_keys)
                intersection_idx = index_intersection(data_idx, idx)
                output_df_chunk = index_to_data(output_df_chunk, intersection_idx)
            output_df.append(output_df_chunk)

        return pd.concat(output_df, ignore_index=True)

    def read_rows_meta_pseudo_df(self, idx: Optional[IndexDF] = None) -> DataDF:
        tasks = self.label_studio_session.get_all_tasks_from_view(
            self.get_or_create_view(), page=1, page_size=100000000
        )
        meta_pseudo_df = pd.DataFrame({
            **{
                primary_key: [task['data'][primary_key] for task in tasks]
                for primary_key in self.primary_keys + self.data_columns
            },
            'completed_at': [str(task['completed_at']) for task in tasks]
        })
        if idx is not None:
            data_idx = data_to_index(meta_pseudo_df, self.primary_keys)
            intersection_idx = index_intersection(data_idx, idx)
            meta_pseudo_df = index_to_data(meta_pseudo_df, intersection_idx)

        return meta_pseudo_df
