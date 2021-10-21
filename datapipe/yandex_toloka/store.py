from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Literal, Tuple, Type, Union, Optional, cast

import numpy as np
import pandas as pd

from sqlalchemy.sql.sqltypes import JSON, Boolean, Float, Integer, String
from sqlalchemy import Column

import toloka.client as toloka
from toloka.client.project.view_spec import ClassicViewSpec, ViewSpec
from toloka.client.project.task_spec import TaskSpec
from toloka.client.pool.mixer_config import MixerConfig

from datapipe.store.database import DBConn, TableStoreDB
from datapipe.store.table_store import TableStore
from datapipe.types import (
    DataDF, DataSchema, IndexDF, data_to_index, index_to_data
)


# Бывают следующие спеки:
SQL_TYPE_TO_FIELD_SPEC: Dict[Type[Column], Type[toloka.project.field_spec.FieldSpec]] = {
    Boolean: toloka.project.field_spec.BooleanSpec,
    String: toloka.project.field_spec.StringSpec,
    Integer: toloka.project.field_spec.IntegerSpec,
    Float: toloka.project.field_spec.FloatSpec,
    JSON: toloka.project.field_spec.JsonSpec,
}
# # Недостающие спеки:
#     toloka.project.field_spec.UrlSpec
#     toloka.project.field_spec.ArrayBooleanSpec
#     toloka.project.field_spec.ArrayStringSpec
#     toloka.project.field_spec.ArrayIntegerSpec
#     toloka.project.field_spec.ArrayFloatSpec
#     toloka.project.field_spec.ArrayUrlSpec
#     toloka.project.field_spec.FieldType
#     toloka.project.field_spec.FieldSpec
#     toloka.project.field_spec.FileSpec
#     toloka.project.field_spec.CoordinatesSpec
#     toloka.project.field_spec.ArrayFileSpec
#     toloka.project.field_spec.ArrayCoordinatesSpec


@dataclass
class TableStoreYandexToloka(TableStore):
    """
        Для подробных примеров параметров для создания проектов можно посмотреть
        https://github.com/Toloka/toloka-kit/tree/main/examples

        Пока работает только для одного перекрытия.
    """

    def __init__(
        self,
        dbconn: Union[DBConn, str],
        token: str,
        environment: Literal['PRODUCTION', 'SANDBOX'],
        input_data_sql_schema: List[Column],
        output_data_sql_schema: List[Column],
        project_identifier: str,
        tasks_per_page_at_create_project: int,
        view_spec_at_create_project: ViewSpec = ClassicViewSpec(markup='', script='', styles=''),
        # Arguments appended to toloka.Project() except 'private_comment' and 'task_spec'
        kwargs_at_create_project: Dict[str, Any] = {},

        # Arguments appended to toloka.Pool() except 'private_name', 'project_id' and 'defaults'
        kwargs_at_create_pool: Dict[str, Any] = {}
    ):
        self.toloka_client = toloka.TolokaClient(
            token=token,
            environment=environment
        )
        input_used_columns = [column.name for column in input_data_sql_schema]
        output_used_columns = [column.name for column in output_data_sql_schema]

        for column in input_data_sql_schema:
            assert column.name not in output_used_columns, (
                f"The column '{column.name}' is already used in data_sql_schema."
            )
        for column in output_data_sql_schema:
            assert column.name not in input_used_columns, (
                f"The column '{column.name}' is already used in data_sql_schema."
            )
        for column in ['is_deleted']:
            assert column not in input_used_columns + output_data_sql_schema, (
                f"The column '{column}' is reserved for this table store."
            )

        self.input_data_sql_schema = input_data_sql_schema
        self.output_data_sql_schema = output_data_sql_schema
        self._data_sql_schema = input_data_sql_schema + output_data_sql_schema

        self.input_spec: Dict[str, toloka.project.field_spec.FieldSpec] = {
            column.name: SQL_TYPE_TO_FIELD_SPEC[type(column.type)]()
            for column in self.input_data_sql_schema
        }
        self.output_spec: Dict[str, toloka.project.field_spec.FieldSpec] = {
            column.name: SQL_TYPE_TO_FIELD_SPEC[type(column.type)]()
            for column in self.output_data_sql_schema
        }

        result = self._get_project_and_pool_by_identifier(project_identifier)
        if result is None:
            self.project = self.toloka_client.create_project(
                toloka.Project(
                    private_comment=project_identifier,
                    task_spec=TaskSpec(
                        input_spec=self.input_spec,
                        output_spec=self.output_spec,
                        view_spec=view_spec_at_create_project
                    ),
                    **kwargs_at_create_project
                )
            )
            self.pool = self.toloka_client.create_pool(
                toloka.Pool(
                    project_id=self.project.id,
                    private_name=project_identifier,
                    defaults=toloka.Pool.Defaults(
                        default_overlap_for_new_tasks=1,
                        default_overlap_for_new_task_suites=0
                    ),
                    mixer_config=MixerConfig(
                        real_tasks_count=tasks_per_page_at_create_project,
                        golden_tasks_count=0,
                        training_tasks_count=0
                    ),
                    **kwargs_at_create_pool
                )
            )
        else:
            self.project, self.pool = result
        self.inner_table_store = TableStoreDB(
            dbconn=dbconn,
            name=project_identifier,
            data_sql_schema=[
                Column(column.name, column.type) for column in self.input_data_sql_schema
            ] + [
                Column('__task_id', String(), primary_key=True),
                Column('is_deleted', Boolean)
            ]
        )

    def _get_project_and_pool_by_identifier(
        self,
        project_identifier: str,
    ) -> Optional[Tuple[toloka.Project, toloka.Pool]]:
        project_search_result = self.toloka_client.find_projects(status=toloka.Project.ProjectStatus.ACTIVE)
        if project_search_result.items is None:
            return None
        project_identifiers = [project.private_comment for project in project_search_result.items]
        if project_identifier not in project_identifiers:
            return None
        assert project_identifiers.count(project_identifier) == 1, (
            f'There are 2 or more active projects with project_identifier="{project_identifier}"'
        )
        project = project_search_result.items[project_identifiers.index(project_identifier)]
        pool_search_result = self.toloka_client.find_pools(project_id=project.id)
        assert pool_search_result.items is not None
        pools_identifiers = [pool.private_name for pool in pool_search_result.items]
        assert pools_identifiers.count(project_identifier) == 1, (
            f'There are 0 or [2 or more] active pools with project_identifier="{project_identifier}"'
        )
        pool = pool_search_result.items[pools_identifiers.index(project_identifier)]
        return project, pool

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self._data_sql_schema if column.primary_key]

    def _get_all_tasks(self, chunksize: int = 1000) -> Iterator[List[toloka.Task]]:
        """
            Получить все задачи без разметки от людей с некоторой метаинформацией
        """
        tasks = []
        for task in self.toloka_client.get_tasks(pool_id=self.pool.id):
            tasks.append(task)
            if len(tasks) == chunksize:
                yield tasks
                tasks = []

        if len(tasks) > 0:
            yield tasks

    def delete_rows(self, idx: IndexDF) -> None:
        """
            Помечаем в толоке задачи "удаленными" через выставление нулевого пересечения и колонки deleted
        """
        for tasks_chunk in self._get_all_tasks():
            tasks_chunk_df = pd.DataFrame.from_records([
                {
                    **(
                        {
                            key: task.input_values[key]
                            for key in task.input_values
                        } if task.input_values is not None else {}
                    ),
                    '__task_id': task.id
                }
                for task in tasks_chunk
            ])
            tasks_chunk_df = index_to_data(tasks_chunk_df, idx)
            # Читаем табличку с индексами задач и отбираем те, что нам нужно удалить
            inner_table_df = self.inner_table_store.read_rows(
                idx=data_to_index(tasks_chunk_df, self.inner_table_store.primary_keys)
            )
            inner_table_df_to_be_deleted = inner_table_df.query('not is_deleted')

            for task_id in inner_table_df_to_be_deleted['__task_id']:
                self.toloka_client.patch_task_overlap_or_min(task_id=task_id, overlap=0)
            # Обновляем табличку с индексами, помечая только что удаленные задачи как удаленные
            inner_table_df_to_be_deleted['is_deleted'] = True
            self.inner_table_store.update_rows(inner_table_df_to_be_deleted)

    def insert_rows(self, df: DataDF) -> None:
        """
            Добавляет в Толоку новые задачи с заданными ключами
        """
        def _convert_if_need(value: Any):
            if isinstance(value, np.int64):
                return int(value)
            return value

        tasks = [
            toloka.Task(
                input_values={
                    column.name: _convert_if_need(df.loc[idx, column.name])
                    for column in self.input_data_sql_schema
                },
                pool_id=self.pool.id
            )
            for idx in df.index
        ]
        # There is maximum 10000 tasks per request

        def chunks(lst: List[Any], n: int):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        for tasks_chunk in chunks(tasks, 10000):
            tasks_chunks_res = self.toloka_client.create_tasks(tasks_chunk, allow_defaults=True)
            assert tasks_chunks_res.items is not None, f"Something goes wrong: {tasks_chunks_res=}"
            self.inner_table_store.insert_rows(
                pd.DataFrame.from_records([
                    {
                        **(
                            {
                                key: _convert_if_need(task.input_values[key])
                                for key in task.input_values
                            } if task.input_values is not None else {}
                        ),
                        '__task_id': tasks_chunks_res.items[str(i)].id,
                        'is_deleted': False
                    }
                    for i, task in enumerate(tasks_chunk)
                ])
            )

    def update_rows(self, df: DataDF) -> None:
        self.delete_rows(data_to_index(df, self.primary_keys))
        self.insert_rows(df)

    def read_rows(self, idx: IndexDF = None) -> DataDF:
        # Читаем все задачи и ищем удаленные задачи
        inner_table_df = self.inner_table_store.read_rows()
        deleted_tasks_df = inner_table_df.query('is_deleted')
        deleted_tasks = set(deleted_tasks_df['__task_id'])

        # Читаем разметку от людей, убирая удаленные задачи
        assignments = []
        for assignment in self.toloka_client.get_assignments(
            status=['SUBMITTED', 'ACCEPTED', 'REJECTED'],
            pool_id=self.pool.id
        ):
            assert assignment.tasks is not None
            assert assignment.solutions is not None

            assignments.extend([
                {
                    **(
                        {
                            key: task.input_values[key]
                            for key in task.input_values
                        } if task.input_values is not None else {}
                    ),
                    **solution.output_values,
                    '__task_id': task.id,
                    '__user_id': assignment.user_id
                }
                for task, solution in zip(assignment.tasks, assignment.solutions)
                if task.id not in deleted_tasks
            ])
        if len(assignments) > 0:
            assignments_df = pd.DataFrame.from_records(assignments)
        else:
            assignments_df = pd.DataFrame(
                {},
                columns=[column.name for column in self.output_data_sql_schema] + ['__task_id', '__user_id']
            )
        completed_task_ids = set(assignments_df['__task_id'])  # noqa: F841

        # Читаем оставшиеся задачи, для которых еще нет разметки и не удалены
        inner_table_df = inner_table_df.query(
            'not is_deleted and not (__task_id in @completed_task_ids)'
        )
        output_df = (
            pd.concat([assignments_df, inner_table_df], ignore_index=True)
            .replace({np.nan: None})
            .convert_dtypes()
        )

        output_df = cast(DataDF, output_df.loc[:, [column.name for column in self._data_sql_schema]])

        if idx is not None:
            output_df = index_to_data(output_df, idx)

        return output_df

    def read_rows_meta_pseudo_df(self, chunksize: int = 1000) -> Iterator[DataDF]:
        """
            Получить все задачи без разметки и data_columns
        """
        # Читаем все задачи и ищем удаленные задачи
        inner_table_df = self.inner_table_store.read_rows()
        deleted_tasks_df = inner_table_df.query('is_deleted')
        deleted_tasks = set(deleted_tasks_df['__task_id'])

        for tasks_chunk in self._get_all_tasks(chunksize):
            yield pd.DataFrame.from_records([
                {
                    **(
                        {
                            key: task.input_values[key]
                            for key in task.input_values
                        } if task.input_values is not None else {}
                    ),
                    '__task_id': task.id,
                    'remaining_overlap': task.remaining_overlap
                }
                for task in tasks_chunk
                if task.id not in deleted_tasks
            ])
