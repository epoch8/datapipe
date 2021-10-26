from dataclasses import dataclass
import logging
from typing import Any, Dict, Iterator, List, Literal, Sequence, Tuple, Type, Union, Optional, cast
from decimal import Decimal

import numpy as np
import pandas as pd

from sqlalchemy.sql.sqltypes import JSON, Boolean, Float, Integer, String
from sqlalchemy import Column

import toloka.client as toloka
from toloka.client.assignment import Assignment
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
FIELD_SPEC_TO_SQL_TYPE: Dict[Type[toloka.project.field_spec.FieldSpec], Type[Column]] = {
    **{
        value: key
        for key, value in SQL_TYPE_TO_FIELD_SPEC.items()
    },
    toloka.project.field_spec.UrlSpec: String,
    toloka.project.field_spec.ArrayJsonSpec: JSON  # type: ignore
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


logger = logging.getLogger('datapipe.yandex_toloka.store')


class TaskFromSuite(toloka.Task):
    """Отдельный класс задач, взятых от TaskSuite"""
    pass


Task = Union[toloka.Task, TaskFromSuite]


def _convert_if_need(value: Any) -> Any:
    if isinstance(value, np.int64):
        return int(value)
    if isinstance(value, Decimal):
        return float(round(value, 9))
    if isinstance(value, list):
        return [_convert_if_need(v) for v in value]
    if isinstance(value, dict):
        for k in value:
            value[k] = _convert_if_need(value[k])
    return value


@dataclass
class TableStoreYandexToloka(TableStore):
    """
        Для подробных примеров параметров в toloka.Project и toloka.Pool для создания проектов можно посмотреть
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
        project_identifier: Union[str, Tuple[Union[str, int], Union[str, int]]],  # str or (project_id, pool_id)
        user_id_column: Optional[str] = 'user_id',
        assignment_id_column: Optional[str] = 'assignement_id',
        view_spec_at_create_project: ViewSpec = ClassicViewSpec(markup='', script='', styles=''),
        # Arguments appended to toloka.Project()
        # Except 'private_comment' and 'task_spec'
        kwargs_at_create_project: Dict[str, Any] = {},

        # Arguments appended to toloka.Pool() when first create
        # Except 'private_name', 'project_id', 'mixer_config' and 'defaults'
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
        for column in ['__task_id', 'is_deleted']:
            assert column not in input_used_columns + output_data_sql_schema, (
                f"The column '{column}' is reserved for this table store."
            )

        self.input_data_sql_schema = input_data_sql_schema
        self.output_data_sql_schema = output_data_sql_schema
        self.data_sql_schema = input_data_sql_schema + output_data_sql_schema

        self.assignment_id_column = assignment_id_column
        self.user_id_column = user_id_column
        if user_id_column is not None:
            assert user_id_column not in input_used_columns + output_data_sql_schema, (
                f"The column '{user_id_column}' is already used in data_sql_schema."
            )
            self.data_sql_schema += [Column(user_id_column, String())]
        if assignment_id_column is not None:
            assert assignment_id_column not in input_used_columns + output_data_sql_schema, (
                f"The column '{assignment_id_column}' is already used in data_sql_schema."
            )
            self.data_sql_schema += [Column(assignment_id_column, String())]

        self.input_spec: Dict[str, toloka.project.field_spec.FieldSpec] = {
            column.name: SQL_TYPE_TO_FIELD_SPEC[type(column.type)]()
            for column in self.input_data_sql_schema
        }
        self.output_spec: Dict[str, toloka.project.field_spec.FieldSpec] = {
            column.name: SQL_TYPE_TO_FIELD_SPEC[type(column.type)]()
            for column in self.output_data_sql_schema
        }

        self.inner_table_store = TableStoreDB(
            dbconn=dbconn,
            name=str(project_identifier),
            data_sql_schema=[
                Column(column.name, column.type, primary_key=column.primary_key)
                for column in self.input_data_sql_schema
            ] + [
                Column('__task_id', String(), primary_key=True),
                Column('is_deleted', Boolean)
            ]
        )
        result = self._get_project_and_pool_by_identifier(project_identifier)
        if result is None:
            self.project = self.toloka_client.create_project(
                toloka.Project(
                    private_comment=str(project_identifier),
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
                    private_name=str(project_identifier),
                    defaults=toloka.Pool.Defaults(
                        default_overlap_for_new_tasks=1,
                        default_overlap_for_new_task_suites=1
                    ),
                    mixer_config=kwargs_at_create_pool.pop(
                        'mixer_config',
                        MixerConfig(
                            real_tasks_count=kwargs_at_create_pool.pop('mixer_config', 1),
                            golden_tasks_count=0,
                            training_tasks_count=0
                        )
                    ),
                    **kwargs_at_create_pool
                )
            )
        else:
            self.project, self.pool = result

        if self.pool.defaults is not None:
            assert self.pool.defaults.default_overlap_for_new_tasks in [0, 1]
            assert self.pool.defaults.default_overlap_for_new_task_suites in [0, 1]

        # Синхронизируем внутреннюю табличку
        self._synchronize_inner_table()

    def _get_project_and_pool_by_identifier(
        self,
        project_identifier: Union[str, Tuple[Union[str, int], Union[str, int]]],
    ) -> Optional[Tuple[toloka.Project, toloka.Pool]]:
        if isinstance(project_identifier, str):
            projects = list(self.toloka_client.get_projects(status=toloka.Project.ProjectStatus.ACTIVE))
            project_identifiers = [project.private_comment for project in projects]
            if project_identifier not in project_identifiers:
                return None
            assert project_identifiers.count(project_identifier) == 1, (
                f'There are 2 or more active projects with project_identifier="{project_identifier}"'
            )
            project = projects[project_identifiers.index(project_identifier)]
            pools = list(self.toloka_client.get_pools(project_id=project.id))
            pools_identifiers = [pool.private_name for pool in pools]
            assert pools_identifiers.count(project_identifier) == 1, (
                f'There are 0 or [2 or more] active pools with project_identifier="{project_identifier}"'
            )
            pool = pools[pools_identifiers.index(project_identifier)]
        else:
            assert isinstance(project_identifier, tuple) and len(project_identifier) == 2
            project_id, pool_id = project_identifier
            project = self.toloka_client.get_project(project_id=str(project_id))
            pool = self.toloka_client.get_pool(pool_id=str(pool_id))

            # Проверяем консинтетность на input_spec и output_spec
            our_input_spec = {key: FIELD_SPEC_TO_SQL_TYPE[type(self.input_spec[key])] for key in self.input_spec}
            our_output_spec = {key: FIELD_SPEC_TO_SQL_TYPE[type(self.output_spec[key])] for key in self.output_spec}
            their_input_spec = {
                key: FIELD_SPEC_TO_SQL_TYPE[type(project.task_spec.input_spec[key])]
                for key in project.task_spec.input_spec
            } if project.task_spec is not None and project.task_spec.input_spec is not None else {}
            their_output_spec = {
                key: FIELD_SPEC_TO_SQL_TYPE[type(project.task_spec.output_spec[key])]
                for key in project.task_spec.output_spec
            } if project.task_spec is not None and project.task_spec.output_spec is not None else {}
            input_difference = set(our_input_spec.items()) ^ set(their_input_spec.items())
            output_difference = set(our_output_spec.items()) ^ set(their_output_spec.items())
            assert len(input_difference) == 0, f"{input_difference=}"
            assert len(output_difference) == 0, f"{output_difference=}"

        return project, pool

    def _synchronize_inner_table(self):
        """
        Синхронизировать внутреннюю таблчку с задачами, на случай, если проект был создан как-то внешним образом
        Задачи могут заливаться так же внешним образом

        Все свежие задачи при такой синхронизации считаются неудаленными, а ключи в проекте -- неповторяющимися
        """
        # Читаем задачи во внутренней табличке
        inner_table_df = self.inner_table_store.read_rows()
        synchronized_tasks = set(inner_table_df['__task_id'])

        for tasks_chunk in self._get_all_tasks():
            self.inner_table_store.insert_rows(
                pd.DataFrame.from_records([
                    {
                        **(
                            {
                                key: task.input_values[key]
                                for key in task.input_values
                            } if task.input_values is not None else {}
                        ),
                        '__task_id': task.id,
                        'is_deleted': False
                    }
                    for task in tasks_chunk if task.id not in synchronized_tasks
                ])
            )

    def get_primary_schema(self) -> DataSchema:
        return [column for column in self.data_sql_schema if column.primary_key]

    def _get_all_tasks(self, chunksize: int = 1000) -> Iterator[Sequence[Task]]:
        """
            Получить все задачи без разметки от людей с некоторой метаинформацией
        """
        tasks: List[Task] = []
        for task in self.toloka_client.get_tasks(pool_id=self.pool.id):
            tasks.append(task)
            if len(tasks) == chunksize:
                yield tasks
                tasks = []

        # Задачи, которые были залиты внешним образом
        for task_suite in self.toloka_client.get_task_suites(pool_id=self.pool.id):
            if task_suite.tasks is not None:
                for base_task in task_suite.tasks:
                    tasks.append(
                        TaskFromSuite(
                            input_values=base_task.input_values,
                            id=base_task.id
                        )
                    )
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
                    '__task_id': task.id,
                    'task': task
                }
                for task in tasks_chunk
            ])
            tasks_chunk_df = index_to_data(tasks_chunk_df, idx)
            # Читаем табличку с индексами задач и отбираем те, что нам нужно удалить
            inner_table_df = self.inner_table_store.read_rows(
                idx=data_to_index(tasks_chunk_df, self.inner_table_store.primary_keys)
            )
            inner_table_df_to_be_deleted = inner_table_df.query('not is_deleted')
            inner_table_df_to_be_deleted_merge_tasks = pd.merge(
                tasks_chunk_df, inner_table_df,
                on=self.inner_table_store.primary_keys
            ).drop_duplicates(['__task_id'])  # Дубликаты могут появиться из-за TaskSuite
            for task_id, task in zip(
                inner_table_df_to_be_deleted_merge_tasks['__task_id'], inner_table_df_to_be_deleted_merge_tasks['task']
            ):
                if isinstance(task, toloka.Task):
                    self.toloka_client.patch_task_overlap_or_min(task_id=task_id, overlap=0)
                elif isinstance(task, TaskFromSuite):
                    self.toloka_client.patch_task_suite_overlap_or_min(task_suite_id=task_id, overlap=0)
            # Обновляем табличку с индексами, помечая только что удаленные задачи как удаленные
            inner_table_df_to_be_deleted['is_deleted'] = True
            self.inner_table_store.update_rows(inner_table_df_to_be_deleted)

    def insert_rows(self, df: DataDF) -> None:
        """
            Добавляет в Толоку новые задачи с заданными ключами
        """
        self.delete_rows(data_to_index(df, self.primary_keys))

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
        self.insert_rows(df)

    def read_rows(self, idx: IndexDF = None) -> DataDF:
        # Читаем все задачи и ищем удаленные задачи
        inner_table_df = self.inner_table_store.read_rows()
        deleted_tasks_df = inner_table_df.query('is_deleted')
        deleted_tasks = set(deleted_tasks_df['__task_id'])

        # Читаем разметку от людей, убирая удаленные задачи
        assignments = []
        assignments_rejected = []
        for assignment in self.toloka_client.get_assignments(
            status=['SUBMITTED', 'ACCEPTED', 'REJECTED'],
            pool_id=self.pool.id
        ):
            assert assignment.tasks is not None
            assert assignment.solutions is not None

            assignment_data = [
                {
                    **(
                        {
                            key: task.input_values[key]
                            for key in task.input_values
                        } if task.input_values is not None else {}
                    ),
                    **{
                        key: _convert_if_need(solution.output_values[key])
                        for key in solution.output_values
                    },
                    '__task_id': task.id,
                    **(
                        {
                            self.user_id_column: assignment.user_id
                        } if self.user_id_column is not None else {}
                    ),
                    **(
                        {
                            self.assignment_id_column: assignment.id
                        } if self.assignment_id_column is not None else {}
                    ),
                }
                for task, solution in zip(assignment.tasks, assignment.solutions)
                if task.id not in deleted_tasks
            ]
            assignments.extend(assignment_data)
            if assignment.status != toloka.assignment.Assignment().Status.REJECTED:
                assignments.extend(assignment_data)
            else:
                assignments_rejected.extend(assignment_data)

        if len(assignments) > 0:
            assignments_df = pd.DataFrame.from_records(assignments)
        else:
            assignments_df = pd.DataFrame(
                {},
                columns=[column.name for column in self.data_sql_schema] + ['__task_id']
            )
        completed_task_ids = set(assignments_df['__task_id'])  # noqa: F841

        # Задачи, чья разметка была отказана, нужно удалять, а затем перезалить их (с новыми __task_id)
        if len(assignments_rejected) > 0:
            assignments_rejected_df = pd.DataFrame.from_records(assignments_rejected)
            rejected_task_ids = set(assignments_rejected_df['__task_id'])  # noqa: F841
            rejected_inner_table_df = inner_table_df.query(
                'not is_deleted and not (__task_id in @rejected_task_ids)'
            )
            if len(rejected_inner_table_df) > 0:
                self.delete_rows(
                    idx=cast(IndexDF, rejected_inner_table_df[
                        [column.name for column in self.input_data_sql_schema if column.primary_key]
                    ])
                )
                self.insert_rows(rejected_inner_table_df[[column.name for column in self.input_data_sql_schema]])
                return self.read_rows(idx=idx)

        # Читаем оставшиеся задачи, для которых еще нет разметки и не удалены
        completed_inner_table_df = inner_table_df.query(
            'not is_deleted and not (__task_id in @completed_task_ids)'
        )
        output_df = (
            pd.concat([assignments_df, inner_table_df], ignore_index=True)
            .replace({np.nan: None})
            .convert_dtypes()
        )

        output_df = cast(DataDF, output_df.loc[:, [column.name for column in self.data_sql_schema]])

        if idx is not None:
            if idx.empty:
                output_df = pd.DataFrame(columns=output_df.columns)
            else:
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
