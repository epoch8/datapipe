from dataclasses import dataclass
import logging
from typing import Any, Dict, Iterator, List, Literal, Sequence, Tuple, Type, Union, Optional, cast
from decimal import Decimal

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
    """

    def __init__(
        self,
        dbconn: Union[DBConn, str],
        token: str,
        environment: Literal['PRODUCTION', 'SANDBOX'],
        input_data_sql_schema: List[Column],
        output_data_sql_schema: List[Column],
        project_identifier: Union[str, Tuple[Union[str, int], Union[str, int]]],  # str or (project_id, pool_id)
        assignments_column: Optional[str] = 'assignments',
        tasks_per_page: int = 1,
        default_overlap: int = 1,
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
        for column in output_data_sql_schema:
            assert not column.primary_key
        for column in ['_task_id', 'is_deleted', '_task', assignments_column]:
            if column is None:
                continue
            assert column not in input_used_columns, (
                f"The column '{column}' is reserved for this table store."
            )

        self.input_data_sql_schema = input_data_sql_schema
        self.data_sql_schema = input_data_sql_schema
        self.assignments_column = assignments_column
        if assignments_column is not None:
            self.data_sql_schema += [Column(assignments_column, JSON)]

        self.input_spec: Dict[str, toloka.project.field_spec.FieldSpec] = {
            column.name: SQL_TYPE_TO_FIELD_SPEC[type(column.type)]()
            for column in self.input_data_sql_schema
        }
        self.output_spec: Dict[str, toloka.project.field_spec.FieldSpec] = {
            column.name: SQL_TYPE_TO_FIELD_SPEC[type(column.type)]()
            for column in output_data_sql_schema
        }

        self.mixed_config = MixerConfig(
            real_tasks_count=tasks_per_page,
            golden_tasks_count=0,
            training_tasks_count=0
        )
        self.defaults = toloka.Pool.Defaults(
            default_overlap_for_new_tasks=default_overlap,
            default_overlap_for_new_task_suites=default_overlap
        )

        self.inner_table_store = TableStoreDB(
            dbconn=dbconn,
            name=str(project_identifier),
            data_sql_schema=[
                Column(column.name, column.type, primary_key=column.primary_key)
                for column in self.input_data_sql_schema
            ] + [
                Column('_task_id', String(), primary_key=True),
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
                    defaults=self.defaults,
                    mixer_config=self.mixed_config,
                    **kwargs_at_create_pool
                )
            )
        else:
            self.project, self.pool = result

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
            assert project.id is not None and pool.id is not None

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

            # Проверяем консинтетность:
            # На defaults (число перекрытия на задачу)
            if pool.defaults != self.defaults:
                try:
                    pool.set_defaults(self.defaults)
                    self.toloka_client.update_pool(pool_id=pool.id, pool=pool)
                except toloka.exceptions.IncorrectActionsApiError as e:
                    logger.warning(f"Couldn't set defaults. Reason: {e=}")
            # На mixer_config (число задач на страницу)
            if pool.mixer_config != self.mixed_config:
                try:
                    pool.set_mixer_config(self.mixed_config)
                    self.toloka_client.update_pool(pool_id=pool.id, pool=pool)
                except toloka.exceptions.IncorrectActionsApiError as e:
                    logger.warning(f"Couldn't set mixer_config. Reason: {e=}")

        return project, pool

    def _synchronize_inner_table(self):
        """
        Синхронизировать внутреннюю таблчку с задачами, на случай, если проект был создан как-то внешним образом
        Задачи могут заливаться так же внешним образом

        Все свежие задачи при такой синхронизации считаются неудаленными, а ключи в проекте -- неповторяющимися
        """
        # Читаем задачи во внутренней табличке
        inner_table_df = self.inner_table_store.read_rows()
        synchronized_tasks = set(inner_table_df['_task_id'])

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
                        '_task_id': task.id,
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
        looked_tasks = set()

        tasks: List[Task] = []
        for task in self.toloka_client.get_tasks(pool_id=self.pool.id):
            tasks.append(task)
            looked_tasks.add(task.id)
            if len(tasks) == chunksize:
                yield tasks
                tasks = []

        # Вытащим также задачи, которые были залиты как "больше 1 задачи на страницу" или внешним образом
        # В task_suite могут попасть задачи из предыдущего get_tasks, поэтому их надо пропустить
        for task_suite in self.toloka_client.get_task_suites(pool_id=self.pool.id):
            if task_suite.tasks is not None:
                for base_task in task_suite.tasks:
                    if base_task.id in looked_tasks:
                        continue
                    tasks.append(
                        TaskFromSuite(
                            input_values=base_task.input_values,
                            id=base_task.id,
                            overlap=cast(Any, task_suite).overlap,
                            remaining_overlap=task_suite.remaining_overlap
                        )
                    )
                    looked_tasks.add(base_task.id)
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
                    '_task_id': task.id,
                    '_task': task
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
            )

            for task_id, task in zip(
                inner_table_df_to_be_deleted_merge_tasks['_task_id'], inner_table_df_to_be_deleted_merge_tasks['_task']
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
        if df.empty:
            return

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

        def chunks(lst: List[Any], n: int):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]
        # Рекомендуемое число -- 10000 задач на реквест
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
                        '_task_id': tasks_chunks_res.items[str(i)].id,
                        'is_deleted': False
                    }
                    for i, task in enumerate(tasks_chunk)
                ])
            )

    def update_rows(self, df: DataDF) -> None:
        self.insert_rows(df)

    def read_rows(self, idx: IndexDF = None) -> DataDF:
        if idx is not None and idx.empty:
            return pd.DataFrame(columns=[column.name for column in self.data_sql_schema])

        # Читаем все задачи и ищем удаленные задачи
        inner_table_df = self.inner_table_store.read_rows()
        deleted_tasks_df = inner_table_df.query('is_deleted')
        deleted_tasks = set(deleted_tasks_df['_task_id'])

        # Читаем разметку от людей, убирая удаленные задачи
        assignments: List[Dict[str, Any]] = []
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
                    **(
                        {
                            self.assignments_column: {
                                'assignment_id': assignment.id,
                                'task_id': task.id,
                                'user_id': assignment.user_id,
                                'result': {
                                    key: _convert_if_need(solution.output_values[key])
                                    for key in solution.output_values
                                }
                            }
                        } if self.assignments_column is not None else {}
                    ),
                    '_task_id': task.id
                }
                for task, solution in zip(assignment.tasks, assignment.solutions)
                if task.id not in deleted_tasks
            ])

        if len(assignments) > 0:
            assignments_df = pd.DataFrame.from_records(assignments)
        else:
            assignments_df = pd.DataFrame(
                {},
                columns=[column.name for column in self.data_sql_schema] + ['_task_id']
            )

        # Контактенируем результаты из assignments_column в список:
        if self.assignments_column is not None:
            input_primary_keys = [column.name for column in self.input_data_sql_schema if column.primary_key]
            assignments_df = (
                assignments_df.groupby(by=input_primary_keys + ['_task_id'])[self.assignments_column]
                .apply(list)
                .reset_index()
                .drop_duplicates(subset=input_primary_keys + ['_task_id'])
            )

        completed_task_ids = set(assignments_df['_task_id'])  # noqa: F841
        # Читаем оставшиеся задачи, для которых еще нет разметки и не удалены
        completed_inner_table_df = inner_table_df.query(
            'not is_deleted and not (_task_id in @completed_task_ids)'
        )
        # Соединяем с предыдущей табличкой, заполняя несделанные задачи пустым списком
        completed_inner_table_df = completed_inner_table_df.reindex(columns=assignments_df.columns, fill_value=[])
        output_df = pd.concat([assignments_df, completed_inner_table_df], ignore_index=True).convert_dtypes()
        output_df = cast(DataDF, output_df.loc[:, [column.name for column in self.data_sql_schema]])

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
        deleted_tasks = set(deleted_tasks_df['_task_id'])

        for tasks_chunk in self._get_all_tasks(chunksize):
            yield pd.DataFrame.from_records([
                {
                    **(
                        {
                            key: task.input_values[key]
                            for key in task.input_values
                        } if task.input_values is not None else {}
                    ),
                    '_task_id': task.id,
                    'overlap': cast(Any, task).overlap,
                    'remaining_overlap': task.remaining_overlap
                }
                for task in tasks_chunk
                if task.id not in deleted_tasks
            ])
