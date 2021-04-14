import logging
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from sqlalchemy.sql.sqltypes import JSON
import sqlalchemy as sql

from datapipe.metastore import MetaStore, PRIMARY_KEY
from datapipe.datatable import DataTable
from datapipe.store.table_store_sql import TableStoreDB, DBConn

from label_studio.storage.base import CloudStorage, BaseForm
from label_studio.storage.filesystem import BaseStorage
from label_studio.project import Project

logger = logging.getLogger(__name__)

TASKS_JSON_SQL_SCHEMA = [sql.Column('data', JSON)]
DATA_JSON_SQL_SCHEMA = [sql.Column('data', JSON)]


class ExternalTasksJSONStorageModified(CloudStorage):

    form = BaseForm
    description = 'Local [loading tasks from "tasks.json" file]'

    def __init__(
        self,
        name: str,
        path: str,
        project_path: str,
        project: Project,
        prefix: str = None,
        create_local_copy: bool = False,
        regex: str = '.*',
        **kwargs
    ):
        self.data_store = MetaStore(DBConn(kwargs['connstr'], kwargs['schema']))
        self.data_table = DataTable(
            self.data_store,
            name=kwargs['data_table_name'],
            table_store=TableStoreDB(
                self.data_store.dbconn,
                f'{kwargs["data_table_name"]}_data',
                data_sql_schema=PRIMARY_KEY + DATA_JSON_SQL_SCHEMA
            )
        )
        super().__init__(
            name=name,
            project_path=project_path,
            project=project,
            path=str(Path(project_path) / 'tasks.json'),
            use_blob_urls=False,
            prefix=None,
            regex=None,
            create_local_copy=False,
            sync_in_thread=False,
            **kwargs
        )

    def _get_client(self):
        pass

    def validate_connection(self):
        pass

    @property
    def url_prefix(self):
        return ''

    @property
    def readable_path(self):
        return str(self.path)

    def _get_value(self, key: int, inplace=False):
        key = str(key)
        df = self.data_table.get_data(idx=[key])
        data = df.loc[key, 'data']
        return data

    def _set_value(self, key: int, value: str):
        key = str(key)
        df = pd.DataFrame({'data': [value]}, index=[key])
        self.data_table.store_chunk(df)

    def set(self, id: int, value: str):
        with self.thread_lock:
            super().set(id, value)

    def set_many(self, ids: List[int], values: List[str]):
        with self.thread_lock:
            for id, value in zip(ids, values):
                super()._pre_set(id, value)
            self._save_ids()

    def _extract_task_id(self, full_key: str) -> int:
        return int(full_key.split(self.key_prefix, 1)[-1])

    def iter_full_keys(self) -> Tuple[List[int]]:
        return (self.key_prefix + key for key in self._get_objects())

    def _get_objects(self) -> Tuple[List[str]]:
        if self.data_table is not None:
            df = self.data_table.get_data(idx=None)
        else:
            df = {}
        if len(df) > 0:
            data = {
                int(k): v for k, v in zip(df.index, df['data'])
            }
        else:
            data = {}
        return (str(id) for id in data)

    def _remove_id_from_keys_map(self, id: int):
        full_key = self.key_prefix + str(id)
        assert id in self._ids_keys_map, 'No such task id: ' + str(id)
        assert self._ids_keys_map[id]['key'] == full_key, (self._ids_keys_map[id]['key'], full_key)
        self._selected_ids.remove(id)
        self._ids_keys_map.pop(id)
        self._keys_ids_map.pop(full_key)

    def _data_table_ids(self):
        return [int(x) for x in self.data_table.get_indexes()]

    def remove(self, id: int):
        with self.thread_lock:
            id = int(id)

            logger.debug('Remove id=' + str(id) + ' from ids.json')
            self._remove_id_from_keys_map(id)
            self._save_ids()

            logger.debug('Remove id=' + str(id) + ' from tasks.json')
            keys = [str(other_key) for other_key in self._dt_ids() if other_key != id]
            self.data_table.sync_meta(chunks=[keys])

    def remove_all(self, ids: List[int] = None):
        with self.thread_lock:
            remove_ids = self._data_table_ids() if ids is None else ids

            for id in remove_ids:
                self._remove_id_from_keys_map(int(id))
            self._save_ids()

            logger.debug('Remove all data from tasks.json')
            sync_meta_ids = [
                str(id) for id in self._data_table_ids()
            ] if ids is None else [str(id) for id in self._data_table_ids() if id not in ids]
            self.data_table.sync_meta(chunks=[sync_meta_ids])


class CompletionsDirStorageModified(BaseStorage):

    form = BaseForm
    description = 'Directory with JSON task files (modified)'

    def __init__(
        self,
        name: str,
        path: str,
        project_path: str,
        project: Project,
        **kwargs
    ):
        self.data_store = MetaStore(DBConn(kwargs['connstr'], kwargs['schema']))
        self.data_table = DataTable(
            self.data_store,
            name=kwargs['data_table_name'],
            table_store=TableStoreDB(
                self.data_store.dbconn,
                f'{kwargs["data_table_name"]}_data',
                data_sql_schema=PRIMARY_KEY + DATA_JSON_SQL_SCHEMA    
            )
        )
        path = str(Path(project_path) / 'annotation')
        super().__init__(
            name=name, path=path, project_path=project_path, project=project, **kwargs
        )

    @property
    def readable_path(self):
        return str(self.path)

    def get(self, id):
        id = str(id)
        df = self.data_table.get_data(idx=[id])
        if len(df) > 0:
            return df.loc[id, 'data']

    def __contains__(self, id):
        id = str(id)
        df = self.data_table.get_indexes(idx=[id])
        return len(df) > 0

    def set(self, id, value):
        id = str(id)
        df = pd.DataFrame({'data': [value]}, index=[id])
        self.data_table.store_chunk(data_df=df)

    def set_many(self, keys, values):
        raise NotImplementedError

    def ids(self):
        return [int(x) for x in self.data_table.get_indexes()]

    def max_id(self):
        return max(self.ids(), default=-1)

    def items(self):
        if self.data_table is None:
            return []

        df = self.data_table.get_data()
        return [
            (int(id), annotation) for id, annotation in zip(df.index, df['data'])
            if annotation is not None
        ]

    def remove(self, key):
        keys = [str(other_key) for other_key in self.ids() if other_key != key]
        self.data_table.sync_meta(chunks=[keys])

    def remove_all(self, ids=None):
        ids = [str(id) for id in self.ids()] if ids is None else [str(id) for id in self.ids() if id not in ids]
        self.data_table.sync_meta(chunks=[ids])

    def empty(self):
        return len(self.ids()) == 0

    def sync(self):
        pass


class ExternalTasksJSONStorageModifiedNoSetNoRemove(ExternalTasksJSONStorageModified):

    form = BaseForm
    description = 'Local [loading tasks from "tasks.json" file] without set/remove'

    def __init__(
        self,
        name: str,
        path: str,
        project_path: str,
        project: Project,
        prefix: str = None,
        create_local_copy: bool = False,
        regex: str = '.*',
        **kwargs
    ):
        super().__init__(
            name=name,
            path=str(Path(project_path) / 'tasks.json'),
            project_path=project_path,
            project=project,
            prefix=None,
            create_local_copy=False,
            regex=None,
            **kwargs
        )

    def _get_client(self):
        pass

    def validate_connection(self):
        pass

    @property
    def url_prefix(self):
        return ''

    @property
    def readable_path(self):
        return str(self.path)

    def _set_value(self, key: int, value: str):
        pass

    def set(self, id: int, value: str):
        pass

    def set_many(self, ids: List[int], values: List[str]):
        pass

    def remove(self, id: int):
        pass

    def remove_all(self, ids: List[int] = None):
        pass
