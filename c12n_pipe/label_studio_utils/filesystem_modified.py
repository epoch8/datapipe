import logging
import ujson as json
import os
import datetime
from pathlib import Path
from typing import Union

import pandas as pd
from c12n_pipe.datatable import DataStore, inc_process
from sqlalchemy.sql.sqltypes import String
import sqlalchemy as sql

from label_studio.storage.base import CloudStorage, BaseForm
from label_studio.storage.filesystem import BaseStorage, DirJSONsStorage

logger = logging.getLogger(__name__)

DB = 'postgresql://postgres:qwertyisALICE666@localhost/postgres'  # FIXME
DATA_STORE = DataStore(
    connstr=DB,
    schema='label_studio',
)
TASKS_JSON_SQL_SCHEMA = [sql.Column('data', String)]
DATA_JSON_SQL_SCHEMA = [sql.Column('data', String)]


def get_data_table_name_from_project(
    project_path: Union[str, Path],
    name: str,
    base_data_table_name: str
) -> str:
    if project_path is not None:
        project_path = Path(project_path)
        with open(project_path / 'config.json', 'r', encoding='utf8') as src:
            config = json.load(src)
        if f'data_table_name_{base_data_table_name}' in config:
            data_table_name = config[f'data_table_name_{base_data_table_name}']
        else:
            now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            data_table_name = f"{project_path.name}_{now}_{name}_{base_data_table_name}"
            config[f'data_table_name_{base_data_table_name}'] = data_table_name
            with open(project_path / 'config.json', 'w', encoding='utf8') as out:
                json.dump(config, out, ensure_ascii=False)
    else:
        data_table_name = f"{name}_{base_data_table_name}"
    return data_table_name


class ExternalTasksJSONStorageModified(CloudStorage):

    form = BaseForm
    description = 'Local [loading tasks from "tasks.json" file]'


    @property
    def data_store(self):
        return DATA_STORE

    @property
    def data_table(self):
        _data_table = DATA_STORE.get_table(
            name=self.data_table_name,
            data_sql_schema=TASKS_JSON_SQL_SCHEMA
        )
        return _data_table

    def __init__(
        self, name, path, project_path,
        prefix=None, create_local_copy=False, regex='.*', **kwargs
    ):
        self.data_table_name = get_data_table_name_from_project(
            project_path=project_path,
            name=name,
            base_data_table_name='tasks_json'
        )
        super().__init__(
            name=name,
            project_path=project_path,
            path=os.path.join(project_path, 'tasks.json'),
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

    def _get_value(self, key, inplace=False):
        key = str(key)
        df = self.data_table.get_data(idx=[key])
        data = json.loads(df.loc[key, 'data'])
        return data

    def _set_value(self, key, value):
        key = str(key)
        df = pd.DataFrame({'data': [json.dumps(value)]}, index=[key])
        self.data_table.store_chunk(df)

    def set(self, id, value):
        with self.thread_lock:
            super().set(id, value)

    def set_many(self, ids, values):
        with self.thread_lock:
            for id, value in zip(ids, values):
                super()._pre_set(id, value)
            self._save_ids()

    def _extract_task_id(self, full_key):
        return int(full_key.split(self.key_prefix, 1)[-1])

    def iter_full_keys(self):
        return (self.key_prefix + key for key in self._get_objects())

    def _get_objects(self):
        df = self.data_table.get_data(idx=None)
        if len(df) > 0:
            data = {
                int(k): v for k, v in zip(df.index, df['data'])
            }
        else:
            data = {}
        return (str(id) for id in data)

    def _remove_id_from_keys_map(self, id):
        full_key = self.key_prefix + str(id)
        assert id in self._ids_keys_map, 'No such task id: ' + str(id)
        assert self._ids_keys_map[id]['key'] == full_key, (self._ids_keys_map[id]['key'], full_key)
        self._selected_ids.remove(id)
        self._ids_keys_map.pop(id)
        self._keys_ids_map.pop(full_key)

    def _data_table_ids(self):
        return [int(x) for x in self.data_table.get_indexes()]

    def remove(self, id):
        with self.thread_lock:
            id = int(id)

            logger.debug('Remove id=' + str(id) + ' from ids.json')
            self._remove_id_from_keys_map(id)
            self._save_ids()

            logger.debug('Remove id=' + str(id) + ' from tasks.json')
            keys = [str(other_key) for other_key in self._dt_ids() if other_key != id]
            self.data_table.sync_meta(chunks=[keys])

    def remove_all(self, ids=None):
        logger.info(f"ExternalTasksJSONStorageModified: Use remove: {ids=}")
        with self.thread_lock:
            remove_ids = self._data_table_ids() if ids is None else ids

            logger.debug('Remove ' + str(len(remove_ids)) + ' records from ids.json')
            for id in remove_ids:
                self._remove_id_from_keys_map(int(id))
            self._save_ids()

            logger.debug('Remove all data from tasks.json')
            sync_meta_ids = [
                str(id) for id in self._data_table_ids()
            ] if ids is None else [str(id) for id in self._data_table_ids() if id not in ids]
            self.data_table.sync_meta(chunks=[sync_meta_ids])


class DirJSONsStorageModified(BaseStorage):

    description = 'Directory with JSON task files (modified)'

    @property
    def data_store(self):
        return DATA_STORE

    @property
    def data_table_tasks(self):
        _data_table = DATA_STORE.get_table(
            name=self.data_table_name_tasks,
            data_sql_schema=TASKS_JSON_SQL_SCHEMA
        )
        return _data_table

    @property
    def data_table(self):
        _data_table = DATA_STORE.get_table(
            name=self.data_table_name,
            data_sql_schema=DATA_JSON_SQL_SCHEMA
        )
        return _data_table

    def __init__(self, name, path, base_data_table_name='directory', project_path=None, project=None, **kwargs):
        self.data_table_name_tasks = get_data_table_name_from_project(
            project_path=project_path,
            name=name,
            base_data_table_name='tasks_json'
        )
        self.data_table_name = get_data_table_name_from_project(
            project_path=project_path,
            name=name,
            base_data_table_name=base_data_table_name
        )
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
            data = json.loads(df.loc[id, 'data'])
            return data

    def __contains__(self, id):
        id = str(id)
        df = self.data_table.get_indexes(idx=[id])
        return len(df) > 0

    def set(self, id, value):
        id = str(id)
        df = pd.DataFrame({'data': [json.dumps(value)]}, index=[id])
        self.data_table.store_chunk(data_df=df)

    def set_many(self, keys, values):
        raise NotImplementedError

    def ids(self):
        return [int(x) for x in self.data_table.get_indexes()]

    def max_id(self):
        return max(self.ids(), default=-1)

    def items(self):
        df = self.data_table.get_data()
        return [
            (int(id), json.loads(annotation)) for id, annotation in zip(df.index, df['data'])
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


class CompletionsDirStorageModified(DirJSONsStorageModified):

    form = BaseForm
    description = 'Local [completions are in "completions" directory] (modified)'

    def __init__(self, name, path, project_path, **kwargs):
        super().__init__(
            name=name,
            project_path=project_path,
            path=os.path.join(project_path, 'completions'),
            base_data_table_name='completions'
        )
