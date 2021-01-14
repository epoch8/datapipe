import ujson as json
import os
import datetime
from pathlib import Path

import pandas as pd
from c12n_pipe.datatable import DataStore
from sqlalchemy.sql.sqltypes import String
import sqlalchemy as sql

from label_studio.storage.base import BaseForm
from label_studio.storage.filesystem import DirJSONsStorage


DB = 'postgresql://postgres:qwertyisALICE666@localhost/postgres'  # FIXME
DATA_STORE = DataStore(
    connstr=DB,
    schema='label_studio',
)
DATA_SQL_SCHEMA = [sql.Column('data', String)]


class DirJSONsStorageModified(DirJSONsStorage):

    description = 'Directory with JSON task files (modified)'

    @property
    def data_store(self):
        return DATA_STORE

    @property
    def data_table(self):
        _data_table = DATA_STORE.get_table(
            name=self.data_table_name,
            data_sql_schema=DATA_SQL_SCHEMA
        )
        return _data_table

    def __init__(self, name, path, data_table_name='directory', project_path=None, project=None, **kwargs):
        super().__init__(
            name=name, path=path, project_path=project_path, project=project, **kwargs
        )
        self.path = Path(self.path)
        if project_path is not None:
            self.project_path = Path(project_path)
            with open(self.project_path / 'config.json', 'r', encoding='utf8') as src:
                config = json.load(src)
            if 'data_table_name' in config:
                self.data_table_name = config['data_table_name']
            else:
                now = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
                self.data_table_name = f"{self.project_path.name}_{now}_{self.name}_{data_table_name}"
                config['data_table_name'] = self.data_table_name
                with open(self.project_path / 'config.json', 'w', encoding='utf8') as out:
                    json.dump(config, out, ensure_ascii=False)
        else:
            self.data_table_name = f"{self.name}_{data_table_name}"

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
        df = pd.DataFrame({
            'data': [json.dumps(value)]
        }, index=[id])
        self.data_table.store_chunk(
            data_df=df
        )
        super().set(id, value)

    def set_many(self, keys, values):
        raise NotImplementedError

    def ids(self):
        return [int(x) for x in self.data_table.get_indexes()]

    def max_id(self):
        return max(self.ids(), default=-1)

    def items(self):
        df = self.data_table.get_data()
        return [(int(id), json.loads(data)) for id, data in zip(df.index, df['data'])]

    def remove(self, key):
        keys = [str(other_key) for other_key in self.ids() if other_key != key]
        self.data_table.sync_meta(
            chunks=[keys],
        )

        super().remove(key)

    def remove_all(self, ids=None):
        ids = [str(id) for id in self.ids()] if ids is None else [str(id) for id in self.ids() if id not in ids]
        self.data_table.sync_meta(
            chunks=[ids]
        )

        super().remove_all(ids=ids)

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
            data_table_name='completions'
        )
