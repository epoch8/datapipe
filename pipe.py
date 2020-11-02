from __future__ import annotations
from typing import Dict, Tuple

import pandas as pd
import time
import logging


logger = logging.getLogger(__name__)


class DataStore(object):
    def __init__(self):
        pass

    def get_df(self, name: str) -> pd.DataFrame:
        raise NotImplementedError

    def put_success(self, name: str, df: pd.DataFrame) -> None:
        raise NotImplementedError

    def put_failure(self, name: str, idx: pd.Index, reason: str) -> None:
        raise NotImplementedError

class MemoryDataStore(object):
    def __init__(self):
        self.data: Dict[str, pd.DataFrame] = {}

    def _get_data(self, name: str) -> pd.DataFrame:
        if name not in self.data:
            self.data[name] = pd.DataFrame(
                {
                    'data': pd.Series([], dtype='object'),
                    'update_ts': pd.Series([], dtype='float'),
                    'version': pd.Series([], dtype='int'),
                    'last_run_ts': pd.Series([], dtype='float'),
                    'last_run_status': pd.Series([], dtype='string'),
                    'last_run_reason': pd.Series([], dtype='string'),
                }
            )

        return self.data[name]

    def get_df(self, name):
        return self._data_to_df(self._get_data(name))

    def _df_to_data_success(self, df: pd.DataFrame) -> pd.DataFrame:
        ts = time.time()
        data_df = pd.DataFrame(
            {
                'data': df.to_dict(orient='records'),
                'version': 1,
                'update_ts': ts,
                'last_run_ts': ts,
                'last_run_status': 'success',
                'last_run_reason': '',
            },
            index=df.index
        )

        return data_df

    def _idx_to_data_failure(self, idx: pd.Index, reason: str) -> pd.DataFrame:
        ts = time.time()
        data_df = pd.DataFrame(
            {
                'data': None,
                'version': 1,
                'update_ts': ts,
                'last_run_ts': ts,
                'last_run_status': 'failed',
                'last_run_reason': reason,
            },
            index = idx
        )

        return data_df

    def _data_to_df(self, data: pd.DataFrame) -> pd.DataFrame:
        bad_idx = data['data'].isnull()

        return pd.DataFrame.from_records(list(data.loc[-bad_idx, 'data']), index=data.index)

    def _compare_idxs_for_update(self, name, idx: pd.Index) -> Tuple[pd.Index, pd.Index]:
        ''' Возвращает пересекающиеся индексы и новые, которые нужно добавить '''

        data = self._get_data(name)

        intersected_idx = idx.intersection(data.index)
        insert_idx = idx.difference(data.index)

        return intersected_idx, insert_idx

    def _update_df(self, name: str, df: pd.DataFrame) -> None:
        data = self._get_data(name)

        if len(df) > 0:
            data.loc[df.index, ['last_run_ts', 'last_run_status']] = df.loc[:, ['last_run_ts', 'last_run_status']]

            good_idx = df.index.difference(df.loc[:, 'data'].isnull())
            if not good_idx.empty:
                data.loc[good_idx, ['data', 'update_ts']] = df.loc[good_idx, ['data', 'update_ts']]
                data.loc[good_idx, 'version'] += 1

    def _insert_df(self, name: str, df: pd.DataFrame) -> None:
        data = self._get_data(name)

        if len(df) > 0:
            self.data[name] = data.append(df)

    def put_success(self, name: str, df: pd.DataFrame) -> None:
        data_df = self._df_to_data_success(df)

        intersected_idx, insert_idx = self._compare_idxs_for_update(name, df.index)

        # TODO возможно, не стоит обновлять объекты, которые фактически не изменились
        # data = self._get_data(name)
        # idx_to_update = intersected_idx[data.loc[intersected_idx, 'data'] != data_df.loc[intersected_idx, 'data']]
        # self._update_data(data_df.loc[idx_to_update])

        self._update_df(name, data_df.loc[intersected_idx])
        self._insert_df(name, data_df.loc[insert_idx])

    def put_failure(self, name: str, idx: pd.Index, reason: str) -> None:
        data_df = self._idx_to_data_failure(idx, reason)

        intersected_idx, insert_idx = self._compare_idxs_for_update(name, idx)
        self._update_df(name, data_df.loc[intersected_idx])

        self._insert_df(name, data_df.loc[insert_idx])

    def what_to_update(self, prev_name: str, next_name: str) -> pd.Index:
        prev_data = self._get_data(prev_name)
        next_data = self._get_data(next_name)

        missing_idx = prev_data.index.difference(next_data.index)

        common_idx = prev_data.index.intersection(next_data.index)

        newer_idx = common_idx[prev_data.loc[common_idx, 'update_ts'] > next_data.loc[common_idx, 'update_ts']]

        return missing_idx.append(newer_idx)



class DataTable(object):
    def __init__(self, ds: DataStore, step_name: str) -> None:
        self.ds = ds
        self.step_name = step_name

    def put_success(self, df: pd.DataFrame) -> None:
        self.ds.put_success(self.step_name, df)

    def put_failure(self, idx: pd.Index, reason: str) -> None:
        self.ds.put_failure(self.step_name, idx, reason)
 
    def get(self) -> pd.DataFrame:
        return self.ds.get_df(self.step_name)

    def get_for_update(self, idxs: pd.Index) -> pd.DataFrame:
        return self._data_to_df(self.data.loc[idxs])


class Source(object):
    def __init__(self, func) -> None:
        self.func = func
    
    def run(self, cur_data: DataTable) -> None:
        try:
            res = self.func()
            cur_data.put_success(res)
        except Exception:
            logger.exception(f'Failed to run source {self.func}')
            # cur_data.put_failure(input_df.index, str(e))


class IncProcess(object):
    def __init__(self, func, inputs) -> None:
        self.func = func
        self.inputs = inputs
    
    def run(self, prev_data: DataTable, cur_data: DataTable) -> None:
        input_idx = prev_data.what_to_update(cur_data)

        if not input_idx.empty:
            input_df = prev_data.get_for_update(input_idx)

            logger.debug(f'Processing {len(input_df)} rows')
            try:
                res = self.func(input_df)
                cur_data.put_success(res)
            except Exception as e:
                cur_data.put_failure(input_df.index, str(e))


class PipeRunner(object):
    def __init__(self, pipeline):
        self.steps = [(name, step) for name, step in pipeline]  # step and its result
        self.data = [(name, DataTable(name)) for name, _ in pipeline]

    def run(self) -> None:
        prev_data = None

        for (name, step), (_, data) in zip(self.steps, self.data):
            logger.debug(f'Running {name}')

            if isinstance(step, Source):
                step.run(data)
            elif isinstance(step, IncProcess):
                assert(prev_data is not None)
                step.run(prev_data, data)

            logger.debug(f'Done {name}')
            logger.debug(f'Result {name}:\n{data.data}')

            prev_data = data

