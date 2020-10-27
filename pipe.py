from __future__ import annotations

import pandas as pd
import time
import logging


logger = logging.getLogger(__name__)


class DataTable(object):
    def __init__(self, step_name: str) -> None:
        self.data = pd.DataFrame(
            {
                'data': pd.Series([], dtype='object'),
                'update_ts': pd.Series([], dtype='float'),
                'version': pd.Series([], dtype='int'),
                'last_run_ts': pd.Series([], dtype='float'),
                'last_run_status': pd.Series([], dtype='string'),
                'last_run_reason': pd.Series([], dtype='string'),
            }
        )

    def put_success(self, df: pd.DataFrame) -> None:
        data_df = self._df_to_data(df)

        intersected_idx = data_df.index.intersection(self.data.index)

        idx_to_update = intersected_idx[self.data.loc[intersected_idx, 'data'] != data_df.loc[intersected_idx, 'data']]
        self._update_df(data_df.loc[idx_to_update])

        idx_to_insert = data_df.index.difference(self.data.index)
        self._insert_df(data_df.loc[idx_to_insert])

    def put_failure(self, idx: pd.Index, reason: str) -> None:
        data_df = self._idx_to_data_failure(idx, reason)

        idx_to_update = idx.intersection(self.data.index)
        self._update_df(data_df.loc[idx_to_update])

        idx_to_insert = idx.difference(self.data.index)
        self._insert_df(data_df.loc[idx_to_insert])

    def _update_df(self, df: pd.DataFrame) -> None:
        if len(df) > 0:
            self.data.loc[df.index, ['last_run_ts', 'last_run_status']] = df.loc[:, ['last_run_ts', 'last_run_status']]

            good_idx = df.index.difference(df.loc[:, 'data'].isnull())
            if not good_idx.empty:
                self.data.loc[good_idx, ['data', 'update_ts']] = df.loc[good_idx, ['data', 'update_ts']]
                self.data.loc[good_idx, 'version'] += 1

    def _insert_df(self, df: pd.DataFrame) -> None:
        if len(df) > 0:
            self.data = self.data.append(df)

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

    def _df_to_data(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def _data_to_df(self, data: pd.DataFrame) -> pd.DataFrame:
        bad_idx = data['data'].isnull()

        return pd.DataFrame.from_records(list(data.loc[-bad_idx, 'data']), index=data.index)

    def get(self) -> pd.DataFrame:
        return self._data_to_df(self.data)

    def what_to_update(self, update_ts_df: DataTable) -> pd.Index:
        '''
        Calculate indexes from this DT that needs to be reprocessed
        '''

        missing_idx = self.data.index.difference(update_ts_df.data.index)

        common_idx = self.data.index.intersection(update_ts_df.data.index)

        newer_idx = common_idx[self.data.loc[common_idx, 'update_ts'] > update_ts_df.data.loc[common_idx, 'update_ts']]

        return missing_idx.append(newer_idx)

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

