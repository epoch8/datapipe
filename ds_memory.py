from typing import Dict, Optional, Tuple, List

import time
import pandas as pd

from pipe import DataStore

class MemoryDataStore(DataStore):
    def __init__(self):
        self.data: Dict[str, pd.DataFrame] = {}

    def get_system_df(self, name: str) -> pd.DataFrame:
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

    def get_df(self, name, idx: Optional[pd.Index] = None, cols: List[str] = None) -> pd.DataFrame:
        data = self.get_system_df(name)

        if idx is None:
            idx = data.index

        data = data.loc[idx]

        return self._data_to_df(data, cols)

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

    def _data_to_df(self, data: pd.DataFrame, cols: Optional[List[str]] = None) -> pd.DataFrame:
        bad_idx = data['data'].isnull()

        data_to_convert = data.loc[-bad_idx]

        if len(data_to_convert) > 0:
            df = pd.DataFrame.from_records(list(data_to_convert.loc[:, 'data']), index=data_to_convert.index)

            if cols is None:
                cols = df.columns.values
            
            return df.loc[:, cols]
        else:
            return pd.DataFrame(columns=cols)

    def _compare_idxs_for_update(self, name, idx: pd.Index) -> Tuple[pd.Index, pd.Index]:
        ''' Возвращает пересекающиеся индексы и новые, которые нужно добавить '''

        data = self.get_system_df(name)

        intersected_idx = idx.intersection(data.index)
        insert_idx = idx.difference(data.index)

        return intersected_idx, insert_idx

    def _update_df(self, name: str, df: pd.DataFrame) -> None:
        data = self.get_system_df(name)

        if len(df) > 0:
            data.loc[df.index, ['last_run_ts', 'last_run_status', 'last_run_reason']] = df.loc[:, ['last_run_ts', 'last_run_status', 'last_run_reason']]

            changed_idx = df.index[data.loc[df.index, 'data'] != df.loc[:, 'data']]
            good_idx = changed_idx.difference(df.loc[changed_idx, 'data'].isnull())

            if not good_idx.empty:
                data.loc[good_idx, ['data', 'update_ts']] = df.loc[good_idx, ['data', 'update_ts']]
                data.loc[good_idx, 'version'] += 1

    def _insert_df(self, name: str, df: pd.DataFrame) -> None:
        data = self.get_system_df(name)

        if len(df) > 0:
            self.data[name] = data.append(df)

    def put_success(self, name: str, df: pd.DataFrame) -> None:
        data_df = self._df_to_data_success(df)

        intersected_idx, insert_idx = self._compare_idxs_for_update(name, df.index)

        self._update_df(name, data_df.loc[intersected_idx])
        self._insert_df(name, data_df.loc[insert_idx])

    def put_failure(self, name: str, idx: pd.Index, reason: str) -> None:
        data_df = self._idx_to_data_failure(idx, reason)

        intersected_idx, insert_idx = self._compare_idxs_for_update(name, idx)
        self._update_df(name, data_df.loc[intersected_idx])

        self._insert_df(name, data_df.loc[insert_idx])

    def what_to_update(self, prev_name: str, next_name: str) -> pd.Index:
        prev_data = self.get_system_df(prev_name)
        next_data = self.get_system_df(next_name)

        missing_idx = prev_data.index.difference(next_data.index)

        common_idx = prev_data.index.intersection(next_data.index)

        newer_idx = common_idx[prev_data.loc[common_idx, 'update_ts'] > next_data.loc[common_idx, 'update_ts']]

        return missing_idx.append(newer_idx)

    def get_for_update(self, prev_name: str, next_name: str, input_cols: Optional[List[str]]) -> pd.DataFrame:
        idx = self.what_to_update(prev_name, next_name)

        return self.get_df(prev_name, idx, input_cols)
