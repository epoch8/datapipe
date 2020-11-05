from typing import Dict, Optional, Tuple, List

import time
import pandas as pd

from pipe import DataStore

class MemoryDataStore(DataStore):
    '''
    Для каждого шага MDS хранит данные в двух DataFrame:

    `data` - фактические данные, структура определяется нодой

    `metadata` - метаданные
        * `update_ts` - таймстемп обновления данных (если после запуска данные не изменились, то не меняется)
        * TODO `last_successful_run_ts` - таймстемп последнего успешного запуска, не обязательно приведшего к обновлению
        * `last_run_ts` - таймстемп последнего запуска (любого успешного или нет)
        * `last_run_status` - статус последнего запуска `success`/`fail`
        * `last_run_reason` - сообщение об ошибке, если последний запуск был `fail`
        * `version` - целое число, версия данных, инкрементируется при каждом изменении

    '''
    def __init__(self):
        self.data: Dict[str, pd.DataFrame] = {}
        self.metadata: Dict[str, pd.DataFrame] = {}

    def get_system_df(self, name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        '''
        Returns: Tuple(data, metadata)
        '''

        if name not in self.metadata:
            self.data[name] = pd.DataFrame()
            self.metadata[name] = pd.DataFrame(
                {
                    'update_ts': pd.Series([], dtype='float'),
                    'version': pd.Series([], dtype='int'),
                    'last_run_ts': pd.Series([], dtype='float'),
                    'last_run_status': pd.Series([], dtype='string'),
                    'last_run_reason': pd.Series([], dtype='string'),
                }
            )

        return self.data[name], self.metadata[name]

    def get_debug_df(self, name: str) -> pd.DataFrame:
        data_df, metadata_df = self.get_system_df(name)

        return pd.concat([data_df, metadata_df], keys=['data', 'metadata'], axis='columns')

    def get_df(self, name, idx: Optional[pd.Index] = None, cols: List[str] = None) -> pd.DataFrame:
        data, _ = self.get_system_df(name)

        if idx is None:
            idx = data.index
        
        if cols is None:
            cols = data.columns.values

        return data.loc[idx, cols]

    def _idx_to_metadata_success(self, idx: pd.Index) -> pd.DataFrame:
        ts = time.time()
        metadata_df = pd.DataFrame(
            {
                'version': 1,
                'update_ts': ts,
                'last_run_ts': ts,
                'last_run_status': 'success',
                'last_run_reason': '',
            },
            index=idx
        )

        return metadata_df

    def _idx_to_data_failure(self, idx: pd.Index, reason: str) -> pd.DataFrame:
        ts = time.time()
        data_df = pd.DataFrame(
            {
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

        _, metadata = self.get_system_df(name)

        intersected_idx = idx.intersection(metadata.index)
        insert_idx = idx.difference(metadata.index)

        return intersected_idx, insert_idx

    def _update_metadata(self, name: str, update_metadata_df: pd.DataFrame) -> None:
        _, metadata_df = self.get_system_df(name)

        if len(update_metadata_df) > 0:
            metadata_df.loc[
                update_metadata_df.index, 
                ['last_run_ts', 'last_run_status', 'last_run_reason']
            ] = update_metadata_df.loc[
                :, 
                ['last_run_ts', 'last_run_status', 'last_run_reason']
            ]

    def _update_data(self, name: str, update_data_df: pd.DataFrame, update_metadata_df: pd.DataFrame) -> None:
        data_df, metadata_df = self.get_system_df(name)

        if len(update_data_df) > 0:
            # TODO переделать на ==
            changed_idx = data_df.loc[update_data_df.index]\
                .compare(update_data_df, align_axis='index').index\
                .get_level_values(0).unique()

            data_df.loc[changed_idx] = update_data_df.loc[changed_idx]
            metadata_df.loc[changed_idx, 'version'] += 1

    def _insert_metadata(self, name: str, insert_metadata_df: pd.DataFrame) -> None:
        _, metadata_df= self.get_system_df(name)

        if len(insert_metadata_df) > 0:
            self.metadata[name] = metadata_df.append(insert_metadata_df)

    def _insert_data(self, name: str, insert_data_df: pd.DataFrame, insert_metadata_df: pd.DataFrame) -> None:
        data_df, _ = self.get_system_df(name)

        if len(insert_data_df) > 0:
            self.data[name] = data_df.append(insert_data_df)

    def put_success(self, name: str, df: pd.DataFrame) -> None:
        metadata_df = self._idx_to_metadata_success(df.index)

        intersected_idx, insert_idx = self._compare_idxs_for_update(name, metadata_df.index)

        self._update_metadata(name, metadata_df.loc[intersected_idx])
        self._update_data(name, df.loc[intersected_idx], metadata_df.loc[intersected_idx])

        self._insert_metadata(name, metadata_df.loc[insert_idx])
        self._insert_data(name, df.loc[insert_idx], metadata_df.loc[insert_idx])

    def put_failure(self, name: str, idx: pd.Index, reason: str) -> None:
        metadata_df = self._idx_to_data_failure(idx, reason)

        intersected_idx, insert_idx = self._compare_idxs_for_update(name, metadata_df.index)

        self._update_metadata(name, metadata_df.loc[intersected_idx])

        self._insert_metadata(name, metadata_df.loc[insert_idx])

    def what_to_update(self, prev_name: str, next_name: str) -> pd.Index:
        prev_data_df, prev_metadata_df = self.get_system_df(prev_name)
        next_data_df, next_metadata_df = self.get_system_df(next_name)

        missing_idx = prev_metadata_df.index.difference(next_metadata_df.index)

        common_idx = prev_metadata_df.index.intersection(next_metadata_df.index)

        newer_idx = common_idx[prev_metadata_df.loc[common_idx, 'update_ts'] > next_metadata_df.loc[common_idx, 'update_ts']]

        return missing_idx.append(newer_idx)

    def get_for_update(self, prev_name: str, next_name: str, input_cols: Optional[List[str]]) -> pd.DataFrame:
        idx = self.what_to_update(prev_name, next_name)

        return self.get_df(prev_name, idx, input_cols)
