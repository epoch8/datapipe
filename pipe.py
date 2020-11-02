from __future__ import annotations
from typing import Dict, Tuple, Optional, List, Union

import pandas as pd
import time
import logging


logger = logging.getLogger(__name__)


class DataStore:
    def __init__(self):
        pass

    def get_system_df(self, name: str) -> pd.DataFrame:
        ''' Получить датафрейм с системной информацией '''
        raise NotImplementedError

    def get_df(self, name: str, idx: Optional[pd.Index] = None) -> pd.DataFrame:
        ''' Получить датафрейм с информацией приложения по заданным индексам '''
        raise NotImplementedError

    def put_success(self, name: str, df: pd.DataFrame) -> None:
        ''' Записать успешный чанк '''
        raise NotImplementedError

    def put_failure(self, name: str, idx: pd.Index, reason: str) -> None:
        ''' Записать проваленный чанк '''
        raise NotImplementedError

    def get_for_update(self, prev_name: str, next_name: str) -> pd.DataFrame:
        ''' Взять данные для обновления (данные шага next_name, которые старше чем соответствующие данные prev_name) '''
        raise NotImplementedError


class Source:
    def __init__(self, func) -> None:
        self.func = func
    
    def run(self, name: str, ds: DataStore) -> None:
        try:
            res = self.func()
            ds.put_success(name, res)
        except Exception:
            logger.exception(f'Failed to run source {self.func}')
            # cur_data.put_failure(input_df.index, str(e))


class IncProcess:
    def __init__(self, func, inputs) -> None:
        self.func = func
        self.inputs = inputs
    
    def run(self, name: str, ds: DataStore, df: pd.DataFrame) -> None:
        logger.debug(f'Processing {len(df)} rows: {df.index}')
        try:
            res = self.func(df)
            ds.put_success(name, res)
        except Exception as e:
            ds.put_failure(name, df.index, str(e))


PipelineType = List[Tuple[str, Union[Source, IncProcess]]]


class PipeRunner(object):
    def __init__(self, ds: DataStore, pipeline: PipelineType):
        self.ds = ds
        self.pipeline = pipeline

    def run(self) -> None:
        prev_name = None

        for cur_name, step in self.pipeline:
            logger.debug(f'Running {cur_name}')

            if isinstance(step, Source):
                step.run(cur_name, self.ds)
            elif isinstance(step, IncProcess):
                assert(prev_name is not None)

                df = self.ds.get_for_update(prev_name, cur_name)

                step.run(cur_name, self.ds, df)

            logger.debug(f'Done {cur_name}')
            logger.debug(f'Result {cur_name}:\n{self.ds.get_system_df(cur_name)}')

            prev_name = cur_name

