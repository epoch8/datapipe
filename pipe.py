from __future__ import annotations
from typing import Dict, Tuple, Optional, List, Union, Sequence, Any

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

    def get_for_update(self, prev_name: str, cur_name: str, input_cols: Optional[List[str]]) -> pd.DataFrame:
        ''' Взять данные для обновления (данные шага next_name, которые старше чем соответствующие данные prev_name) '''
        raise NotImplementedError


class Process:
    def __init__(self, input_cols: List[str] = None):
        self.input_cols = input_cols

    def run(self, ds: DataStore, prev_name: Optional[str], cur_name: str) -> None:
        raise NotImplementedError


class Source(Process):
    def __init__(self, func, args: List[Any] = [], kwargs: Dict[str, Any] = {}, input_cols: List[str] = None) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs

        super().__init__(input_cols)
    
    def run(self, ds: DataStore, prev_name: Optional[str], cur_name: str) -> None:
        assert prev_name is None

        try:
            res = self.func(*self.args, **self.kwargs)
            ds.put_success(cur_name, res)
        except Exception:
            logger.exception(f'Failed to run source {self.func}')
            # cur_data.put_failure(input_df.index, str(e))


class IncProcess(Process):
    def __init__(self, func, args: List[Any] = [], kwargs: Dict[str, Any] = {}, input_cols: List[str] = None) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs

        super().__init__(input_cols)

    def run(self, ds: DataStore, prev_name: Optional[str], cur_name: str) -> None:
        assert prev_name is not None

        df = ds.get_for_update(prev_name, cur_name, self.input_cols)

        logger.debug(f'Processing {len(df)} rows: {df.index}')

        if len(df) > 0:
            try:
                res = self.func(df, *self.args, **self.kwargs)
                ds.put_success(cur_name, res)
            except Exception as e:
                logger.exception(f'Failed to run {self.func}(*{self.args}, **{self.kwargs})')
                ds.put_failure(cur_name, df.index, str(e))


class Sample(Process):
    def __init__(self, n: Optional[int] = None, frac: Optional[float] = None, random_state: Optional[int] = 38):
        self.n = n
        self.frac = frac
        self.random_state = random_state

        super().__init__()

    def run(self, ds: DataStore, prev_name: Optional[str], cur_name: str) -> None:
        assert prev_name is not None

        df = ds.get_df(prev_name)
        sample_df = df.sample(n=self.n, frac=self.frac, random_state=self.random_state)

        ds.put_success(cur_name, sample_df)


PipelineType = List[Tuple[str, Process]]


class PipeRunner(object):
    def __init__(self, ds: DataStore, pipeline: PipelineType):
        self.ds = ds

        prev_head: Optional[str] = None
        prev_names: List[Optional[str]] = [prev_head] + [i[0] for i in pipeline[:-1]]

        self.pipeline = [
            (prev_name, cur_name, step) 
            for prev_name, (cur_name, step) 
            in zip(prev_names, pipeline)
        ]

    def run(self) -> pd.DataFrame:
        for prev_name, cur_name, step in self.pipeline:
            logger.debug(f'Running {cur_name}')

            step.run(self.ds, prev_name, cur_name)

            logger.debug(f'Done {cur_name}')
            logger.debug(f'Result {cur_name}:\n{self.ds.get_system_df(cur_name)}')

        return self.ds.get_df(cur_name)