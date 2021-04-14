from abc import ABC
from typing import Optional

import pandas as pd

from datapipe.store.types import Index


class TableDataStore(ABC):
    def delete_rows(self, idx: Index) -> None:
        raise NotImplemented
    
    def insert_rows(self, df: pd.DataFrame) -> None:
        raise NotImplemented
    
    def update_rows(self, df: pd.DataFrame) -> None:
        self.delete_rows(df.index)
        self.insert_rows(df)
    
    def read_rows(self, idx: Optional[Index] = None) -> pd.DataFrame:
        raise NotImplemented

    def read_rows_meta_pseudo_df(self, idx: Optional[Index] = None) -> pd.DataFrame:
        '''
        Подготовить датафрейм с "какбы данными" на основе которых посчитается хеш и обновятся метаданные
        '''
        raise NotImplemented
