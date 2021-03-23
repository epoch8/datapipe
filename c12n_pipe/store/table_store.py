from abc import ABC
from typing import Optional

import pandas as pd

from c12n_pipe.store.types import Index


class TableStore(ABC):
    def delete_rows(self, idx: Index) -> None:
        raise NotImplemented
    
    def insert_rows(self, df: pd.DataFrame) -> None:
        raise NotImplemented
    
    def update_rows(self, df: pd.DataFrame) -> None:
        self.delete_rows(df.index)
        self.insert_rows(df)
    
    def read_rows(self, idx: Optional[Index] = None) -> pd.DataFrame:
        raise NotImplemented


