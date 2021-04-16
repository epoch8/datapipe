from typing import Union, List

import pandas as pd
from sqlalchemy import Column

Index = Union[List, pd.Index]
ChunkMeta = Index

DataSchema = List[Column]
