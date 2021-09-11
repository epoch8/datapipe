from typing import List, NewType

import pandas as pd
from sqlalchemy import Column

Index = pd.DataFrame
ChunkMeta = Index

DataSchema = List[Column]

# Dataframe with columns (<index_cols ...>, hash, create_ts, update_ts, process_ts, delete_ts)
MetadataDF = NewType('MetadataDF', pd.DataFrame)

# Dataframe with columns (<index_cols ...>, <data_cols ...>)
DataDF = NewType('DataDF', pd.DataFrame)
