from typing import List, NewType

import pandas as pd
from sqlalchemy import Column

Index = pd.DataFrame
ChunkMeta = Index

DataSchema = List[Column]

DataChunk = NewType('DataChunk', pd.DataFrame)
MetadataChunk = NewType('MetadataChunk', pd.DataFrame)
