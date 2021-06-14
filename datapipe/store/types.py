from typing import List

import pandas as pd
from sqlalchemy import Column

# тип данных с перечислением индексных колонок
# в дальнейшем может быть расширен типизацией каждого индекса
# на текущий момент предполагается, что все индексы строковые
IndexMeta = List[str]

# данные со значениями индексов для конкретных строк
Index = pd.DataFrame

ChunkMeta = Index

DataSchema = List[Column]
