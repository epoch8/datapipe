from dataclasses import dataclass, field
from typing import Any, List, Dict, Optional

import pandas as pd
from datapipe.types import LabelDict


@dataclass
class RunConfig:
    # Массив глобально применяемых фильтров
    # если не пуст, то во время запуска обрабатываются только те строки,
    # которые строго соответствуют фильтру
    # (в случае, если у таблицы есть идентификатор с совпадающим именем).
    filters: List[LabelDict] = field(default_factory=list)
    labels: LabelDict = field(default_factory=dict)

    @classmethod
    def add_labels(cls, rc: Optional["RunConfig"], labels: LabelDict) -> "RunConfig":
        if rc is not None:
            return RunConfig(
                filters=rc.filters,
                labels={**rc.labels, **labels},
            )
        else:
            return RunConfig(labels=labels)

    @classmethod
    def add_filters(cls, rc: Optional["RunConfig"], filters: List[LabelDict]) -> "RunConfig":
        if rc is not None:
            return RunConfig(
                filters=list(
                    pd.DataFrame(filters)
                    .drop_duplicates()
                    .apply(lambda row : row.dropna().to_dict(), axis=1)
                ),
            )
        else:
            return RunConfig(filters=filters)