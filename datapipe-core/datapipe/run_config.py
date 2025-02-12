from dataclasses import dataclass, field
from typing import Any, Dict, Optional

LabelDict = Dict[str, Any]


@dataclass
class RunConfig:
    # Массив глобально применяемых фильтров
    # если не пуст, то во время запуска обрабатываются только те строки,
    # которые строго соответствуют фильтру
    # (в случае, если у таблицы есть идентификатор с совпадающим именем).
    filters: LabelDict = field(default_factory=dict)
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
