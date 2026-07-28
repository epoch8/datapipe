from dataclasses import dataclass, field
from typing import Any, Optional

from datapipe.settings import settings

LabelDict = dict[str, Any]


@dataclass
class RunConfig:
    # Массив глобально применяемых фильтров
    # если не пуст, то во время запуска обрабатываются только те строки,
    # которые строго соответствуют фильтру
    # (в случае, если у таблицы есть идентификатор с совпадающим именем).
    filters: LabelDict = field(default_factory=dict)

    labels: LabelDict = field(default_factory=dict)

    # Если True, то при возникновении ошибки в процессе обработки, выполнение
    # всего процесса будет остановлено
    fail_fast: bool = field(default_factory=lambda: settings.fail_fast)

    @classmethod
    def add_labels(cls, rc: Optional["RunConfig"], labels: LabelDict) -> "RunConfig":
        if rc is not None:
            return RunConfig(
                filters=rc.filters,
                labels={**rc.labels, **labels},
                fail_fast=rc.fail_fast,
            )
        else:
            return RunConfig(labels=labels)
