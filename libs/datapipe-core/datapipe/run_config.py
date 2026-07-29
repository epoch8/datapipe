from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from datapipe.settings import settings

if TYPE_CHECKING:
    from datapipe.run_callback import RunCallback

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

    # Callback for run/step lifecycle events, including step progress.
    # Use CompositeRunCallback to attach more than one.
    callback: "RunCallback | None" = None

    @classmethod
    def add_labels(cls, rc: "RunConfig | None", labels: LabelDict) -> "RunConfig":
        if rc is not None:
            return replace(rc, labels={**rc.labels, **labels})
        else:
            return RunConfig(labels=labels)

    @classmethod
    def with_callback(cls, rc: "RunConfig | None", callback: "RunCallback") -> "RunConfig":
        if rc is not None:
            return replace(rc, callback=callback)
        else:
            return RunConfig(callback=callback)
