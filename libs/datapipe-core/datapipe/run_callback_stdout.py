import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from datapipe.compute import ComputeStep


def _format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.2f}s"

    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"

    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


class StdoutRunCallback:
    """Prints throttled step-progress lines to stdout, no third-party dependency."""

    def __init__(self, min_interval: float = 5.0) -> None:
        self.min_interval = min_interval
        self._last_printed: dict[str, float] = {}
        self._step_start: dict[str, float] = {}

    def on_run_start(self, steps: "Sequence[ComputeStep]") -> None:
        pass

    def on_step_start(self, step: "ComputeStep") -> None:
        self._last_printed.pop(step.name, None)
        self._step_start[step.name] = time.monotonic()

    def on_step_progress(self, step: "ComputeStep", completed: int, total: int | None) -> None:
        now = time.monotonic()
        last = self._last_printed.get(step.name)
        is_edge = completed == 0 or completed == total
        if last is not None and not is_edge and now - last < self.min_interval:
            return

        self._last_printed[step.name] = now
        suffix = f"/{total}" if total is not None else ""
        line = f"{step.name}: {completed}{suffix}"

        start = self._step_start.get(step.name)
        if start is not None and completed > 0:
            avg = (now - start) / completed
            line += f" (avg {_format_duration(avg)}/it"
            if total is not None:
                line += f", ETA {_format_duration(avg * (total - completed))}"
            line += ")"

        print(line, flush=True)

    def on_step_success(self, step: "ComputeStep") -> None:
        self._last_printed.pop(step.name, None)
        self._step_start.pop(step.name, None)

    def on_step_error(self, step: "ComputeStep", error: BaseException) -> None:
        self._last_printed.pop(step.name, None)
        self._step_start.pop(step.name, None)

    def on_run_success(self) -> None:
        pass

    def on_run_error(self, error: BaseException) -> None:
        pass
