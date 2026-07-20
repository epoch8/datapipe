from __future__ import annotations

from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app.observability.logging.log_buffer import RunLogBuffer
from datapipe_app.observability.run_logs import RunLogsBackend


def _store(tmp_path, name: str = "obs.db") -> ObservabilityStore:
    return ObservabilityStore.from_url(
        f"sqlite:///{tmp_path / name}",
        run_logs_backend=RunLogsBackend.memory(),
    )


def test_run_logs_persist_without_finish_run(tmp_path):
    store = _store(tmp_path)
    buffer = RunLogBuffer(store)
    run_id = "run-persist"
    buffer.start_run(run_id)
    for i in range(150):
        buffer.append(run_id, "INFO", f"line {i + 1}")

    reloaded = RunLogBuffer(store)
    lines = reloaded.get_lines(run_id)

    assert len(lines) == 150
    assert lines[0].message == "line 1"
    assert lines[-1].message == "line 150"


def test_get_lines_merges_db_and_memory(tmp_path):
    store = _store(tmp_path)
    buffer = RunLogBuffer(store)
    run_id = "run-merge"
    buffer.start_run(run_id)
    buffer.append(run_id, "INFO", "from memory")

    lines = buffer.get_lines(run_id)
    assert len(lines) == 1
    assert lines[0].message == "from memory"
