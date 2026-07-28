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


def test_get_lines_pages_beyond_memory_ring(tmp_path):
    """API clients must be able to read seq > MAX_LINES_PER_RUN from DB."""
    from datapipe_app.observability.logging.log_buffer import MAX_LINES_PER_RUN

    store = _store(tmp_path)
    buffer = RunLogBuffer(store)
    run_id = "run-page"
    buffer.start_run(run_id)
    total = MAX_LINES_PER_RUN + 250
    for i in range(total):
        buffer.append(run_id, "INFO", f"line {i + 1}")
    buffer.finish_run(run_id)

    first_page = buffer.get_lines(run_id, after=0, limit=100)
    assert len(first_page) == 100
    assert first_page[0].seq == 1
    assert first_page[-1].seq == 100

    mid = buffer.get_lines(run_id, after=MAX_LINES_PER_RUN, limit=100)
    assert len(mid) == 100
    assert mid[0].seq == MAX_LINES_PER_RUN + 1
    assert mid[-1].message == f"line {MAX_LINES_PER_RUN + 100}"

    assert buffer.get_max_seq(run_id) == total


def test_get_lines_merges_db_and_memory(tmp_path):
    store = _store(tmp_path)
    buffer = RunLogBuffer(store)
    run_id = "run-merge"
    buffer.start_run(run_id)
    buffer.append(run_id, "INFO", "from memory")

    lines = buffer.get_lines(run_id)
    assert len(lines) == 1
    assert lines[0].message == "from memory"
