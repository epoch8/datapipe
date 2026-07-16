from __future__ import annotations

from datapipe_app.observability.logging.log_buffer import RunLogBuffer
from datapipe_app.observability.runs.recorder import RunRecorder
from datapipe_app.observability.store.db import ObservabilityStore


def test_update_step_progress_writes_throttled_log_lines(tmp_path):
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    buffer = RunLogBuffer(store)
    recorder = RunRecorder(store, pipeline_id="pipe", log_buffer=buffer)

    run_id = recorder.start_run(trigger="test")
    recorder.start_step("my_step", run_id=run_id)
    recorder.update_step_progress("my_step", processed=0, total=10, run_id=run_id)
    recorder.update_step_progress("my_step", processed=1, total=10, run_id=run_id)
    recorder.update_step_progress("my_step", processed=2, total=10, run_id=run_id)
    recorder.update_step_progress("my_step", processed=10, total=10, run_id=run_id)
    recorder.finish_step("my_step", status="completed", run_id=run_id)
    recorder.finish_run(status="completed", run_id=run_id)

    messages = [line.message for line in buffer.get_lines(run_id)]
    progress = [m for m in messages if m.startswith("Progress: my_step")]
    assert progress[0] == "Progress: my_step 0/10 (0%)"
    assert progress[-1] == "Progress: my_step 10/10 (100%)"
    # Intermediate updates within the throttle window are collapsed.
    assert len(progress) >= 2
    assert len(progress) <= 3
