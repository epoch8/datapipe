from __future__ import annotations

import pytest

pytest.importorskip("datapipe_ml")

from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app.observability.logging.log_buffer import RunLogBuffer
from datapipe_app.observability.plugins.registry import ObservabilityRegistry, load_observability_plugins
from datapipe_app.observability.runs.run_output_capture import capture_run_output
from datapipe_app_ml_ops.observability.training.training_service import TrainingService
from datapipe_ml.core.multiprocessing import _spawn


def _subprocess_print_child(queue, message: str) -> None:
    import sys

    print(message, flush=True)
    sys.stdout.flush()
    queue.put("ok")


def test_ml_observability_plugin_registers_enricher_on_attach() -> None:
    from datapipe_app_ml_ops.ops.ops_specs import DatapipeOpsSpec, OpsDataSpec
    from datapipe_app_ml_ops.ops.spec_registry import OpsSpecRegistry

    registry = ObservabilityRegistry()
    load_observability_plugins(registry)

    assert registry.collectors == []
    assert registry.enrichers == []

    ops_specs = OpsSpecRegistry()
    ops_specs.add_many([DatapipeOpsSpec(id="demo", title="Demo", data=OpsDataSpec(tables=[]))])
    registry.attach_ops_specs(ops_specs)

    assert len(registry.enrichers) == 1


def test_training_service_without_specs_returns_empty(tmp_path) -> None:
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs.db'}")
    service = TrainingService(store=store, ds=None, catalog=None)

    response = service.list_runs("demo", limit=10)
    assert response.total == 0
    assert response.rows == []


def test_capture_run_output_records_subprocess_stdout(tmp_path) -> None:
    store = ObservabilityStore.from_url(f"sqlite:///{tmp_path / 'obs3.db'}")
    buffer = RunLogBuffer(store)
    run_id = "run-3"
    buffer.start_run(run_id)

    with capture_run_output(buffer, run_id):
        assert _spawn(_subprocess_print_child, "Ultralytics epoch 1/30") == "ok"

    messages = [line.message for line in buffer.get_lines(run_id)]
    assert "Ultralytics epoch 1/30" in messages
