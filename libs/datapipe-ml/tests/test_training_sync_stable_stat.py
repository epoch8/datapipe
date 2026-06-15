from __future__ import annotations

import pytest

from datapipe_ml.training import sync as sync_module


def test_stable_stat_rejects_changing_file(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_stat(_url: str):
        calls["count"] += 1
        if calls["count"] == 1:
            return (100, 1.0)
        if calls["count"] == 2:
            return (200, 2.0)
        return (200, 2.0)

    monkeypatch.setattr(sync_module, "_stat_url", fake_stat)
    monkeypatch.setattr(sync_module.time, "sleep", lambda *_args, **_kwargs: None)

    with pytest.raises(RuntimeError, match="Checkpoint changed while being inspected"):
        sync_module._stable_stat("s3://bucket/checkpoint.pt", max_attempts=1)
