from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.tensorflow


def test_fsspec_model_checkpoint_mirrors_asynchronously(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pytest.importorskip("tensorflow")
    from datapipe_ml.frameworks.tensorflow.callbacks import FsspecModelCheckpoint

    remote_url = str(tmp_path / "remote" / "best.keras")

    started = threading.Event()
    release = threading.Event()
    copied_urls: list[str] = []

    def slow_copy(src: str, dst: str, *, label: str = "file", concurrency: int = 8) -> None:
        copied_urls.append(dst)
        started.set()
        assert release.wait(timeout=5.0)
        dst_path = Path(dst)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        dst_path.write_bytes(Path(src).read_bytes())

    monkeypatch.setattr(
        "datapipe_ml.frameworks.tensorflow.callbacks.copy_url_to_url",
        slow_copy,
    )

    callback = FsspecModelCheckpoint(remote_filepath_pattern=remote_url, mirror_async=True)
    local_ckpt = Path(callback._tmpdir.name) / "best.keras"
    local_ckpt.write_bytes(b"checkpoint")
    callback._mirror_if_exists(epoch=0, logs={})

    assert started.wait(timeout=5.0)
    assert not Path(remote_url).exists()

    release.set()
    callback._wait_for_mirror_threads()

    assert Path(remote_url).read_bytes() == b"checkpoint"
    assert copied_urls == [remote_url]


def test_fsspec_model_checkpoint_waits_for_pending_uploads_on_train_end(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pytest.importorskip("tensorflow")
    from datapipe_ml.frameworks.tensorflow.callbacks import FsspecModelCheckpoint

    remote_url = str(tmp_path / "remote" / "best.keras")

    upload_started = threading.Event()
    release = threading.Event()

    def slow_copy(src: str, dst: str, *, label: str = "file", concurrency: int = 8) -> None:
        upload_started.set()
        assert release.wait(timeout=5.0)

    monkeypatch.setattr(
        "datapipe_ml.frameworks.tensorflow.callbacks.copy_url_to_url",
        slow_copy,
    )

    callback = FsspecModelCheckpoint(remote_filepath_pattern=remote_url, mirror_async=True)
    local_ckpt = Path(callback._tmpdir.name) / "best.keras"
    local_ckpt.write_bytes(b"checkpoint")
    callback._mirror_if_exists(epoch=0, logs={})
    assert upload_started.wait(timeout=5.0)

    train_end_finished = threading.Event()

    def finish_training() -> None:
        callback.on_train_end()
        train_end_finished.set()

    thread = threading.Thread(target=finish_training)
    thread.start()

    time.sleep(0.2)
    assert not train_end_finished.is_set()

    release.set()
    thread.join(timeout=5.0)
    assert train_end_finished.is_set()
