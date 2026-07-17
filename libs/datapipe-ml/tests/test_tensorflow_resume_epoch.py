from __future__ import annotations

import pytest


@pytest.mark.tensorflow
def test_initial_epoch_prefers_manifest_epoch_over_filename() -> None:
    pytest.importorskip("tensorflow")
    from datapipe_ml.frameworks.tensorflow.classification_runner import _initial_epoch_from_resume

    assert _initial_epoch_from_resume("s3://bucket/models/run/99__val_loss.keras", 12) == 12
    assert _initial_epoch_from_resume("s3://bucket/models/run/99__val_loss.keras", None) == 99
    assert _initial_epoch_from_resume(None, None) == 0
    assert _initial_epoch_from_resume("s3://bucket/models/run/bad-name.keras", None) == 0
    assert _initial_epoch_from_resume("/fake/path/models/run/99__val_loss.keras", None) == 99
    assert _initial_epoch_from_resume("/fake/path/models/run/bad-name.keras", None) == 0
    assert _initial_epoch_from_resume("/tmp/path/models/run/99__val_loss.keras", None) == 99
