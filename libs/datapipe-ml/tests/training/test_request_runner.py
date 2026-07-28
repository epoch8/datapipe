from __future__ import annotations

import pandas as pd

from datapipe_ml.training.request_runner import run_training_request

TRAIN_CONFIG_ID_COL = "detection_train_config_id"
TRAIN_CONFIG_PARAMS_COL = "detection_train_config__params"


class _FakeStatusDT:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def get_data(self, idx=None):
        return self._df


def _request_df(
    *,
    kind="auto",
    enabled=True,
    force=False,
    max_within_time="1w",
    request_id="auto_1",
    config_id="cfg1",
    params=None,
):
    return pd.DataFrame(
        [
            {
                "training_request_id": request_id,
                TRAIN_CONFIG_ID_COL: config_id,
                "training_request__kind": kind,
                "training_request__enabled": enabled,
                "training_request__force": force,
                "training_request__max_within_time": max_within_time,
                "training_request__config_params_snapshot": params or {"imgsz": 640, "epochs": 5},
            }
        ]
    )


def _run(df_req, train_callable, **kwargs):
    return run_training_request(
        pd.DataFrame([{"detection_frozen_dataset_id": "fd1"}]),
        df_req,
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        train_callable=train_callable,
        train_config_id_col=TRAIN_CONFIG_ID_COL,
        train_config_params_col=TRAIN_CONFIG_PARAMS_COL,
        **kwargs,
    )


def _nonempty_outputs():
    return (
        pd.DataFrame([{"detection_model_id": "m1"}]),
        pd.DataFrame([{"detection_model_id": "m1"}]),
        pd.DataFrame([{"training_status_id": "s1"}]),
    )


def test_request_runner_builds_legacy_config_dataframe():
    captured = {}

    def fake_train(*, df_train_config, max_within_time, force_training, **kw):
        captured["df_train_config"] = df_train_config
        return _nonempty_outputs()

    _run(_request_df(params={"imgsz": 640, "epochs": 5}), fake_train)

    dtc = captured["df_train_config"]
    assert list(dtc.columns) == [TRAIN_CONFIG_ID_COL, TRAIN_CONFIG_PARAMS_COL]
    assert dtc.iloc[0][TRAIN_CONFIG_ID_COL] == "cfg1"
    assert dtc.iloc[0][TRAIN_CONFIG_PARAMS_COL] == {"imgsz": 640, "epochs": 5}


def test_request_id_is_written_to_all_outputs():
    def fake_train(**kw):
        return _nonempty_outputs()

    out = _run(_request_df(request_id="auto_abc"), fake_train)
    assert len(out) == 3
    for df in out:
        assert not df.empty
        assert (df["training_request_id"] == "auto_abc").all()


def test_auto_request_uses_max_within_time():
    captured = {}

    def fake_train(*, max_within_time, force_training, **kw):
        captured["max_within_time"] = max_within_time
        captured["force_training"] = force_training
        return _nonempty_outputs()

    _run(_request_df(kind="auto", max_within_time="2w", force=False), fake_train)
    assert captured["max_within_time"] == "2w"
    assert captured["force_training"] is False


def test_manual_request_forces_training_when_force_flag_set():
    captured = {}

    def fake_train(*, max_within_time, force_training, **kw):
        captured["max_within_time"] = max_within_time
        captured["force_training"] = force_training
        return _nonempty_outputs()

    _run(
        _request_df(kind="manual", max_within_time="1w", force=True, request_id="request_1"),
        fake_train,
    )
    assert captured["max_within_time"] is None
    assert captured["force_training"] is True


def test_manual_request_respects_max_within_time_without_force():
    captured = {}

    def fake_train(*, max_within_time, force_training, **kw):
        captured["max_within_time"] = max_within_time
        captured["force_training"] = force_training
        return _nonempty_outputs()

    _run(
        _request_df(kind="manual", max_within_time="1w", force=False, request_id="request_1"),
        fake_train,
    )
    assert captured["max_within_time"] == "1w"
    assert captured["force_training"] is False


def test_disabled_request_returns_empty_without_training():
    calls = {"n": 0}

    def fake_train(**kw):
        calls["n"] += 1
        return _nonempty_outputs()

    out = _run(_request_df(enabled=False), fake_train)
    assert calls["n"] == 0
    assert all(df.empty for df in out)


def test_manual_request_is_one_shot():
    calls = {"n": 0}

    def fake_train(**kw):
        calls["n"] += 1
        return _nonempty_outputs()

    status_df = pd.DataFrame(
        [{"training_request_id": "request_1", "training_status__status": "completed"}]
    )
    out = _run(
        _request_df(kind="manual", max_within_time=None, request_id="request_1"),
        fake_train,
        dt_training_status=_FakeStatusDT(status_df),
    )
    assert calls["n"] == 0
    assert all(df.empty for df in out)


def test_manual_request_runs_when_no_prior_status():
    calls = {"n": 0}

    def fake_train(**kw):
        calls["n"] += 1
        return _nonempty_outputs()

    status_df = pd.DataFrame(
        [{"training_request_id": "other_request", "training_status__status": "completed"}]
    )
    out = _run(
        _request_df(kind="manual", max_within_time=None, request_id="request_1"),
        fake_train,
        dt_training_status=_FakeStatusDT(status_df),
    )
    assert calls["n"] == 1
    assert (out[0]["training_request_id"] == "request_1").all()


def test_request_runner_scopes_related_frames_to_frozen_dataset():
    captured = {}
    fd_col = "detection_frozen_dataset_id"

    def fake_train(**kw):
        captured["df_frozen"] = kw["df_frozen_dataset"]
        captured["df_img"] = kw["df_resized_images"]
        captured["df_txt"] = kw["df_yolo_txt"]
        captured["df_cls"] = kw["df_class_names"]
        return _nonempty_outputs()

    req = _request_df(request_id="auto_1")
    req[fd_col] = "fd_keep"
    df_fd = pd.DataFrame(
        [
            {fd_col: "fd_keep", "n": 1},
            {fd_col: "fd_other", "n": 2},
        ]
    )
    df_cls = pd.DataFrame(
        [
            {fd_col: "fd_keep", "class_names": ["a"]},
            {fd_col: "fd_other", "class_names": ["b"]},
        ]
    )
    df_img = pd.DataFrame(
        [
            {fd_col: "fd_keep", "filepath": "keep.jpg"},
            {fd_col: "fd_other", "filepath": "other.jpg"},
        ]
    )
    df_txt = df_img.copy()

    run_training_request(
        df_fd,
        req,
        df_cls,
        df_img,
        df_txt,
        train_callable=fake_train,
        train_config_id_col=TRAIN_CONFIG_ID_COL,
        train_config_params_col=TRAIN_CONFIG_PARAMS_COL,
        frozen_dataset_id_col=fd_col,
    )

    assert list(captured["df_frozen"][fd_col]) == ["fd_keep"]
    assert list(captured["df_cls"][fd_col]) == ["fd_keep"]
    assert list(captured["df_img"][fd_col]) == ["fd_keep"]
    assert list(captured["df_txt"][fd_col]) == ["fd_keep"]
