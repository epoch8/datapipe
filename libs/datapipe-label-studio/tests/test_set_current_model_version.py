from unittest.mock import MagicMock

import pandas as pd
import pytest

from datapipe_label_studio.upload_predictions_pipeline import (
    LabelStudioUploadPredictions,
    _assign_model_version_column,
    _compose_model_version,
    _validate_prediction_primary_keys,
    set_label_studio_current_model_version,
)


def test_set_label_studio_current_model_version_enables_prelabeling():
    ls = MagicMock()
    get_project_context = MagicMock(return_value=(ls, 42))
    df__best_model = pd.DataFrame({"model_version": ["cat_dog_yolo_smoke"]})

    result = set_label_studio_current_model_version(
        df__best_model=df__best_model,
        idx=pd.DataFrame(),
        get_project_context=get_project_context,
        model_keys=["model_version"],
    )

    assert result.to_dict(orient="records") == [
        {"project_id": 42, "model_version": "cat_dog_yolo_smoke"}
    ]
    ls.projects.update.assert_called_once_with(
        id=42,
        show_collab_predictions=True,
        model_version="cat_dog_yolo_smoke",
    )


def test_set_label_studio_current_model_version_supports_multiple_model_keys():
    ls = MagicMock()
    get_project_context = MagicMock(return_value=(ls, 42))
    df__best_model = pd.DataFrame({"country": ["ru"], "detection_model_id": ["m1"]})

    result = set_label_studio_current_model_version(
        df__best_model=df__best_model,
        idx=pd.DataFrame(),
        get_project_context=get_project_context,
        model_keys=["country", "detection_model_id"],
    )

    assert result.iloc[0]["model_version"] == "ru/m1"
    ls.projects.update.assert_called_once_with(
        id=42,
        show_collab_predictions=True,
        model_version="ru/m1",
    )


def test_assign_model_version_column_supports_multiple_model_keys():
    df = pd.DataFrame(
        {
            "country": ["ru", "us"],
            "detection_model_id": ["m1", "m2"],
        }
    )

    assert list(_assign_model_version_column(df, ["country", "detection_model_id"])) == [
        "ru/m1",
        "us/m2",
    ]


def test_compose_model_version_supports_multiple_model_keys():
    row = pd.Series({"country": "ru", "detection_model_id": "m1"})
    assert _compose_model_version(row, ["country", "detection_model_id"]) == "ru/m1"


def test_set_label_studio_current_model_version_rejects_multiple_rows():
    df__best_model = pd.DataFrame({"model_version": ["v1", "v2"]})

    with pytest.raises(ValueError, match="exactly one row"):
        set_label_studio_current_model_version(
            df__best_model=df__best_model,
            idx=pd.DataFrame(),
            get_project_context=MagicMock(),
            model_keys=["model_version"],
        )


def test_set_label_studio_current_model_version_empty_input():
    result = set_label_studio_current_model_version(
        df__best_model=pd.DataFrame(columns=["model_version"]),
        idx=pd.DataFrame(),
        get_project_context=MagicMock(),
        model_keys=["model_version"],
    )

    assert result.empty
    assert list(result.columns) == ["project_id", "model_version"]


def test_validate_prediction_primary_keys_requires_model_keys_in_primary_keys():
    with pytest.raises(ValueError, match="must be included in primary_keys"):
        _validate_prediction_primary_keys(["image_name"], ["detection_model_id"])


def test_label_studio_upload_predictions_requires_model_keys_in_primary_keys():
    with pytest.raises(ValueError, match="must be included in primary_keys"):
        LabelStudioUploadPredictions(
            input__item__has__prediction="images_with_predictions",
            input__label_studio_project_task="ls_task",
            input__best_model="best_detection_model",
            output__label_studio_current_model_version="ls_current_model_version",
            output__label_studio_project_prediction="ls_predictions",
            ls_url="http://localhost",
            api_key="token",
            project_identifier="project",
            primary_keys=["image_name"],
            model_keys=["detection_model_id"],
        )
