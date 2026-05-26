from __future__ import annotations

import pytest

pytestmark = pytest.mark.tensorflow


def _sample_image():
    import numpy as np

    return np.arange(6 * 8 * 3, dtype=np.uint8).reshape((6, 8, 3))


def _make_config(**overrides):
    from datapipe_ml.tasks.classification.train.tensorflow import (
        TF_ClassificationTrainingConfig,
    )

    params = dict(
        image_size=(16, 16),
        seed=42,
        batch_size=1,
        arch=None,
        init_lr=0.001,
        reduce_lr_patience=1,
        reduce_lr_factor=0.5,
        early_stopping_patience=1,
        epochs=1,
        label_smoothing=0.0,
        augmentations=False,
        augment_func_file=None,
        class_weight=False,
    )
    params.update(overrides)
    return TF_ClassificationTrainingConfig(**params)


def test_tf_classification_training_config_restores_model_spec_from_dict():
    from datapipe_ml.tasks.classification.train.tensorflow import TFModelSpec

    config = _make_config(
        model_spec=dict(
            factory="tests.helpers.tf_custom_models:build_tiny_custom_classifier",
            kwargs={"filters": 8},
            preprocess_mean=[0.5, 0.5, 0.5],
            preprocess_std=[0.25, 0.25, 0.25],
        )
    )

    assert isinstance(config.model_spec, TFModelSpec)
    assert config.model_spec.kwargs == {"filters": 8}
    assert config.model_spec.preprocess_mean == (0.5, 0.5, 0.5)
    assert config.model_spec.preprocess_std == (0.25, 0.25, 0.25)


def test_tf_classification_train_config_id_includes_model_spec_kwargs():
    from datapipe_ml.tasks.classification.train.tensorflow import (
        TFModelSpec,
        get_tf_classification_train_config_id,
    )

    base_spec = dict(factory="tests.helpers.tf_custom_models:build_tiny_custom_classifier")
    config_a = _make_config(model_spec=TFModelSpec(**base_spec, kwargs={"filters": 4}))
    config_b = _make_config(model_spec=TFModelSpec(**base_spec, kwargs={"filters": 8}))

    config_id_a = get_tf_classification_train_config_id(config_a)
    config_id_b = get_tf_classification_train_config_id(config_b)

    assert "build_tiny_custom_classifier" in config_id_a
    assert '"filters": 4' in config_id_a
    assert '"filters": 8' in config_id_b
    assert config_id_a != config_id_b


@pytest.mark.tensorflow
def test_get_model_uses_custom_tf_model_spec_factory():
    tf = pytest.importorskip("tensorflow")
    from datapipe_ml.frameworks.tensorflow.classification_runner import get_model
    from datapipe_ml.tasks.classification.train.tensorflow import TFModelSpec

    model = get_model(
        image_size=(16, 16),
        arch=None,
        class_names_len=2,
        model_spec=TFModelSpec(
            factory="tests.helpers.tf_custom_models:build_tiny_custom_classifier",
            kwargs={"filters": 6},
        ),
        class_names=["cat", "dog"],
    )

    assert isinstance(model, tf.keras.Model)
    assert model.input_shape == (None, 16, 16, 3)
    assert model.output_shape == (None, 2)
    assert model.layers[0].filters == 6


def test_default_classification_augmentations_keep_image_shape_and_dtype():
    from datapipe_ml.frameworks.tensorflow.classification_runner import (
        get_default_augmentations_train,
        transforms_func,
    )

    image = _sample_image()[:6, :6]
    augmented = transforms_func(image, get_default_augmentations_train())

    assert augmented.shape == image.shape
    assert augmented.dtype == image.dtype


def test_resize_transforms_pad_to_target_size_with_albumentations_v2_fill_arg():
    from datapipe_ml.frameworks.tensorflow.classification_runner import (
        get_resize_transforms,
        transforms_func,
    )

    resized = transforms_func(_sample_image(), get_resize_transforms((12, 12)))

    assert resized.shape == (12, 12, 3)


def test_augment_function_can_be_loaded_from_script_file(tmp_path):
    import numpy as np

    from datapipe_ml.frameworks.tensorflow.classification_runner import (
        get_augment_from_script_file,
    )

    script_path = tmp_path / "augment.py"
    script_path.write_text(
        "\n".join(
            [
                "import numpy as np",
                "",
                "def augment(image):",
                "    return np.zeros_like(image) + 7",
            ]
        )
    )

    augment = get_augment_from_script_file(script_path)
    augmented = augment(_sample_image())

    assert np.unique(augmented).tolist() == [7]
    assert augmented.dtype == _sample_image().dtype
