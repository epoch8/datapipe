import importlib
import importlib.util
import multiprocessing as mp
import os
import random
import shutil
import tempfile
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

import cv2
import fsspec
import imageio
import numpy as np
import pandas as pd
import tensorflow as tf
from albumentations import (
    Blur,
    Compose,
    GaussNoise,
    LongestMaxSize,
    MedianBlur,
    MotionBlur,
    Normalize,
    OneOf,
    PadIfNeeded,
    RandomBrightnessContrast,
    RandomRotate90,
    ShiftScaleRotate,
    VerticalFlip,
)
from natsort import natsorted
from pathy import FluidPath, Pathy
from sklearn.utils.class_weight import compute_class_weight

from datapipe_ml.core.multiprocessing import finish_training_subprocess
from datapipe_ml.core.training_subprocess import run_training_subprocess_body
from datapipe_ml.frameworks.tensorflow.callbacks import (
    FsspecModelCheckpoint,
)
from datapipe_ml.frameworks.tensorflow.checkpoint_selection import select_best_classification_checkpoint
from datapipe_ml.frameworks.tensorflow.checkpoint_sync import is_tf_last_checkpoint_path
from datapipe_ml.training.staging import (
    copied_file_to_local_temp,
    make_local_staging_dir,
    stage_files_to_local_folder,
)
from datapipe_ml.training.sync import PeriodicTrainingSync
from datapipe_ml.training.specs import TrainingSyncConfig


@dataclass
class TFModelSpec:
    factory: str
    kwargs: Dict[str, Any] = field(default_factory=dict)
    preprocess_mean: Optional[Tuple[float, float, float]] = None
    preprocess_std: Optional[Tuple[float, float, float]] = None

    def __post_init__(self):
        if isinstance(self.preprocess_mean, list):
            self.preprocess_mean = tuple(self.preprocess_mean)  # type: ignore[assignment]
        if isinstance(self.preprocess_std, list):
            self.preprocess_std = tuple(self.preprocess_std)  # type: ignore[assignment]


def get_seed(seed: int):
    os.environ["PYTHONHASHSEED"] = "0"
    random.seed(seed)
    np.random.seed(seed)
    tf.random.set_seed(seed)


def get_preprocess_input_script_str(
    image_size: Tuple[int, int] = (128, 128),
    mean: Tuple[float, float, float] = (0.485, 0.456, 0.406),
    std: Tuple[float, float, float] = (0.229, 0.224, 0.225),
):
    return f"""
from typing import cast,List, Tuple
import cv2
import numpy as np
from albumentations import Compose, LongestMaxSize, Normalize, PadIfNeeded


def preprocess_input(
    input: List[np.ndarray],
    image_size: Tuple[int, int] = {image_size},
    mean: Tuple[float, float, float] = {mean},
    std: Tuple[float, float, float] = {std},
) -> np.ndarray:
    width, height = image_size
    transforms = Compose(
        [
            LongestMaxSize(max_size=max(image_size)),
            PadIfNeeded(height, width, border_mode=cv2.BORDER_CONSTANT, fill=0),
            Normalize(mean=mean, std=std),
        ]
    )
    res = np.array([transforms(image=np.array(item))["image"] for item in input])
    return res"""


def import_from_string(path: str) -> Callable[..., tf.keras.Model]:
    module_name, separator, attr_name = path.partition(":")
    if not separator or not module_name or not attr_name:
        raise ValueError(f"Expected factory path in 'module:function' format, got {path!r}")
    module = importlib.import_module(module_name)
    try:
        factory: object = module.__dict__[attr_name]
    except KeyError as exc:
        raise AttributeError(f"Module {module_name!r} has no attribute {attr_name!r}") from exc
    if not callable(factory):
        raise TypeError(f"Model factory {path!r} is not callable")
    return cast(Callable[..., tf.keras.Model], factory)


def get_custom_model(
    model_spec: TFModelSpec,
    image_size: Tuple[int, int],
    class_names: List[str],
) -> tf.keras.Model:
    model_factory = import_from_string(model_spec.factory)
    model = model_factory(image_size=image_size, class_names=class_names, **model_spec.kwargs)
    if not isinstance(model, tf.keras.Model):
        raise TypeError(f"Model factory {model_spec.factory!r} must return tf.keras.Model")
    print(f"base model - custom factory {model_spec.factory}")
    return model


def get_model(
    image_size: Tuple[int, int],
    arch: Optional[str],
    class_names_len: int,
    resume_checkpoint_filepath: Optional[str] = None,
    model_spec: Optional[TFModelSpec] = None,
    class_names: Optional[List[str]] = None,
):
    if resume_checkpoint_filepath is None:
        if model_spec is not None:
            if class_names is None:
                raise ValueError("class_names must be provided when model_spec is used")
            return get_custom_model(model_spec=model_spec, image_size=image_size, class_names=class_names)
        if arch is None:
            raise ValueError("Either arch or model_spec must be provided")
        if arch == "tiny_cnn":
            model = tf.keras.Sequential(
                [
                    tf.keras.layers.Input(shape=(image_size[1], image_size[0], 3)),
                    tf.keras.layers.Conv2D(8, 3, activation="relu"),
                    tf.keras.layers.GlobalAveragePooling2D(),
                    tf.keras.layers.Dense(class_names_len, activation="softmax"),
                ]
            )
            print("base model - tiny_cnn")
            return model
        if arch in ["mobilenetv3"]:
            import keras_cv_attention_models

            base_model = keras_cv_attention_models.mobilenetv3.MobileNetV3Small100(
                input_shape=(image_size[1], image_size[0], 3),
                num_classes=0,
                pretrained="imagenet",
            )
            print("base model - mobilenetv3")

        elif arch in ["mobilenetv2"]:
            base_model = tf.keras.applications.mobilenet_v2.MobileNetV2(
                input_shape=(image_size[1], image_size[0], 3),
                include_top=False,
                weights="imagenet",  # best is 224
            )
            print("base model - mobilenetv2")
        elif arch in ["mobilenet"]:
            base_model = tf.keras.applications.mobilenet.MobileNet(
                input_shape=(image_size[1], image_size[0], 3),
                include_top=False,
                weights="imagenet",  # best is 224
            )
            print("base model - mobilenet")
        elif arch in ["coatnet"]:
            import keras_cv_attention_models

            base_model = keras_cv_attention_models.coatnet.CoAtNet2(
                input_shape=(image_size[1], image_size[0], 3),  # best is 224
                num_classes=0,
                pretrained="imagenet21k",
            )
            print("base model - coatnet")

        elif arch in ["beit"]:
            import keras_cv_attention_models

            base_model = keras_cv_attention_models.beit.BeitV2BasePatch16(
                input_shape=(image_size[1], image_size[0], 3),  # best is 224
                num_classes=0,
                pretrained="imagenet21k-ft1k",
            )
            print("base model - beit")

        elif arch in ["caformer"]:
            import keras_cv_attention_models

            base_model = keras_cv_attention_models.caformer.CAFormerS36(
                input_shape=(image_size[1], image_size[0], 3),  # best is 224
                num_classes=0,
                pretrained="imagenet21k-ft1k",
            )
            print("base model - caformer")

        elif arch in ["gcvit"]:
            import keras_cv_attention_models

            base_model = keras_cv_attention_models.gcvit.GCViT_Base(
                input_shape=(image_size[1], image_size[0], 3),  # best is 224
                num_classes=0,
                pretrained="imagenet",
            )
            print("base model - gcvit")
        else:
            from classification_models.tfkeras import Classifiers

            model_class, _ = Classifiers.get(arch)
            base_model = model_class(
                input_shape=(image_size[1], image_size[0], 3), weights="imagenet", include_top=False
            )
            print(f"base model - {arch} from classifiers")

        if arch in ["beit"]:
            model = tf.keras.Sequential(
                [
                    base_model,
                    tf.keras.layers.Flatten(),
                    tf.keras.layers.Dropout(rate=0.5),
                    tf.keras.layers.Dense(class_names_len, activation="softmax"),
                ]
            )
        else:
            model = tf.keras.Sequential(
                [
                    base_model,
                    tf.keras.layers.GlobalAveragePooling2D(),
                    tf.keras.layers.Dropout(rate=0.5),
                    tf.keras.layers.Dense(class_names_len, activation="softmax"),
                ]
            )

    else:
        with copied_file_to_local_temp(
            resume_checkpoint_filepath,
            suffix=".keras",
            label="TensorFlow resume checkpoint",
        ) as checkpoint_path:
            model = tf.keras.models.load_model(str(checkpoint_path))

    return model


def read_image_filepath(filepath: bytes):
    with fsspec.open(filepath.decode(), "rb") as src:
        image = np.asarray(imageio.imread(src))
    return ensure_rgb_uint8(image)


def ensure_rgb_uint8(image: np.ndarray) -> np.ndarray:
    """Normalize loaded images to HxWx3 so tf.data batching does not mix ranks."""
    if image.ndim == 2:
        image = np.stack([image, image, image], axis=-1)
    elif image.ndim == 3 and image.shape[-1] == 1:
        image = np.concatenate([image, image, image], axis=-1)
    elif image.ndim == 3 and image.shape[-1] == 4:
        image = image[..., :3]
    elif image.ndim != 3 or image.shape[-1] != 3:
        raise ValueError(f"Unsupported image shape for classification training: {image.shape}")
    return image


# Augmentations


def get_default_augmentations_train():
    transforms_train = Compose(
        transforms=[
            OneOf([GaussNoise()], p=0.5),
            OneOf(
                [
                    MotionBlur(p=0.2),
                    MedianBlur(blur_limit=3, p=0.1),
                    Blur(blur_limit=3, p=0.1),
                ],
                p=0.5,
            ),
            RandomBrightnessContrast(0.1, 0.2),
            OneOf(
                [
                    # Flip(),
                    # HorizontalFlip(),
                    VerticalFlip(),
                    RandomRotate90(),
                    # Transpose(),
                ],
                p=0.3,
            ),
            ShiftScaleRotate(
                shift_limit=0.05,
                scale_limit=0.2,
                rotate_limit=30,
                p=0.5,
                border_mode=cv2.BORDER_CONSTANT,  # constant border avoids reflection artifacts at crop edges
                fill=0,
            ),
        ]
    )
    return transforms_train


def get_resize_transforms(image_size: Tuple[int, int]):
    resize_transforms = Compose(
        transforms=[
            LongestMaxSize(max_size=max(image_size)),
            PadIfNeeded(image_size[1], image_size[0], border_mode=cv2.BORDER_CONSTANT, fill=0),
        ]
    )
    return resize_transforms


def transforms_func(img: np.ndarray, transforms: Callable):
    return transforms(image=img)["image"]


# Dataset


def default_transform(**kwargs):
    return kwargs


def get_augment_from_script_file(script_file: Union[str, Path, Pathy]) -> Callable[[np.ndarray], np.ndarray]:
    script_file_pathy = Pathy.fluid(str(script_file))
    with fsspec.open(str(script_file_pathy), "r") as src:
        script_code = src.read()
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpdirname_path = Path(tmpdirname)
        script_file_tmp = tmpdirname_path / f"augment_{tmpdirname_path.name}.py"
        with open(str(script_file_tmp), "w") as out:
            out.write(script_code)
        module_name = f"datapipe_ml_augment_{tmpdirname_path.name}"
        spec = importlib.util.spec_from_file_location(module_name, script_file_tmp)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load augment module from {script_file_tmp}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module.augment


def set_shape(image: tf.Tensor, label: tf.Tensor, image_size: Tuple[int, int], class_names: List[str]):
    image.set_shape(tf.TensorShape([None, image_size[1], image_size[0], 3]))
    label.set_shape(tf.TensorShape([None, len(class_names)]))
    return image, label


def get_dataset(
    filepaths: List[str],
    n_labels: List[str],
    image_size: Tuple[int, int],
    class_names: List[str],
    batch_size: int,
    seed: int,
    augmentations: bool,
    train: bool,
    augment_func_file: Optional[str],
    normalize: Normalize,
) -> tf.data.Dataset:
    assert len(filepaths) == len(n_labels)

    # --- TF lookup: str label -> int index
    keys = tf.constant(class_names, dtype=tf.string)
    vals = tf.constant(list(range(len(class_names))), dtype=tf.int32)
    table = tf.lookup.StaticHashTable(
        tf.lookup.KeyValueTensorInitializer(keys, vals),
        default_value=-1,
    )

    if augmentations:
        if augment_func_file is None:
            augment_func = cast(
                Callable[[np.ndarray], np.ndarray],
                partial(transforms_func, transforms=get_default_augmentations_train()),
            )
        else:
            augment_func = get_augment_from_script_file(augment_func_file)

    resize_transforms = get_resize_transforms(image_size)

    ds = tf.data.Dataset.from_tensor_slices((filepaths, n_labels))
    if train:
        ds = ds.shuffle(len(filepaths), seed=seed)

    ds = ds.map(
        lambda fp, lbl: (tf.numpy_function(read_image_filepath, [fp], tf.uint8), lbl),
        num_parallel_calls=tf.data.AUTOTUNE,
    )

    if augmentations:
        ds = ds.map(
            lambda img, lbl: (
                tf.numpy_function(
                    func=lambda x: augment_func(x).astype(np.uint8, copy=False),
                    inp=[img],
                    Tout=tf.uint8,
                ),
                lbl,
            ),
            num_parallel_calls=tf.data.AUTOTUNE,
        )

    ds = (
        ds.map(
            lambda img, lbl: (
                tf.numpy_function(
                    func=lambda x: transforms_func(x, resize_transforms).astype(np.uint8, copy=False),
                    inp=[img],
                    Tout=tf.uint8,
                ),
                lbl,
            ),
            num_parallel_calls=tf.data.AUTOTUNE,
        )
        .map(
            lambda img, lbl: (
                tf.numpy_function(
                    func=lambda x: transforms_func(x, normalize).astype(np.float32, copy=False),
                    inp=[img],
                    Tout=tf.float32,
                ),
                lbl,
            ),
            num_parallel_calls=tf.data.AUTOTUNE,
        )
        .map(
            lambda img, lbl: (img, tf.one_hot(table.lookup(lbl), depth=len(class_names), dtype=tf.float32)),
            num_parallel_calls=tf.data.AUTOTUNE,
        )
        .batch(batch_size)
        .map(lambda img, lbl: set_shape(img, lbl, image_size, class_names), num_parallel_calls=tf.data.AUTOTUNE)
        .prefetch(tf.data.AUTOTUNE)
    )
    return ds


def get_callbacks(
    logdir_exp: Path,
    reduce_lr_patience: int,
    reduce_lr_factor: float,
    early_stopping_patience: int,
):
    callbacks = [
        tf.keras.callbacks.CSVLogger(
            str(logdir_exp / "history.csv"),
            separator=",",
            append=True,
        ),
        FsspecModelCheckpoint(
            str(
                logdir_exp
                / (
                    "{epoch:03d}__train_precision_{precision:.2f}_"
                    "train_recall_{recall:.2f}__val_precision_{val_precision:.2f}_"
                    "val_recall_{val_recall:.2f}_val_f1_score_{val_f1_score:.2f}.keras"
                )
            ),
            save_weights_only=False,
            save_best_only=True,
            monitor="val_f1_score",
            mode="max",
            verbose=True,
        ),
        FsspecModelCheckpoint(
            str(logdir_exp / "{epoch:03d}__last.keras"),
            save_weights_only=False,
            save_best_only=False,
            verbose=True,
            rolling_last=True,
        ),
        tf.keras.callbacks.EarlyStopping(patience=early_stopping_patience, monitor="val_f1_score", mode="max"),
        tf.keras.callbacks.ReduceLROnPlateau(
            factor=reduce_lr_factor,
            patience=reduce_lr_patience,
            monitor="val_f1_score",
            mode="max",
        ),
    ]
    return callbacks


def _initial_epoch_from_resume(
    resume_checkpoint_filepath: Optional[str],
    resume_checkpoint_epoch: Optional[int],
) -> int:
    """Prefer manifest epoch; fall back to checkpoint filename only when manifest has no epoch."""
    if resume_checkpoint_epoch is not None:
        return int(resume_checkpoint_epoch)
    if resume_checkpoint_filepath is None:
        return 0
    try:
        return int(Pathy.fluid(resume_checkpoint_filepath).name.split("__")[0])
    except (TypeError, ValueError):
        print(f"Could not parse initial epoch from checkpoint filename: {resume_checkpoint_filepath!r}")
        return 0


# Training loop


def train_on_tensorflow(
    logdir_exp: FluidPath,
    config: "TF_ClassificationTrainingConfig",
    class_names: List[str],
    train_dataset: tf.data.Dataset,
    val_dataset: tf.data.Dataset,
    callbacks: List[tf.keras.callbacks.Callback],
    preprocess_input_script_str: str,
    class_weight: Optional[Dict[int, float]],
    clean_checkpoints_after_train: bool,
    resume_checkpoint_filepath: Optional[str],
    resume_checkpoint_epoch: Optional[int],
    sync_config: Optional[TrainingSyncConfig],
) -> Tuple[FluidPath, Optional[str], Optional[str], str]:
    protocol, logdir_exp_str_path = fsspec.core.split_protocol(str(logdir_exp))
    fs = fsspec.filesystem(protocol)
    fs.makedirs(logdir_exp_str_path, exist_ok=True)
    if protocol is None or protocol == "file":
        protocol_str = "" if protocol is None else "file://"
    else:
        protocol_str = f"{protocol}://"

    preprocess_input_script_path = str(logdir_exp / "preprocess_input.py")
    with fs.open(str(logdir_exp / "preprocess_input.py"), "w") as out:
        out.write(preprocess_input_script_str)

    best_model_path = None
    traceback_logs = None
    interrupted = False
    try:
        strategy = tf.distribute.MirroredStrategy()
        print("Number of devices: {}".format(strategy.num_replicas_in_sync))

        if resume_checkpoint_filepath is not None:
            print(f"Getting model: {resume_checkpoint_filepath=}")
            initial_epoch = _initial_epoch_from_resume(resume_checkpoint_filepath, resume_checkpoint_epoch)
        else:
            print("Getting raw model")
            initial_epoch = 0
        print(f"Train with {len(class_names)=}: {class_names=}")

        with strategy.scope():
            optimizer = tf.keras.optimizers.Adam(learning_rate=config.init_lr)
            model = get_model(
                image_size=config.image_size,
                arch=config.arch,
                class_names_len=len(class_names),
                resume_checkpoint_filepath=resume_checkpoint_filepath,
                model_spec=config.model_spec,
                class_names=class_names,
            )
            print("Compile the model")
            model.compile(
                optimizer=optimizer,
                loss=tf.keras.losses.CategoricalCrossentropy(label_smoothing=config.label_smoothing),
                metrics=[
                    tf.keras.metrics.Precision(name="precision"),
                    tf.keras.metrics.Recall(name="recall"),
                    tf.keras.metrics.F1Score(average="macro", name="f1_score"),
                ],
            )
        output_sync = (
            PeriodicTrainingSync(
                src=str(logdir_exp.parent),
                dst=str(logdir_exp.parent),
                config=sync_config,
                model_id=logdir_exp.name,
            )
            if sync_config is not None and sync_config.enabled
            else None
        )
        print("Fit...")
        if output_sync is None:
            model.fit(
                x=train_dataset,
                epochs=config.epochs,
                callbacks=callbacks,
                validation_data=val_dataset,
                initial_epoch=initial_epoch,
                class_weight=class_weight,
            )
        else:
            with output_sync:
                model.fit(
                    x=train_dataset,
                    epochs=config.epochs,
                    callbacks=callbacks,
                    validation_data=val_dataset,
                    initial_epoch=initial_epoch,
                    class_weight=class_weight,
                )
    except Exception as e:
        from traceback_with_variables import format_exc

        traceback_logs = format_exc(e)
        print(e)
        print("Training model failed.")
        raise e
    except KeyboardInterrupt:
        interrupted = True
        print("Caught KeyboardInterrupt in train process. Stopping the train...")
    finally:
        all_model_paths: List[str] = sorted(fs.glob(str(logdir_exp / "*.keras")))
        best_model_paths = [path for path in all_model_paths if not is_tf_last_checkpoint_path(path)]
        if len(best_model_paths) >= 1:
            selected_best = select_best_classification_checkpoint(best_model_paths)
            assert selected_best is not None
            best_model_path = selected_best
            best_model_path = f"{protocol_str}{best_model_path}"
            print(f"Best model: {best_model_path}")
            if clean_checkpoints_after_train and len(best_model_paths) > 3:
                for model_path in best_model_paths[:-2]:
                    # Keep only the last two checkpoints.
                    # Training may be interrupted while a checkpoint is being written.
                    fs.rm(model_path)
            if clean_checkpoints_after_train:
                for model_path in all_model_paths:
                    if is_tf_last_checkpoint_path(model_path):
                        fs.rm(model_path)
        else:
            print(f"Exp failed. Removing {logdir_exp}.")
            shutil.rmtree(logdir_exp, ignore_errors=True)

    if interrupted:
        raise KeyboardInterrupt

    return logdir_exp, best_model_path, traceback_logs, preprocess_input_script_path


@dataclass
class TrainModelResult:
    classification_model_id: Optional[str] = None
    model_path: Optional[str] = None
    class_names: Optional[List[str]] = None
    traceback_logs: Optional[str] = None
    preprocess_input_script_path: Optional[str] = None


@dataclass
class TF_ClassificationTrainingConfig:
    image_size: Tuple[int, int]
    seed: int
    batch_size: int
    arch: Optional[str]
    init_lr: float
    reduce_lr_patience: int
    reduce_lr_factor: float
    early_stopping_patience: int
    epochs: int
    label_smoothing: float
    augmentations: bool
    augment_func_file: Optional[str]
    class_weight: bool  # TODO: support custom Dict[str, float] per-class weight dictionaries
    model_spec: Optional[TFModelSpec] = None

    def __post_init__(self):
        if self.model_spec is not None and isinstance(self.model_spec, dict):
            self.model_spec = TFModelSpec(**self.model_spec)
        if self.arch is None and self.model_spec is None:
            raise ValueError("Either arch or model_spec must be provided")


# Run!
def train_model_main(
    df__data: pd.DataFrame,
    config: TF_ClassificationTrainingConfig,
    classification_model_id: str,
    models_dir: str,
    clean_checkpoints_after_train: bool,
    tmp_folder: str,
    resume_checkpoint_filepath: Optional[str],
    resume_checkpoint_epoch: Optional[int],
    sync_config: Optional[TrainingSyncConfig],
    persisted_models_dir: Optional[str] = None,
) -> TrainModelResult:
    get_seed(seed=config.seed)
    image_staging = make_local_staging_dir(
        tmp_folder=tmp_folder,
        name=f"tmp_images_{classification_model_id}",
        use_managed_tmp=tmp_folder.startswith("/tmp"),
    )
    df__data["image__image_path_local"] = stage_files_to_local_folder(
        cast(List[str], df__data["image__image_path"]),
        image_staging.path,
        label="TensorFlow images",
    )
    df__train = df__data[df__data["subset_id"] == "train"]
    class_names_set = set(df__train["label"])
    class_names = natsorted(class_names_set)

    if config.model_spec is not None and config.model_spec.preprocess_mean is not None:
        if config.model_spec.preprocess_std is None:
            raise ValueError("model_spec.preprocess_std must be provided with model_spec.preprocess_mean")
        normalize = Normalize(mean=config.model_spec.preprocess_mean, std=config.model_spec.preprocess_std)
        preprocess_input_script_str = get_preprocess_input_script_str(
            image_size=config.image_size,
            mean=config.model_spec.preprocess_mean,
            std=config.model_spec.preprocess_std,
        )
    elif config.arch is not None and any(arch_cand in config.arch for arch_cand in ["mobilenet", "nasnet"]):
        normalize = Normalize(mean=(1 / 2, 1 / 2, 1 / 2), std=(1 / 2, 1 / 2, 1 / 2))
        preprocess_input_script_str = get_preprocess_input_script_str(
            image_size=config.image_size, mean=(1 / 2, 1 / 2, 1 / 2), std=(1 / 2, 1 / 2, 1 / 2)
        )
    else:
        normalize = Normalize()
        preprocess_input_script_str = get_preprocess_input_script_str(config.image_size)

    train_dataset = get_dataset(
        filepaths=cast(List[str], df__train["image__image_path_local"]),
        n_labels=cast(List[str], df__train["label"]),
        image_size=config.image_size,
        class_names=class_names,
        batch_size=config.batch_size,
        seed=config.seed,
        augmentations=config.augmentations,
        train=True,
        augment_func_file=config.augment_func_file,
        normalize=normalize,
    )
    df__val = df__data[df__data["subset_id"] == "val"]
    df__val = df__val[df__val["label"].isin(class_names_set)]
    val_dataset = get_dataset(
        filepaths=cast(List[str], df__val["image__image_path_local"]),
        n_labels=cast(List[str], df__val["label"]),
        image_size=config.image_size,
        class_names=class_names,
        batch_size=config.batch_size,
        seed=config.seed,
        augmentations=False,
        train=False,
        augment_func_file=None,
        normalize=normalize,
    )
    if isinstance(config.class_weight, bool) and config.class_weight and len(class_names) > 1:
        class_weight = {
            idx: cls_weight
            for idx, cls_weight in enumerate(
                compute_class_weight(
                    class_weight="balanced", classes=np.array(class_names), y=df__train["label"].tolist()
                )
            )
        }
    else:
        class_weight = None
    print("Getting callbacks")
    logdir_exp = Pathy.fluid(models_dir) / classification_model_id
    callbacks = get_callbacks(
        logdir_exp=logdir_exp,
        reduce_lr_patience=config.reduce_lr_patience,
        reduce_lr_factor=config.reduce_lr_factor,
        early_stopping_patience=config.early_stopping_patience,
    )
    print(f"total {config.epochs=}")
    logdir_exp, best_model_path, traceback_logs, preprocess_input_script_path = train_on_tensorflow(
        logdir_exp=Pathy.fluid(str(logdir_exp)),
        config=config,
        class_names=class_names,
        train_dataset=train_dataset,
        val_dataset=val_dataset,
        callbacks=callbacks,
        preprocess_input_script_str=preprocess_input_script_str,
        class_weight=class_weight,
        clean_checkpoints_after_train=clean_checkpoints_after_train,
        resume_checkpoint_filepath=resume_checkpoint_filepath,
        resume_checkpoint_epoch=resume_checkpoint_epoch,
        sync_config=sync_config,
    )
    if persisted_models_dir and best_model_path is not None:
        from datapipe_ml.training.sync import remap_path_under_root

        best_model_path = remap_path_under_root(str(best_model_path), models_dir, persisted_models_dir)
        preprocess_input_script_path = remap_path_under_root(
            preprocess_input_script_path,
            models_dir,
            persisted_models_dir,
        )
    if best_model_path is not None:
        return TrainModelResult(
            classification_model_id=classification_model_id,
            model_path=str(best_model_path),
            class_names=class_names,
            traceback_logs=traceback_logs,
            preprocess_input_script_path=preprocess_input_script_path,
        )
    else:
        return TrainModelResult(traceback_logs=traceback_logs)


def train_process(
    queue: mp.Queue,
    df__data: pd.DataFrame,
    config: TF_ClassificationTrainingConfig,
    classification_model_id: str,
    models_dir: str,
    clean_checkpoints_after_train: bool,
    tmp_folder: str,
    resume_checkpoint_filepath: Optional[str],
    resume_checkpoint_epoch: Optional[int],
    sync_config: Optional[TrainingSyncConfig],
    persisted_models_dir: Optional[str] = None,
):
    print("Training process begin!")
    train_model_result = None
    traceback_logs = None

    def _on_failure(logs: str) -> None:
        nonlocal traceback_logs
        traceback_logs = logs
        print(logs)
        print("Training model failed.")

    train_model_result, traceback_logs = run_training_subprocess_body(
        action=lambda: train_model_main(
            df__data=df__data,
            config=config,
            classification_model_id=classification_model_id,
            models_dir=models_dir,
            clean_checkpoints_after_train=clean_checkpoints_after_train,
            tmp_folder=tmp_folder,
            resume_checkpoint_filepath=resume_checkpoint_filepath,
            resume_checkpoint_epoch=resume_checkpoint_epoch,
            sync_config=sync_config,
            persisted_models_dir=persisted_models_dir,
        ),
        on_failure=_on_failure,
    )
    if train_model_result is None:
        train_model_result = TrainModelResult(traceback_logs=traceback_logs)
    failed = train_model_result.classification_model_id is None
    print("Training process exited!")
    finish_training_subprocess(queue, train_model_result, failed=failed)
