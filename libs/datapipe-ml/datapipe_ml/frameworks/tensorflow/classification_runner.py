import importlib

# from tensorflow.io import read_file, decode_image
import multiprocessing as mp
import os
import random
import shutil
import sys
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

from datapipe_ml.core.files import copy_url_to_url, parallel_copy_filepaths_to_folder
from datapipe_ml.frameworks.tensorflow.callbacks import (
    FsspecModelCheckpoint,
)


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
    factory = getattr(module, attr_name)
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
        with tempfile.NamedTemporaryFile(suffix=".keras") as tmpfile:
            copy_url_to_url(
                resume_checkpoint_filepath, tmpfile.name, label="TensorFlow resume checkpoint", concurrency=1
            )
            model = tf.keras.models.load_model(tmpfile.name)

    return model


def read_image_filepath(filepath: bytes):
    with fsspec.open(filepath.decode(), "rb") as src:
        return imageio.imread(src)


# Аугментации


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
                border_mode=cv2.BORDER_CONSTANT,  # todo: research
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


# Датасет


def default_transform(**kwargs):
    return kwargs


def get_augment_from_script_file(script_file: Union[str, Path, Pathy]) -> Callable[[np.ndarray], np.ndarray]:
    script_file_pathy = Pathy.fluid(str(script_file))
    with fsspec.open(str(script_file_pathy), "r") as src:
        script_code = src.read()
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpdirname_path = Path(tmpdirname)
        module_folder = tmpdirname_path / "module"
        module_folder.mkdir()
        script_file_tmp = module_folder / f"augment_{tmpdirname_path.name}.py"
        with open(str(script_file_tmp), "w") as out:
            out.write(script_code)
        sys.path.append(str(script_file_tmp.parent.absolute()))
        module = importlib.import_module(script_file_tmp.stem)
        importlib.reload(module)
        sys.path.pop()
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
        tf.keras.callbacks.EarlyStopping(patience=early_stopping_patience, monitor="val_f1_score", mode="max"),
        tf.keras.callbacks.ReduceLROnPlateau(
            factor=reduce_lr_factor,
            patience=reduce_lr_patience,
            monitor="val_f1_score",
            mode="max",
        ),
    ]
    return callbacks


# Само обучение


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
) -> Tuple[FluidPath, Optional[str], Optional[str], str]:
    protocol, logdir_exp_str_path = fsspec.core.split_protocol(str(logdir_exp))
    fs = fsspec.filesystem(protocol)
    fs.makedirs(logdir_exp_str_path)
    if protocol is None or protocol == "file":
        protocol_str = "" if protocol is None else "file://"
    else:
        protocol_str = f"{protocol}://"

    preprocess_input_script_path = str(logdir_exp / "preprocess_input.py")
    with fs.open(str(logdir_exp / "preprocess_input.py"), "w") as out:
        out.write(preprocess_input_script_str)

    best_model_path = None
    traceback_logs = None
    try:
        strategy = tf.distribute.MirroredStrategy()
        print("Number of devices: {}".format(strategy.num_replicas_in_sync))
        RESUME_CHECKPOINT_FILEPATH = os.environ.get("TRAIN__RESUME_CHECKPOINT_FILEPATH", None)

        if RESUME_CHECKPOINT_FILEPATH is not None:
            print(f"Getting model: {RESUME_CHECKPOINT_FILEPATH=}")
            initial_epoch = int(Pathy.fluid(RESUME_CHECKPOINT_FILEPATH).name.split("__")[0])  # xxx__<name>...<>.h5
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
                resume_checkpoint_filepath=RESUME_CHECKPOINT_FILEPATH,
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
        print("Fit...")
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
        print("Catched KeyboardInterrupt in train process. Stopping the train...")
    finally:
        best_model_paths: List[str] = sorted(fs.glob(str(logdir_exp / "*.keras")))
        if len(best_model_paths) >= 1:
            best_model_path = best_model_paths[-1]
            best_model_path = f"{protocol_str}{best_model_path}"
            print(f"Best model: {best_model_path}")
            if clean_checkpoints_after_train and len(best_model_paths) > 3:
                for model_path in best_model_paths[:-2]:
                    # Удаляем все кроме последних 2
                    # Тк возможен случай, когда отменили обучение во время сохранения модели
                    fs.rm(model_path)
        else:
            print(f"Exp failed. Removing {logdir_exp}.")
            shutil.rmtree(logdir_exp, ignore_errors=True)

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
    class_weight: bool  # TODO: внедрить кастомную логику
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
) -> TrainModelResult:
    get_seed(seed=config.seed)
    if tmp_folder.startswith("/tmp"):
        folder = tempfile.TemporaryDirectory()
        folder_path = Path(folder.name)
    else:
        folder_path = Path(tmp_folder)
    print(f"Parallel: copying images to local tmp folder {folder_path=}...")
    df__data["image__image_path_local"] = parallel_copy_filepaths_to_folder(
        cast(List[str], df__data["image__image_path"]), str(folder_path)
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
):
    print("Training process begin!")
    train_model_result = None
    traceback_logs = None
    try:
        train_model_result = train_model_main(
            df__data=df__data,
            config=config,
            classification_model_id=classification_model_id,
            models_dir=models_dir,
            clean_checkpoints_after_train=clean_checkpoints_after_train,
            tmp_folder=tmp_folder,
        )
    except Exception as e:
        from traceback_with_variables import format_exc

        traceback_logs = format_exc(e)
        print(traceback_logs)
        print("Training model failed.")
        # os.kill(os.getppid(), signal.SIGKILL)
    finally:
        if train_model_result is None:
            train_model_result = TrainModelResult(traceback_logs=traceback_logs)
        queue.put(train_model_result)
        print("Training process exited!")
        exit(0)
