from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from datapipe.compute import (
    Catalog,
    ComputeStep,
    Pipeline,
    PipelineStep,
    Table,
    build_compute,
)
from datapipe.datatable import DataStore
from datapipe.executor import ExecutorConfig
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform, DatatableBatchTransform
from datapipe.store.database import TableStoreDB
from datapipe.store.filedir import PILFile, TableStoreFiledir
from datapipe.types import Labels
from pathy import Pathy
from sqlalchemy import JSON, Column, Float
from sqlalchemy.sql.sqltypes import Integer, String

from datapipe_ml.core.datapipe import check_columns_are_in_table, get_datatable
from datapipe_ml.frameworks.yolo.dataset import get_size_for_resize


@dataclass
class YoloModeSpec:
    # names specific to a mode (detection/segmentation)
    fd_folder_name: str  # e.g. "detection_frozen_dataset" / "segmentation_frozen_dataset"
    model_prefix: str  # e.g. "detection_model" / "segmentation_model"
    train_config_id_col: str  # e.g. "detection_train_config_id" / "segmentation_train_config_id"
    train_config_params_col: str  # e.g. "detection_train_config__params" / ...
    labels_adapter_factory: Callable[[str], Any]  # e.g. CustomYOLOLabelsFile / CustomYOLOV8SegmentatorLabelsFile
    get_train_configs_func: Callable[..., Any]  # e.g. get_yolov8_detection_train_configs(...)
    resize_and_prepare_images_func: Callable[..., Any]
    get_class_names_from_gt_func: Callable[..., Any]
    train_callable: Callable[..., Any]  # train_yolov5 / train_yolov8 / train_yolov8_segmentation
    # where to store models
    models_subdir: str  # "models" or "segmentation_models"
    sqlalchemy_class_names_type = JSON
    extra_class_names_columns: List[Column] = field(default_factory=list)
    extra_model_columns: List[Column] = field(default_factory=list)
    extra_class_names_to_yaml_fields: List[str] = field(default_factory=list)
    required_has_gt_columns: List[str] = field(default_factory=list)


@dataclass
class _YoloComputeSchema:
    model_primary_keys: List[str]
    model_other_primary_keys: List[str]
    model_primary_columns: List[Column]
    filename_pattern: str


def _resolve_yolo_schema(
    *,
    ds: DataStore,
    input__frozen_dataset: str,
    input__frozen_dataset__has__image_gt: str,
    primary_keys: List[str],
    model_primary_keys: Optional[List[str]],
    model_id__name: str,
    frozen_dataset_id__name: str,
    image__image_path__name: str,
    bbox_id__name: Optional[str],
    separator_to_split_attrnames: str,
    mode: YoloModeSpec,
) -> _YoloComputeSchema:
    if model_primary_keys is None:
        model_primary_keys = [model_id__name]
    if model_id__name not in model_primary_keys:
        raise ValueError(f"{model_id__name!r} must be present in model_primary_keys")
    model_other_primary_keys = [x for x in model_primary_keys if x != model_id__name]

    check_columns_are_in_table(
        ds,
        input__frozen_dataset,
        model_other_primary_keys + [frozen_dataset_id__name],
    )
    if bbox_id__name is not None:
        required_has_gt_columns = mode.required_has_gt_columns or [
            "subset_id",
            bbox_id__name,
            image__image_path__name,
            "x_min",
            "y_min",
            "x_max",
            "y_max",
            "label",
        ]
    else:
        required_has_gt_columns = mode.required_has_gt_columns or [
            "subset_id",
            image__image_path__name,
            "bboxes",
            "labels",
        ]
    check_columns_are_in_table(
        ds,
        input__frozen_dataset__has__image_gt,
        model_other_primary_keys + required_has_gt_columns,
    )

    dt__input_has_gt = get_datatable(ds, input__frozen_dataset__has__image_gt)
    model_primary_columns = [col for col in dt__input_has_gt.primary_schema if col.name in model_other_primary_keys] + [
        Column(model_id__name, String, primary_key=True)
    ]
    filename_pattern = separator_to_split_attrnames.join(
        [f"{{{attr}}}" for attr in primary_keys if attr not in model_other_primary_keys]
    )
    return _YoloComputeSchema(
        model_primary_keys=model_primary_keys,
        model_other_primary_keys=model_other_primary_keys,
        model_primary_columns=model_primary_columns,
        filename_pattern=filename_pattern,
    )


def _model_context_dir(model_other_primary_keys: List[str], dt__input_has_gt) -> str:
    return "/".join([f"{{{c.name}}}" for c in dt__input_has_gt.primary_schema if c.name in model_other_primary_keys])


def _build_yolo_pipeline_steps(
    *,
    ds: DataStore,
    input__frozen_dataset: str,
    input__frozen_dataset__has__image_gt: str,
    output__train_config: str,
    output__size_for_resize: str,
    output__frozen_dataset__class_names: str,
    output__frozen_dataset__resized_image_file: str,
    output__frozen_dataset__yolo_txt: str,
    output__model: str,
    output__model_is_trained_on_frozen_dataset: str,
    working_dir: str,
    primary_keys: List[str],
    model_other_primary_keys: List[str],
    model_id__name: str,
    frozen_dataset_id__name: str,
    image__image_path__name: str,
    bbox_id__name: Optional[str],
    labels: Optional[Labels],
    prepare_data_executor_config: Optional[ExecutorConfig],
    resize_images: bool,
    max_within_time: str,
    save_checkpoints_to_cloud: bool,
    tmp_folder: str,
    model_suffix: str,
    ignore_errors_sample_sizes: bool,
    mode: YoloModeSpec,
    train_configs_list: Dict[str, Any],
    training_launcher_config: Optional[Any],
    extra_train_kwargs: Optional[Dict[str, Any]],
) -> List[PipelineStep]:
    return [
        BatchGenerate(
            func=mode.get_train_configs_func,
            outputs=[output__train_config],
            kwargs=dict(**train_configs_list),
            labels=labels,
        ),
        BatchTransform(
            func=get_size_for_resize,
            inputs=[output__train_config],
            outputs=[output__size_for_resize],
            transform_keys=[mode.train_config_id_col],
            chunk_size=16,
            executor_config=prepare_data_executor_config,
            labels=labels,
            kwargs=dict(
                resize_images=resize_images,
                train_config_id_col=mode.train_config_id_col,
                train_config_params_col=mode.train_config_params_col,
            ),
        ),
        BatchTransform(
            func=mode.get_class_names_from_gt_func,
            inputs=[input__frozen_dataset__has__image_gt],
            outputs=[output__frozen_dataset__class_names],
            transform_keys=model_other_primary_keys + [frozen_dataset_id__name],
            chunk_size=1,
            labels=labels,
            kwargs=dict(
                bbox_id__name=bbox_id__name,
                filedir=(Pathy.fluid(working_dir) / mode.fd_folder_name),
                detection_model_other_primary_keys=model_other_primary_keys,
                detection_frozen_dataset_id__name=frozen_dataset_id__name,
                keypoints_model_other_primary_keys=model_other_primary_keys,
                keypoints_frozen_dataset_id__name=frozen_dataset_id__name,
            ),
        ),
        BatchTransform(
            func=mode.resize_and_prepare_images_func,
            inputs=[
                input__frozen_dataset__has__image_gt,
                output__size_for_resize,
                output__frozen_dataset__class_names,
            ],
            outputs=[
                output__frozen_dataset__resized_image_file,
                output__frozen_dataset__yolo_txt,
            ],
            transform_keys=primary_keys + [frozen_dataset_id__name] + ["width", "height", "subset_id"],
            chunk_size=4,
            kwargs=dict(
                primary_keys=primary_keys,
                bbox_id__name=bbox_id__name,
                image__image_path__name=image__image_path__name,
                detection_model_other_primary_keys=model_other_primary_keys,
                detection_frozen_dataset_id__name=frozen_dataset_id__name,
                keypoints_model_other_primary_keys=model_other_primary_keys,
                keypoints_frozen_dataset_id__name=frozen_dataset_id__name,
            ),
            executor_config=prepare_data_executor_config,
            labels=labels,
        ),
        DatatableBatchTransform(
            func=mode.train_callable,
            inputs=[
                input__frozen_dataset,
                output__train_config,
                output__frozen_dataset__class_names,
                output__frozen_dataset__resized_image_file,
                output__frozen_dataset__yolo_txt,
            ],
            outputs=[
                output__model,
                output__model_is_trained_on_frozen_dataset,
            ],
            transform_keys=model_other_primary_keys + [frozen_dataset_id__name, mode.train_config_id_col],
            chunk_size=1,
            kwargs=dict(
                models_dir=str(Pathy.fluid(working_dir) / mode.models_subdir),
                max_within_time=max_within_time,
                dt__detection_model=get_datatable(ds, output__model),
                dt__segmentation_model=get_datatable(ds, output__model),
                dt__detection_model_is_trained_on_detection_frozen_dataset=get_datatable(ds, 
                    output__model_is_trained_on_frozen_dataset
                ),
                dt__segm_model_is_trained_on_segm_frozen_dataset=get_datatable(ds, 
                    output__model_is_trained_on_frozen_dataset
                ),
                dt__keypoints_model=get_datatable(ds, output__model),
                dt__keypoints_model_is_trained_on_keypoints_frozen_dataset=get_datatable(ds, 
                    output__model_is_trained_on_frozen_dataset
                ),
                save_checkpoints_to_cloud=save_checkpoints_to_cloud,
                detection_model_other_primary_keys=model_other_primary_keys,
                segmentation_model_other_primary_keys=model_other_primary_keys,
                keypoints_model_other_primary_keys=model_other_primary_keys,
                detection_model_id__name=model_id__name,
                segmentation_model_id__name=model_id__name,
                keypoints_model_id__name=model_id__name,
                detection_frozen_dataset_id__name=frozen_dataset_id__name,
                segmentation_frozen_dataset_id__name=frozen_dataset_id__name,
                keypoints_frozen_dataset_id__name=frozen_dataset_id__name,
                extra_class_names_to_yaml_fields=mode.extra_class_names_to_yaml_fields,
                tmp_folder=tmp_folder,
                ignore_errors_sample_sizes=ignore_errors_sample_sizes,
                model_suffix=model_suffix,
                training_launcher_config=training_launcher_config,
                **(extra_train_kwargs or {}),
            ),
            labels=labels,
        ),
    ]


def _register_yolo_tables(
    *,
    ds: DataStore,
    catalog: Catalog,
    output__train_config: str,
    output__size_for_resize: str,
    output__frozen_dataset__class_names: str,
    output__frozen_dataset__resized_image_file: str,
    output__frozen_dataset__yolo_txt: str,
    output__model: str,
    output__model_is_trained_on_frozen_dataset: str,
    working_dir: str,
    primary_keys: List[str],
    model_other_primary_keys: List[str],
    model_primary_columns: List[Column],
    model_context_dir: str,
    filename_pattern: str,
    frozen_dataset_id__name: str,
    create_table: bool,
    mode: YoloModeSpec,
    dt__input_has_gt,
) -> None:
    catalog.add_datatable(
        output__train_config,
        Table(
            ds.get_or_create_table(
                output__train_config,
                TableStoreDB(
                    dbconn=ds.meta_dbconn,
                    name=output__train_config,
                    data_sql_schema=[
                        Column(mode.train_config_id_col, String, primary_key=True),
                        Column(mode.train_config_params_col, mode.sqlalchemy_class_names_type),
                    ],
                    create_table=create_table,
                ),
            ).table_store
        ),
    )
    catalog.add_datatable(
        output__size_for_resize,
        Table(
            ds.get_or_create_table(
                output__size_for_resize,
                TableStoreDB(
                    dbconn=ds.meta_dbconn,
                    name=output__size_for_resize,
                    data_sql_schema=[
                        Column(mode.train_config_id_col, String, primary_key=True),
                        Column("width", Integer, primary_key=True),
                        Column("height", Integer, primary_key=True),
                    ],
                    create_table=create_table,
                ),
            ).table_store
        ),
    )
    catalog.add_datatable(
        output__frozen_dataset__class_names,
        Table(
            ds.get_or_create_table(
                output__frozen_dataset__class_names,
                TableStoreDB(
                    dbconn=ds.meta_dbconn,
                    name=output__frozen_dataset__class_names,
                    data_sql_schema=model_primary_columns[:-1]
                    + [
                        Column(frozen_dataset_id__name, String, primary_key=True),
                        Column("class_names", mode.sqlalchemy_class_names_type),
                    ]
                    + mode.extra_class_names_columns,
                    create_table=create_table,
                ),
            ).table_store
        ),
    )
    file_primary_schema = (
        model_primary_columns[:-1]
        + [
            Column(frozen_dataset_id__name, String, primary_key=True),
            Column("width", Integer, primary_key=True),
            Column("height", Integer, primary_key=True),
            Column("subset_id", String, primary_key=True),
        ]
        + [
            col
            for col in dt__input_has_gt.primary_schema
            if col.name in primary_keys and col.name not in model_other_primary_keys
        ]
    )
    catalog.add_datatable(
        output__frozen_dataset__resized_image_file,
        Table(
            ds.get_or_create_table(
                output__frozen_dataset__resized_image_file,
                TableStoreFiledir(
                    (
                        Pathy.fluid(working_dir)
                        / mode.fd_folder_name
                        / model_context_dir
                        / f"{{{frozen_dataset_id__name}}}"
                        / "{width}x{height}"
                        / "{subset_id}"
                        / "images"
                        / f"{filename_pattern}.jpg"
                    ),
                    adapter=PILFile("JPEG"),
                    add_filepath_column=True,
                    enable_rm=False,
                    read_data=False,
                    primary_schema=file_primary_schema,
                ),
            ).table_store
        ),
    )
    catalog.add_datatable(
        output__frozen_dataset__yolo_txt,
        Table(
            ds.get_or_create_table(
                output__frozen_dataset__yolo_txt,
                TableStoreFiledir(
                    (
                        Pathy.fluid(working_dir)
                        / mode.fd_folder_name
                        / model_context_dir
                        / f"{{{frozen_dataset_id__name}}}"
                        / "{width}x{height}"
                        / "{subset_id}"
                        / "labels"
                        / f"{filename_pattern}.txt"
                    ),
                    adapter=mode.labels_adapter_factory("jpg"),
                    add_filepath_column=True,
                    read_data=False,
                    enable_rm=False,
                    primary_schema=file_primary_schema,
                ),
            ).table_store
        ),
    )
    catalog.add_datatable(
        output__model,
        Table(
            ds.get_or_create_table(
                output__model,
                TableStoreDB(
                    dbconn=ds.meta_dbconn,
                    name=output__model,
                    data_sql_schema=model_primary_columns
                    + [
                        Column(f"{mode.model_prefix}__input_size", JSON),
                        Column(f"{mode.model_prefix}__score_threshold", Float),
                        Column(f"{mode.model_prefix}__model_path", String),
                        Column(f"{mode.model_prefix}__type", String),
                        Column(f"{mode.model_prefix}__class_names", mode.sqlalchemy_class_names_type),
                    ]
                    + mode.extra_model_columns,
                    create_table=create_table,
                ),
            ).table_store
        ),
    )
    catalog.add_datatable(
        output__model_is_trained_on_frozen_dataset,
        Table(
            ds.get_or_create_table(
                output__model_is_trained_on_frozen_dataset,
                TableStoreDB(
                    dbconn=ds.meta_dbconn,
                    name=output__model_is_trained_on_frozen_dataset,
                    data_sql_schema=model_primary_columns
                    + [
                        Column(frozen_dataset_id__name, String, primary_key=True),
                        Column(mode.train_config_id_col, String, primary_key=True),
                        Column(mode.train_config_params_col, mode.sqlalchemy_class_names_type),
                    ],
                    create_table=create_table,
                ),
            ).table_store
        ),
    )


def build_yolo_compute(
    *,
    ds: DataStore,
    catalog: Catalog,
    # io table names
    input__frozen_dataset: str,
    input__frozen_dataset__has__image_gt: str,
    output__train_config: str,
    output__size_for_resize: str,
    output__frozen_dataset__class_names: str,
    output__frozen_dataset__resized_image_file: str,
    output__frozen_dataset__yolo_txt: str,
    output__model: str,
    output__model_is_trained_on_frozen_dataset: str,
    working_dir: str,
    primary_keys: List[str],
    model_primary_keys: Optional[List[str]],
    model_id__name: str,
    frozen_dataset_id__name: str,
    image__image_path__name: str,
    bbox_id__name: Optional[str],
    separator_to_split_attrnames: str,
    create_table: bool,
    labels: Optional[Labels],
    executor_config: Optional[ExecutorConfig],
    prepare_data_executor_config: Optional[ExecutorConfig],
    resize_images: bool,
    max_within_time: str,
    save_checkpoints_to_cloud: bool,
    tmp_folder: str,
    model_suffix: str,
    ignore_errors_sample_sizes: bool = False,
    mode: YoloModeSpec,
    train_configs_list: Dict[str, Any],
    training_launcher_config: Optional[Any] = None,
    extra_train_kwargs: Optional[Dict[str, Any]] = None,
) -> List[ComputeStep]:
    """
    Constructs the whole YOLO pipeline (DB schemas + filedir tables + steps) for any v5/v8 detect/segment mode.
    """

    schema = _resolve_yolo_schema(
        ds=ds,
        input__frozen_dataset=input__frozen_dataset,
        input__frozen_dataset__has__image_gt=input__frozen_dataset__has__image_gt,
        primary_keys=primary_keys,
        model_primary_keys=model_primary_keys,
        model_id__name=model_id__name,
        frozen_dataset_id__name=frozen_dataset_id__name,
        image__image_path__name=image__image_path__name,
        bbox_id__name=bbox_id__name,
        separator_to_split_attrnames=separator_to_split_attrnames,
        mode=mode,
    )
    model_primary_keys = schema.model_primary_keys
    model_other_primary_keys = schema.model_other_primary_keys
    model_primary_columns = schema.model_primary_columns
    pattern = schema.filename_pattern
    dt__input_has_gt = get_datatable(ds, input__frozen_dataset__has__image_gt)
    model_context_dir = _model_context_dir(model_other_primary_keys, dt__input_has_gt)

    _register_yolo_tables(
        ds=ds,
        catalog=catalog,
        output__train_config=output__train_config,
        output__size_for_resize=output__size_for_resize,
        output__frozen_dataset__class_names=output__frozen_dataset__class_names,
        output__frozen_dataset__resized_image_file=output__frozen_dataset__resized_image_file,
        output__frozen_dataset__yolo_txt=output__frozen_dataset__yolo_txt,
        output__model=output__model,
        output__model_is_trained_on_frozen_dataset=output__model_is_trained_on_frozen_dataset,
        working_dir=working_dir,
        primary_keys=primary_keys,
        model_other_primary_keys=model_other_primary_keys,
        model_primary_columns=model_primary_columns,
        model_context_dir=model_context_dir,
        filename_pattern=pattern,
        frozen_dataset_id__name=frozen_dataset_id__name,
        create_table=create_table,
        mode=mode,
        dt__input_has_gt=dt__input_has_gt,
    )

    # ==== Pipeline ===============================================================
    steps = _build_yolo_pipeline_steps(
        ds=ds,
        input__frozen_dataset=input__frozen_dataset,
        input__frozen_dataset__has__image_gt=input__frozen_dataset__has__image_gt,
        output__train_config=output__train_config,
        output__size_for_resize=output__size_for_resize,
        output__frozen_dataset__class_names=output__frozen_dataset__class_names,
        output__frozen_dataset__resized_image_file=output__frozen_dataset__resized_image_file,
        output__frozen_dataset__yolo_txt=output__frozen_dataset__yolo_txt,
        output__model=output__model,
        output__model_is_trained_on_frozen_dataset=output__model_is_trained_on_frozen_dataset,
        working_dir=working_dir,
        primary_keys=primary_keys,
        model_other_primary_keys=model_other_primary_keys,
        model_id__name=model_id__name,
        frozen_dataset_id__name=frozen_dataset_id__name,
        image__image_path__name=image__image_path__name,
        bbox_id__name=bbox_id__name,
        labels=labels,
        prepare_data_executor_config=prepare_data_executor_config,
        resize_images=resize_images,
        max_within_time=max_within_time,
        save_checkpoints_to_cloud=save_checkpoints_to_cloud,
        tmp_folder=tmp_folder,
        model_suffix=model_suffix,
        ignore_errors_sample_sizes=ignore_errors_sample_sizes,
        mode=mode,
        train_configs_list=train_configs_list,
        training_launcher_config=training_launcher_config,
        extra_train_kwargs=extra_train_kwargs,
    )
    return build_compute(ds, catalog, Pipeline(steps))
