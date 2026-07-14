import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Protocol, cast

import pandas as pd
from datapipe.compute import (
    Catalog,
    ComputeStep,
    Pipeline,
    PipelineStep,
    Table,
    build_compute,
)
from datapipe.datatable import DataStore, DataTable
from datapipe.run_config import RunConfig
from datapipe.step.datatable_transform import DatatableTransform
from datapipe.store.database import TableStoreDB
from datapipe.store.filedir import TableStoreFiledir
from datapipe.types import PipelineInput, PipelineOutput, IndexDF, Labels
from pathy import Pathy
from sqlalchemy import Column, and_, func, select
from sqlalchemy.sql.sqltypes import JSON, DateTime, Integer, String

from datapipe_ml.core.datapipe import (
    check_columns_are_in_table,
    get_datatable,
    get_pipeline_table_name,
    is_last_frozen_dataset_old_enough,
    pipeline_input_as_table,
    pipeline_output_as_table,
)

logger = logging.getLogger("datapipe.ml.freeze")


class _SqlTableMetaLike(Protocol):
    sql_table: Any


def _get_datatable_sql_meta_table(dt: DataTable):
    return cast(_SqlTableMetaLike, dt.meta).sql_table


def get_changed_idxs_since_last_freeze_sql(
    ds: DataStore,
    dt__frozen_dataset: DataTable,
    dt__image__ground_truth: DataTable,
    created_at_col: str,
) -> IndexDF:
    assert isinstance(dt__frozen_dataset.table_store, TableStoreDB)
    frozen_dataset_table = dt__frozen_dataset.table_store.data_table
    image_ground_truth_meta_table = _get_datatable_sql_meta_table(dt__image__ground_truth)

    with ds.meta_dbconn.con.begin() as conn:
        max_created_at = conn.execute(select(func.max(frozen_dataset_table.c[created_at_col]))).scalar_one_or_none()

        stmt = select(*[image_ground_truth_meta_table.c[k] for k in dt__image__ground_truth.primary_keys])
        if max_created_at is not None:
            stmt = stmt.where(image_ground_truth_meta_table.c["update_ts"] > max_created_at.timestamp())

        df_changed = pd.read_sql(stmt, conn)

    return IndexDF(df_changed)


def freeze_dataset(
    ds: DataStore,
    input_dts: List[DataTable],
    output_dts: List[DataTable],
    run_config: Optional[RunConfig],
    kwargs: Optional[Dict[str, Any]] = None,
):
    # unpack kwargs
    kwargs = kwargs or {}
    primary_keys: List[str] = kwargs["primary_keys"]
    min_delta: int = kwargs["min_delta"]
    min_within_time: str = kwargs["min_within_time"]
    working_dir: str = kwargs["working_dir"]
    image__image_path__name: str = kwargs["image__image_path__name"]
    frozen_dataset_id__name: str = kwargs["frozen_dataset_id__name"]
    bbox_id__name: Optional[str] = kwargs.get("bbox_id__name")
    label_column_name: str = kwargs["label_column_name"]
    extra_gt_columns: List[str] = kwargs.get("extra_gt_columns", [])
    model_type: str = kwargs["model_type"]

    # unpack inputs
    dt__image, dt__subset__has__image, dt__image__ground_truth = input_dts
    dt__frozen_dataset, dt__frozen_dataset__has__image_gt = output_dts

    # check minimum time since last freeze
    if not is_last_frozen_dataset_old_enough(
        dt__frozen_dataset=dt__frozen_dataset,
        frozen_dataset_id__name=frozen_dataset_id__name,
        frozen_dataset__created_at__name=f"{model_type}_frozen_dataset__created_at",
        min_within_time=min_within_time,
    ):
        raise ValueError("Not enough time passed since last frozen dataset.")

    # drop nulls in idx and check delta
    idx = get_changed_idxs_since_last_freeze_sql(
        ds=ds,
        dt__frozen_dataset=dt__frozen_dataset,
        dt__image__ground_truth=dt__image__ground_truth,
        created_at_col=f"{model_type}_frozen_dataset__created_at",
    ).dropna()
    if len(idx) < min_delta:
        raise ValueError(f"Not enough changed idx for freezing ({len(idx)} < {min_delta})")
    logger.info(f"Got {len(idx)=} >= {min_delta}, that's ok.")

    # get ground truth dataframe
    assert isinstance(dt__image__ground_truth.table_store, TableStoreDB)
    assert isinstance(dt__subset__has__image.table_store, TableStoreDB)
    tbl__image__ground_truth = dt__image__ground_truth.table_store.data_table
    tbl__subset__has__image = dt__subset__has__image.table_store.data_table
    if isinstance(dt__image.table_store, TableStoreDB):
        tbl__image = dt__image.table_store.data_table
        main_sql = (
            select(
                *[
                    tbl__image__ground_truth.c[col.name]
                    for col in tbl__image__ground_truth.c
                    if col.name not in ["subset_id", image__image_path__name]
                ],
                tbl__subset__has__image.c["subset_id"],
                tbl__image.c[image__image_path__name],
            )
            .select_from(tbl__image__ground_truth)
            .join(
                tbl__subset__has__image,
                and_(
                    *[
                        tbl__image__ground_truth.c[primary_key] == tbl__subset__has__image.c[primary_key]
                        for primary_key in primary_keys
                    ],
                ),
            )
            .join(
                tbl__image,
                and_(
                    *[
                        tbl__image__ground_truth.c[primary_key] == tbl__image.c[primary_key]
                        for primary_key in primary_keys
                    ],
                ),
            )
            .where(tbl__subset__has__image.c["subset_id"].in_(["train", "val", "test"]))
        )
    elif isinstance(dt__image.table_store, TableStoreFiledir):
        main_sql = (
            select(
                *[
                    tbl__image__ground_truth.c[col.name]
                    for col in tbl__image__ground_truth.c
                    if col.name not in ["subset_id", image__image_path__name]
                ],
                tbl__subset__has__image.c["subset_id"],
            )
            .select_from(tbl__image__ground_truth)
            .join(
                tbl__subset__has__image,
                and_(
                    *[
                        tbl__image__ground_truth.c[primary_key] == tbl__subset__has__image.c[primary_key]
                        for primary_key in primary_keys
                    ],
                ),
            )
            .where(tbl__subset__has__image.c["subset_id"].in_(["train", "val", "test"]))
        )

    with ds.meta_dbconn.con.begin() as conn:
        df__frozen_dataset__has__image_gt = pd.read_sql(main_sql, conn)

    if df__frozen_dataset__has__image_gt.empty:
        raise ValueError("No ground truth.")

    if isinstance(dt__image.table_store, TableStoreFiledir):
        df__image_with_filepath = dt__image.table_store._read_rows_fast(
            cast(IndexDF, df__frozen_dataset__has__image_gt[primary_keys])
        )
        df__image_with_filepath[image__image_path__name] = df__image_with_filepath["filepath"]
        df__frozen_dataset__has__image_gt = pd.merge(
            df__frozen_dataset__has__image_gt,
            df__image_with_filepath[primary_keys + [image__image_path__name]],
            on=primary_keys,
        )

    # generate new frozen dataset ID
    date = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    frozen_dataset_id = f"{date}_{uuid.uuid4()}"
    logger.info(f"Freezing '{frozen_dataset_id}'")

    # set image path column
    df__frozen_dataset__has__image_gt.rename(columns={image__image_path__name: "image__image_path"}, inplace=True)
    # set frozen dataset ID
    df__frozen_dataset__has__image_gt[frozen_dataset_id__name] = frozen_dataset_id

    # count unique images per split
    primary_with_bbox = primary_keys + ([bbox_id__name] if bbox_id__name else [])
    counts: Dict[str, int] = {}
    for split in ["train", "val", "test"]:
        counts[split] = len(
            df__frozen_dataset__has__image_gt[df__frozen_dataset__has__image_gt["subset_id"] == split].drop_duplicates(
                subset=primary_with_bbox
            )
        )
    if counts["train"] == 0:
        raise ValueError("No train samples.")
    if counts["val"] == 0:
        raise ValueError("No val samples.")
    total = counts["train"] + counts["val"] + counts["test"]

    # prepare metadata row for frozen_dataset table
    df__frozen_dataset = pd.DataFrame(
        [
            {
                **{frozen_dataset_id__name: frozen_dataset_id},
                **{f"{model_type}_frozen_dataset__created_at": datetime.strptime(date, "%Y%m%d_%H%M")},
                **{
                    f"{model_type}_frozen_dataset__folder_filepath": str(
                        Pathy.fluid(working_dir) / f"{model_type}_frozen_dataset" / frozen_dataset_id
                    )
                },
                **{f"{model_type}_frozen_dataset__images_count": total},
                **{f"{model_type}_frozen_dataset__train_images_count": counts["train"]},
                **{f"{model_type}_frozen_dataset__val_images_count": counts["val"]},
                **{f"{model_type}_frozen_dataset__test_images_count": counts["test"]},
            }
        ]
    )

    # build new has_image_gt rows
    cols_to_keep = primary_keys + [frozen_dataset_id__name] + ["subset_id", "image__image_path"] + extra_gt_columns
    if model_type == "classification":
        cols_to_keep += [label_column_name]
    else:
        if bbox_id__name is not None:
            cols_to_keep += ["x_min", "y_min", "x_max", "y_max", label_column_name]
        else:
            cols_to_keep += ["bboxes", "labels"] + (["masks"] if model_type == "segmentation" else [])
    df__frozen_dataset__has__image_gt = df__frozen_dataset__has__image_gt[cols_to_keep]

    logger.info(f"Freezing dataset: {len(df__frozen_dataset__has__image_gt)} images")
    dt__frozen_dataset__has__image_gt.store_chunk(df__frozen_dataset__has__image_gt)
    dt__frozen_dataset.store_chunk(df__frozen_dataset)


@dataclass
class FreezeDatasetStep(PipelineStep):
    input__image: PipelineInput
    input__image__ground_truth: PipelineInput
    input__subset__has__image: PipelineInput
    output__frozen_dataset: PipelineOutput
    output__frozen_dataset__has__image_gt: PipelineOutput
    working_dir: str
    primary_keys: List[str]
    min_delta: int = 10
    min_within_time: str = "1w"
    create_table: bool = False
    image__image_path__name: str = "image__image_path"
    bbox_id__name: Optional[str] = None
    labels: Optional[Labels] = None
    frozen_dataset_primary_keys: Optional[List[str]] = None
    frozen_dataset_id__name: str = "frozen_dataset_id"
    model_type: str = "generic"
    label_column_name: str = "label"
    extra_gt_columns: List[str] = field(default_factory=list)

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        # set default primary keys for frozen_dataset
        if self.frozen_dataset_primary_keys is None:
            self.frozen_dataset_primary_keys = [self.frozen_dataset_id__name]

        # check input table schemas
        check_columns_are_in_table(ds, self.input__image, self.primary_keys + [self.image__image_path__name])
        gt_cols = self.primary_keys.copy()
        if self.model_type == "classification":
            gt_cols += [self.label_column_name]
        else:
            if self.bbox_id__name is not None:
                gt_cols += [self.bbox_id__name, "x_min", "y_min", "x_max", "y_max", self.label_column_name]
            else:
                gt_cols += ["bboxes", "labels"]
                if self.model_type == "segmentation":
                    gt_cols.append("masks")
        gt_cols += self.extra_gt_columns
        check_columns_are_in_table(ds, self.input__image__ground_truth, gt_cols)

        # ensure subset table has primary_keys + subset_id
        if not any(
            check_columns_are_in_table(ds, self.input__subset__has__image, [pk, "subset_id"], raise_exc=False)
            for pk in self.primary_keys
        ):
            raise ValueError(f"Table {self.input__subset__has__image} missing primary key or 'subset_id'")

        # get DataTable objects
        # dt__image = get_datatable(ds, self.input__image)
        dt__image__ground_truth = get_datatable(ds, self.input__image__ground_truth)

        # prepare schema for output tables
        prim_schema = [
            col for col in dt__image__ground_truth.primary_schema if col.name in self.frozen_dataset_primary_keys
        ] + [Column(self.frozen_dataset_id__name, String, primary_key=True)]
        prefix = f"{self.model_type}_frozen_dataset"

        # register frozen_dataset table
        catalog.add_datatable(
            get_pipeline_table_name(self.output__frozen_dataset),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__frozen_dataset),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__frozen_dataset),
                        data_sql_schema=prim_schema
                        + [
                            Column(f"{prefix}__created_at", DateTime),
                            Column(f"{prefix}__folder_filepath", String),
                            Column(f"{prefix}__images_count", Integer),
                            Column(f"{prefix}__train_images_count", Integer),
                            Column(f"{prefix}__val_images_count", Integer),
                            Column(f"{prefix}__test_images_count", Integer),
                            Column(f"{prefix}__class_names", JSON),
                        ],
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )

        # register frozen_dataset__has__image_gt table
        extra_cols: List[Column] = [
            Column("subset_id", String, primary_key=True),
            Column("image__image_path", String),
        ]
        if self.model_type == "classification":
            extra_cols += [Column(self.label_column_name, String)]
        else:
            if self.bbox_id__name is not None:
                extra_cols += [
                    Column("x_min", Integer),
                    Column("y_min", Integer),
                    Column("x_max", Integer),
                    Column("y_max", Integer),
                    Column(self.label_column_name, String),
                ]
            else:
                extra_cols += [
                    Column("bboxes", JSON),
                    Column("labels", JSON),
                ]
                if self.model_type == "segmentation":
                    extra_cols.append(Column("masks", JSON))
        extra_cols += [Column(column_name, JSON) for column_name in self.extra_gt_columns]
        catalog.add_datatable(
            get_pipeline_table_name(self.output__frozen_dataset__has__image_gt),
            Table(
                ds.get_or_create_table(
                    get_pipeline_table_name(self.output__frozen_dataset__has__image_gt),
                    TableStoreDB(
                        dbconn=ds.meta_dbconn,
                        name=get_pipeline_table_name(self.output__frozen_dataset__has__image_gt),
                        data_sql_schema=prim_schema
                        + [
                            c
                            for c in dt__image__ground_truth.primary_schema
                            if c.name not in self.frozen_dataset_primary_keys
                        ]
                        + extra_cols,
                        create_table=self.create_table,
                    ),
                ).table_store
            ),
        )

        # build pipeline with generic freeze function
        pipeline = Pipeline(
            [
                DatatableTransform(
                    func=freeze_dataset,
                    inputs=[
                        pipeline_input_as_table(self.input__image),
                        pipeline_input_as_table(self.input__subset__has__image),
                        pipeline_input_as_table(self.input__image__ground_truth),
                    ],
                    outputs=[
                        pipeline_output_as_table(self.output__frozen_dataset),
                        pipeline_output_as_table(self.output__frozen_dataset__has__image_gt),
                    ],
                    kwargs={
                        "model_type": self.model_type,
                        "primary_keys": self.primary_keys,
                        "min_delta": self.min_delta,
                        "min_within_time": self.min_within_time,
                        "working_dir": self.working_dir,
                        "image__image_path__name": self.image__image_path__name,
                        "frozen_dataset_primary_keys": self.frozen_dataset_primary_keys,
                        "frozen_dataset_id__name": self.frozen_dataset_id__name,
                        "bbox_id__name": self.bbox_id__name,
                        "label_column_name": self.label_column_name,
                        "extra_gt_columns": self.extra_gt_columns,
                    },
                    labels=self.labels,
                )
            ]
        )
        return build_compute(ds, catalog, pipeline)
