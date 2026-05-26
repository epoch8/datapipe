import os
import time
import xml.etree.ElementTree as ET
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
import requests
from datapipe.compute import Catalog, Pipeline, Table, build_compute, run_steps
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import do_batch_generate
from datapipe.store.database import TableStoreDB
from cvat_sdk.models import LabeledDataRequest, LabeledShapeRequest, PatchedLabelRequest, ProjectWriteRequest
from PIL import Image
from sqlalchemy import Column, String

from datapipe_cvat.cvat_step import (
    CVATStep,
    _ensure_cvat_file_path_column,
    create_cvat_client,
    int_from_scalar,
    plan_cvat_frame_updates,
    reset_task_jobs_status,
)


pytestmark = pytest.mark.cvat
TASKS_COUNT = 4


class _Shape:
    def __init__(self, type, label_id, points, occluded=False, z_order=0):
        self.type = type
        self.label_id = label_id
        self.points = points
        self.occluded = occluded
        self.z_order = z_order


class _Annotations:
    def __init__(self, shapes):
        self.shapes = shapes


class _Job:
    def __init__(self, state):
        self.state = state
        self.updates = []

    def update(self, values):
        self.updates.append(values)
        self.state = values.get("state", self.state)


class _Task:
    def __init__(self, jobs):
        self._jobs = jobs

    def get_jobs(self):
        return self._jobs


def wait_until_cvat_is_up(cvat_url: str, timeout_seconds: int = 120) -> None:
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            response = requests.get(f"{cvat_url.rstrip('/')}/api/server/about", timeout=5)
            if response.status_code < 500:
                return
        except requests.RequestException as exc:
            last_error = exc
        time.sleep(1)
    raise RuntimeError(f"CVAT did not become ready at {cvat_url}") from last_error


def _require_cvat(cvat_url: str) -> None:
    if os.environ.get("CVAT_URL") is None:
        pytest.skip("CVAT_URL is not set")
    wait_until_cvat_is_up(cvat_url)


def _shape_type_value(shape_type) -> str:
    value = getattr(shape_type, "value", shape_type)
    return str(value).lower()


def _annotations_to_cvat_image_xml(annotations, labels_by_id: dict[int, str], image_path) -> str:
    image = Image.open(image_path)
    image_element = ET.Element(
        "image",
        {
            "name": image_path.name,
            "width": str(image.width),
            "height": str(image.height),
        },
    )
    for shape in annotations.shapes:
        label = labels_by_id[shape.label_id]
        shape_type = _shape_type_value(shape.type)
        if shape_type == "rectangle":
            ET.SubElement(
                image_element,
                "box",
                {
                    "label": label,
                    "source": "manual",
                    "occluded": "1" if shape.occluded else "0",
                    "xtl": str(shape.points[0]),
                    "ytl": str(shape.points[1]),
                    "xbr": str(shape.points[2]),
                    "ybr": str(shape.points[3]),
                    "z_order": str(shape.z_order or 0),
                },
            )
        elif shape_type == "polygon":
            points = ";".join(
                f"{shape.points[idx]},{shape.points[idx + 1]}" for idx in range(0, len(shape.points), 2)
            )
            ET.SubElement(
                image_element,
                "polygon",
                {
                    "label": label,
                    "source": "manual",
                    "occluded": "1" if shape.occluded else "0",
                    "points": points,
                    "z_order": str(shape.z_order or 0),
                },
            )
        elif shape_type == "points":
            points = ";".join(
                f"{shape.points[idx]},{shape.points[idx + 1]}" for idx in range(0, len(shape.points), 2)
            )
            ET.SubElement(
                image_element,
                "points",
                {
                    "label": label,
                    "source": "manual",
                    "occluded": "1" if shape.occluded else "0",
                    "points": points,
                    "z_order": str(shape.z_order or 0),
                },
            )
    return ET.tostring(image_element, encoding="unicode")


def test_reset_task_jobs_status_resets_completed_jobs_only():
    completed_job = _Job("completed")
    active_job = _Job("new")

    reset_task_jobs_status(_Task([completed_job, active_job]))

    assert completed_job.updates == [{"state": "new"}]
    assert completed_job.state == "new"
    assert active_job.updates == []


def test_int_from_scalar_accepts_numpy_integer():
    assert int_from_scalar(np.int64(5)) == 5


def test_ensure_cvat_file_path_keeps_existing_path_when_image_path_is_missing():
    df = pd.DataFrame(
        {
            "image_path": [np.nan, "/tmp/new_image.jpg"],
            "cvat__file_path": ["existing_image.jpg", np.nan],
        }
    )

    result = _ensure_cvat_file_path_column(df, "image_path", cloud_storage_bucket=None)

    assert result["cvat__file_path"].tolist() == ["existing_image.jpg", "new_image.jpg"]


SCENARIO_BATCHES = [
    ("all_u_same", ["U_same", "U_same", "U_same", "U_same"]),
    ("all_a_same", ["A_same", "A_same", "A_same", "A_same"]),
    ("u_changed", ["U_changed", "U_same", "U_same", "U_same"]),
    ("a_changed", ["A_changed", "U_same", "U_same", "U_same"]),
    ("u_deleted", ["U_deleted", "U_same", "U_same", "U_same"]),
    ("a_deleted", ["A_deleted", "U_same", "U_same", "U_same"]),
    ("a_changed_u_deleted", ["A_changed", "U_deleted", "U_same", "U_same"]),
    ("a_changed_a_deleted", ["A_changed", "A_deleted", "U_same", "U_same"]),
    ("u_changed_a_deleted", ["U_changed", "A_deleted", "U_same", "U_same"]),
    ("two_a_changed_two_a_deleted", ["A_changed", "A_changed", "A_deleted", "A_deleted"]),
    ("two_u_changed_two_u_deleted", ["U_changed", "U_changed", "U_deleted", "U_deleted"]),
    ("mixed_full", ["A_changed", "U_changed", "A_deleted", "U_deleted"]),
    ("all_a_changed", ["A_changed", "A_changed", "A_changed", "A_changed"]),
    ("all_a_deleted", ["A_deleted", "A_deleted", "A_deleted", "A_deleted"]),
    ("a_changed_three_a_same", ["A_changed", "A_same", "A_same", "A_same"]),
    ("a_deleted_three_a_same", ["A_deleted", "A_same", "A_same", "A_same"]),
]

REAL_CVAT_SCENARIO_BATCHES = [
    ("all_u_same", ["U_same", "U_same", "U_same", "U_same"]),
    ("all_a_same", ["A_same", "A_same", "A_same", "A_same"]),
    ("mixed_full", ["A_changed", "U_changed", "A_deleted", "U_deleted"]),
]


def _scenario_data(states):
    existing_rows = []
    input_rows = []
    annotation_rows = []
    for frame_id, state in enumerate(states):
        image_id = f"image_{frame_id}"
        existing_rows.append(
            {
                "image_id": image_id,
                "task_queue_id": "queue1",
                "project_id": 1,
                "inner_task_id": 0,
                "task_id": 10,
                "cvat__file_path": f"{image_id}.jpg",
                "inner_frame_id": frame_id,
            }
        )
        if state.startswith("A_"):
            annotation_rows.append(
                {
                    "image_id": image_id,
                    "task_queue_id": "queue1",
                    "inner_task_id": 0,
                    "annotations": '<image><box label="cat" xtl="1" ytl="1" xbr="2" ybr="2" /></image>',
                }
            )
        if state.endswith("_deleted"):
            continue
        image_path = f"/tmp/{image_id}.jpg"
        if state.endswith("_changed"):
            image_path = f"/tmp/{image_id}_changed.jpg"
        input_rows.append(
            {
                "image_id": image_id,
                "task_queue_id": "queue1",
                "inner_task_id": 0,
                "image_path": image_path,
                "annotations": '<image />',
            }
        )
    return (
        pd.DataFrame(input_rows, columns=["image_id", "task_queue_id", "inner_task_id", "image_path", "annotations"]),
        pd.DataFrame(existing_rows),
        pd.DataFrame(annotation_rows, columns=["image_id", "task_queue_id", "inner_task_id", "annotations"]),
    )


@pytest.mark.parametrize("delete_unannotated_tasks_only_on_update", [False, True])
@pytest.mark.parametrize("scenario_name,states", SCENARIO_BATCHES)
def test_plan_cvat_frame_updates_scenario_matrix(scenario_name, states, delete_unannotated_tasks_only_on_update):
    df__input_batches, df__cvat_files, df__local_annotations = _scenario_data(states)

    plan = plan_cvat_frame_updates(
        df__input_batches=df__input_batches,
        df__cvat_files=df__cvat_files,
        df__local_annotations=df__local_annotations,
        primary_keys=["image_id", "task_queue_id"],
        file_path_column="image_path",
        cloud_storage_bucket=None,
        delete_unannotated_tasks_only_on_update=delete_unannotated_tasks_only_on_update,
    )

    expected_delete = []
    expected_upload = []
    expected_keep = []
    for frame_id, state in enumerate(states):
        image_id = f"image_{frame_id}"
        is_changed = state.endswith("_changed")
        is_deleted = state.endswith("_deleted")
        if is_changed or is_deleted:
            expected_delete.append(image_id)
            if is_changed:
                expected_upload.append(image_id)

    assert sorted(plan.rows_to_delete["image_id"].tolist()) == expected_delete, scenario_name
    assert sorted(plan.rows_to_upload["image_id"].tolist()) == expected_upload, scenario_name
    assert sorted(plan.rows_to_keep["image_id"].tolist()) == expected_keep, scenario_name


def _make_image_files(tmp_dir, ids: list[str]) -> dict[str, str]:
    images_dir = tmp_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    image_paths = {}
    for idx, image_id in enumerate(ids):
        image_path = images_dir / f"{image_id}.jpg"
        Image.new("RGB", (100 + idx, 50 + idx), color="white").save(image_path)
        image_paths[image_id] = str(image_path)
    return image_paths


def _make_image_df(tmp_dir, ids: list[str] | None = None) -> pd.DataFrame:
    ids = ids or [f"image_{idx}" for idx in range(TASKS_COUNT)]
    image_paths = _make_image_files(tmp_dir, ids)
    return pd.DataFrame(
        {
            "image_id": ids,
            "task_queue_id": ["queue1"] * len(ids),
            "image_path": [image_paths[image_id] for image_id in ids],
            "annotations": [
                '<image><box label="cat" source="manual" occluded="0" xtl="10" ytl="5" xbr="30" ybr="25" z_order="0" /></image>'
                for _ in ids
            ],
        }
    )


def _make_scenario_initial_df(tmp_dir, scenario_name: str, states) -> pd.DataFrame:
    ids = [f"{scenario_name}_image_{idx}" for idx in range(len(states))]
    image_paths = _make_image_files(tmp_dir, ids)
    return pd.DataFrame(
        {
            "image_id": ids,
            "task_queue_id": [scenario_name] * len(ids),
            "image_path": [image_paths[image_id] for image_id in ids],
            "annotations": ["<image />" for _ in ids],
        }
    )


def _make_scenario_updated_df(tmp_dir, scenario_name: str, states) -> pd.DataFrame:
    rows = []
    for idx, state in enumerate(states):
        if state.endswith("_deleted"):
            continue
        image_id = f"{scenario_name}_image_{idx}"
        path_image_id = f"{image_id}_changed" if state.endswith("_changed") else image_id
        image_path = _make_image_files(tmp_dir, [path_image_id])[path_image_id]
        rows.append(
            {
                "image_id": image_id,
                "task_queue_id": scenario_name,
                "image_path": image_path,
                "annotations": "<image />",
            }
        )
    return pd.DataFrame(rows, columns=["image_id", "task_queue_id", "image_path", "annotations"])


def _store_generated_df(df: pd.DataFrame):
    def _gen():
        yield df

    return _gen


@pytest.fixture
def cvat_pipeline_case(tmp_dir, dbconn, cvat_url, cvat_credentials):
    _require_cvat(cvat_url)
    client = create_cvat_client(cvat_url, "", cvat_credentials)
    project = client.projects.create(
        ProjectWriteRequest(
            name=f"datapipe-cvat-pipeline-test-{uuid4().hex}",
            labels=[
                PatchedLabelRequest(name="cat"),
                PatchedLabelRequest(name="dog"),
                PatchedLabelRequest(name="keypoint"),
            ],
        )
    )
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "image_raw": Table(
                store=TableStoreDB(
                    dbconn=dbconn,
                    name="image_raw",
                    data_sql_schema=[
                        Column("image_id", String, primary_key=True),
                        Column("task_queue_id", String, primary_key=True),
                        Column("image_path", String),
                        Column("annotations", String),
                    ],
                    create_table=True,
                )
            )
        }
    )
    pipeline = Pipeline(
        [
            CVATStep(
                input="image_raw",
                output__input_batches="image_batches",
                output__cvat_task="cvat_task",
                output__cvat_files="cvat_files",
                output__cvat_annotation="cvat_annotation",
                task_sync_table="cvat_task_sync_table",
                cvat_url=cvat_url,
                cvat_organization="",
                cvat_credentials=cvat_credentials,
                cvat_project_id=project.id,
                primary_keys=["image_id", "task_queue_id"],
                file_path_column="image_path",
                cloud_storage_bucket=None,
                files_batch=2,
                minimum_files_in_job=2,
                task_queue_id__name="task_queue_id",
                task_name_format="datapipe-cvat-test {task_queue_id} batch={inner_task_id}",
                max_attempts=1,
                attempt_poll_s=1,
                create_table=True,
            )
        ]
    )
    steps = build_compute(ds, catalog, pipeline)

    try:
        yield ds, steps, client, project, tmp_dir
    finally:
        try:
            for task in project.get_tasks():
                client.tasks.remove_by_ids([task.id])
            client.projects.remove_by_ids([project.id])
        except Exception:
            pass


@pytest.fixture
def cvat_pipeline_delete_case(tmp_dir, dbconn, cvat_url, cvat_credentials):
    _require_cvat(cvat_url)
    client = create_cvat_client(cvat_url, "", cvat_credentials)
    project = client.projects.create(
        ProjectWriteRequest(
            name=f"datapipe-cvat-pipeline-delete-test-{uuid4().hex}",
            labels=[
                PatchedLabelRequest(name="cat"),
                PatchedLabelRequest(name="dog"),
                PatchedLabelRequest(name="keypoint"),
            ],
        )
    )
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "image_raw": Table(
                store=TableStoreDB(
                    dbconn=dbconn,
                    name="image_raw",
                    data_sql_schema=[
                        Column("image_id", String, primary_key=True),
                        Column("task_queue_id", String, primary_key=True),
                        Column("image_path", String),
                        Column("annotations", String),
                    ],
                    create_table=True,
                )
            )
        }
    )
    pipeline = Pipeline(
        [
            CVATStep(
                input="image_raw",
                output__input_batches="image_batches",
                output__cvat_task="cvat_task",
                output__cvat_files="cvat_files",
                output__cvat_annotation="cvat_annotation",
                task_sync_table="cvat_task_sync_table",
                cvat_url=cvat_url,
                cvat_organization="",
                cvat_credentials=cvat_credentials,
                cvat_project_id=project.id,
                primary_keys=["image_id", "task_queue_id"],
                file_path_column="image_path",
                cloud_storage_bucket=None,
                delete_unannotated_tasks_only_on_update=True,
                files_batch=2,
                minimum_files_in_job=2,
                task_queue_id__name="task_queue_id",
                task_name_format="datapipe-cvat-delete-test {task_queue_id} batch={inner_task_id}",
                max_attempts=1,
                attempt_poll_s=1,
                create_table=True,
            )
        ]
    )
    steps = build_compute(ds, catalog, pipeline)

    try:
        yield ds, steps, client, project, tmp_dir
    finally:
        try:
            for task in project.get_tasks():
                client.tasks.remove_by_ids([task.id])
            client.projects.remove_by_ids([project.id])
        except Exception:
            pass


def _run_with_df(ds: DataStore, steps, df: pd.DataFrame) -> None:
    do_batch_generate(
        func=_store_generated_df(df),
        ds=ds,
        output_dts=[ds.get_table("image_raw")],
    )
    run_steps(ds, steps)


def _build_cvat_pipeline_case(
    dbconn,
    cvat_url,
    cvat_credentials,
    project,
    delete_unannotated_tasks_only_on_update: bool,
):
    ds = DataStore(dbconn, create_meta_table=True)
    catalog = Catalog(
        {
            "image_raw": Table(
                store=TableStoreDB(
                    dbconn=dbconn,
                    name="image_raw",
                    data_sql_schema=[
                        Column("image_id", String, primary_key=True),
                        Column("task_queue_id", String, primary_key=True),
                        Column("image_path", String),
                        Column("annotations", String),
                    ],
                    create_table=True,
                )
            )
        }
    )
    pipeline = Pipeline(
        [
            CVATStep(
                input="image_raw",
                output__input_batches="image_batches",
                output__cvat_task="cvat_task",
                output__cvat_files="cvat_files",
                output__cvat_annotation="cvat_annotation",
                task_sync_table="cvat_task_sync_table",
                cvat_url=cvat_url,
                cvat_organization="",
                cvat_credentials=cvat_credentials,
                cvat_project_id=project.id,
                primary_keys=["image_id", "task_queue_id"],
                file_path_column="image_path",
                cloud_storage_bucket=None,
                delete_unannotated_tasks_only_on_update=delete_unannotated_tasks_only_on_update,
                files_batch=4,
                minimum_files_in_job=4,
                task_queue_id__name="task_queue_id",
                task_name_format="datapipe-cvat-scenario {task_queue_id} batch={inner_task_id}",
                max_attempts=1,
                attempt_poll_s=1,
                create_table=True,
            )
        ]
    )
    return ds, build_compute(ds, catalog, pipeline)


@pytest.fixture
def cvat_scenario_case(tmp_dir, dbconn, cvat_url, cvat_credentials, request):
    _require_cvat(cvat_url)
    delete_unannotated_tasks_only_on_update = bool(request.param)
    client = create_cvat_client(cvat_url, "", cvat_credentials)
    project = client.projects.create(
        ProjectWriteRequest(
            name=f"datapipe-cvat-scenario-test-{uuid4().hex}",
            labels=[PatchedLabelRequest(name="cat")],
        )
    )
    ds, steps = _build_cvat_pipeline_case(
        dbconn=dbconn,
        cvat_url=cvat_url,
        cvat_credentials=cvat_credentials,
        project=project,
        delete_unannotated_tasks_only_on_update=delete_unannotated_tasks_only_on_update,
    )
    try:
        yield ds, steps, client, project, tmp_dir, delete_unannotated_tasks_only_on_update
    finally:
        try:
            for task in project.get_tasks():
                client.tasks.remove_by_ids([task.id])
            client.projects.remove_by_ids([project.id])
        except Exception:
            pass


def _mark_scenario_annotations(ds: DataStore, project, scenario_name: str, states) -> None:
    tasks = [task for task in project.get_tasks() if f" {scenario_name} batch=0" in task.name]
    assert len(tasks) == 1
    task = tasks[0]
    labels_by_name = {label.name: label.id for label in task.get_labels()}
    shapes = []
    annotation_rows = []
    cvat_files = (
        ds.get_table("cvat_files")
        .get_data()
        .query("task_queue_id == @scenario_name")
        .sort_values("image_id")
        .reset_index(drop=True)
    )
    for idx, state in enumerate(states):
        if not state.startswith("A_"):
            continue
        frame_row = cvat_files.iloc[idx]
        shapes.append(
            LabeledShapeRequest(
                type="rectangle",
                label_id=labels_by_name["cat"],
                frame=int(frame_row["inner_frame_id"]),
                points=[10, 5, 30, 25],
                occluded=False,
                outside=False,
                z_order=0,
            )
        )
        annotation_rows.append(
            {
                "image_id": frame_row["image_id"],
                "task_queue_id": frame_row["task_queue_id"],
                "inner_task_id": frame_row["inner_task_id"],
                "annotations": '<image><box label="cat" xtl="10" ytl="5" xbr="30" ybr="25" /></image>',
            }
        )
    task.set_annotations(LabeledDataRequest(version=0, tags=[], shapes=shapes, tracks=[]))
    if annotation_rows:
        ds.get_table("cvat_annotation").store_chunk(pd.DataFrame(annotation_rows))


@pytest.mark.parametrize("cvat_scenario_case", [False, True], indirect=True)
def test_real_cvat_pipeline_frame_update_scenarios(cvat_scenario_case):
    ds, steps, _, project, tmp_dir, delete_unannotated_tasks_only_on_update = cvat_scenario_case
    initial_df = pd.concat(
        [
            _make_scenario_initial_df(tmp_dir, scenario_name, states)
            for scenario_name, states in REAL_CVAT_SCENARIO_BATCHES
        ],
        ignore_index=True,
    )
    updated_df = pd.concat(
        [
            _make_scenario_updated_df(tmp_dir, scenario_name, states)
            for scenario_name, states in REAL_CVAT_SCENARIO_BATCHES
        ],
        ignore_index=True,
    )

    _run_with_df(ds, steps, initial_df)
    assert len(project.get_tasks()) == len(REAL_CVAT_SCENARIO_BATCHES)
    for scenario_name, states in REAL_CVAT_SCENARIO_BATCHES:
        _mark_scenario_annotations(ds, project, scenario_name, states)
    task_ids_before_by_scenario = {task.name.split(" ")[1]: task.id for task in project.get_tasks()}
    initial_cvat_files = ds.get_table("cvat_files").get_data()

    _run_with_df(ds, steps, updated_df)

    task_ids_after = {task.id for task in project.get_tasks()}
    expected_new_tasks = sum(
        int(
            any(
                state.endswith("_changed")
                and not (delete_unannotated_tasks_only_on_update and state.startswith("A_"))
                for state in states
            )
        )
        for _, states in REAL_CVAT_SCENARIO_BATCHES
    )
    assert len(task_ids_after) == len(REAL_CVAT_SCENARIO_BATCHES) + expected_new_tasks
    assert set(task_ids_before_by_scenario.values()).issubset(task_ids_after)
    cvat_files = ds.get_table("cvat_files").get_data()

    expected_rows = 0
    for scenario_name, states in REAL_CVAT_SCENARIO_BATCHES:
        for idx, state in enumerate(states):
            image_id = f"{scenario_name}_image_{idx}"
            is_annotated = state.startswith("A_")
            is_changed = state.endswith("_changed")
            is_deleted = state.endswith("_deleted")
            if is_deleted:
                expected_rows += int(delete_unannotated_tasks_only_on_update and is_annotated)
            else:
                expected_rows += 1

            row = cvat_files[cvat_files["image_id"] == image_id]
            if delete_unannotated_tasks_only_on_update and is_annotated and (is_changed or is_deleted):
                previous_row = initial_cvat_files[initial_cvat_files["image_id"] == image_id].iloc[0]
                assert len(row) == 1, scenario_name
                assert row.iloc[0]["cvat__file_path"] == previous_row["cvat__file_path"]
            elif is_deleted:
                assert len(row) == 0, scenario_name
            elif is_changed:
                assert len(row) == 1, scenario_name
                assert row.iloc[0]["cvat__file_path"].endswith("_changed.jpg")
            else:
                assert len(row) == 1, scenario_name

    assert len(cvat_files) == expected_rows


def _set_first_task_completed(client, ds: DataStore, project) -> None:
    task = project.get_tasks()[0]
    labels_by_name = {label.name: label.id for label in task.get_labels()}
    task.set_annotations(
        LabeledDataRequest(
            version=0,
            tags=[],
            shapes=[
                LabeledShapeRequest(
                    type="rectangle",
                    label_id=labels_by_name["dog"],
                    frame=0,
                    points=[11, 6, 31, 26],
                    occluded=False,
                    outside=False,
                    z_order=0,
                )
            ],
            tracks=[],
        )
    )
    job = task.get_jobs()[0]
    job.update({"state": "completed"})
    df_task = ds.get_table("cvat_task").get_data()
    ds.get_table("cvat_task_sync_table").store_chunk(
        pd.DataFrame(
            [
                {
                    **row.to_dict(),
                    "cvat_job__status": "completed",
                    "cvat_job__last_updated": pd.Timestamp.utcnow().to_pydatetime(),
                }
                for _, row in df_task.iterrows()
                if row["task_id"] == task.id
            ]
        )
    )


def test_cvat_pipeline_moderation(cvat_pipeline_case):
    ds, steps, client, project, tmp_dir = cvat_pipeline_case

    run_steps(ds, steps)
    run_steps(ds, steps)

    _run_with_df(ds, steps, _make_image_df(tmp_dir))

    assert len(ds.get_table("image_batches").get_data()) == TASKS_COUNT
    assert len(ds.get_table("cvat_task").get_data()) == 2
    assert len(ds.get_table("cvat_files").get_data()) == TASKS_COUNT
    assert len(project.get_tasks()) == 2

    run_steps(ds, steps)
    assert len(project.get_tasks()) == 2

    _set_first_task_completed(client, ds, project)
    run_steps(ds, steps)

    assert len(ds.get_table("cvat_annotation").get_data()) == 2


def test_cvat_pipeline_when_data_is_changed(cvat_pipeline_case):
    ds, steps, _, project, tmp_dir = cvat_pipeline_case
    df1 = _make_image_df(tmp_dir)
    df2 = df1.copy()
    df2.loc[0, "annotations"] = (
        '<image><box label="cat" source="manual" occluded="0" xtl="12" ytl="7" xbr="32" ybr="27" z_order="0" /></image>'
    )

    _run_with_df(ds, steps, df1)
    task_ids_before = sorted(ds.get_table("cvat_task").get_data()["task_id"].tolist())

    _run_with_df(ds, steps, df2)

    assert len(ds.get_table("cvat_files").get_data()) == TASKS_COUNT
    assert len(project.get_tasks()) == 2
    assert sorted(ds.get_table("cvat_task").get_data()["task_id"].tolist()) == task_ids_before


def test_cvat_pipeline_when_some_data_is_deleted(cvat_pipeline_case):
    ds, steps, _, project, tmp_dir = cvat_pipeline_case
    df1 = _make_image_df(tmp_dir)
    df2 = df1.iloc[:2].reset_index(drop=True)

    _run_with_df(ds, steps, df1)
    _run_with_df(ds, steps, df2)

    assert len(ds.get_table("image_raw").get_data()) == 2
    assert len(ds.get_table("cvat_files").get_data()) == 2
    assert len(project.get_tasks()) == 2


def test_cvat_pipeline_when_task_is_missing_from_cvat(cvat_pipeline_case):
    ds, steps, client, project, tmp_dir = cvat_pipeline_case
    df = _make_image_df(tmp_dir)

    _run_with_df(ds, steps, df)
    missing_task_id = project.get_tasks()[0].id
    missing_cvat_task = ds.get_table("cvat_task").get_data()
    missing_cvat_task = missing_cvat_task[missing_cvat_task["task_id"] == missing_task_id]
    missing_cvat_files = ds.get_table("cvat_files").get_data()
    missing_cvat_files = missing_cvat_files[missing_cvat_files["task_id"] == missing_task_id]
    client.tasks.remove_by_ids([missing_task_id])
    ds.get_table("cvat_task").delete_by_idx(missing_cvat_task)
    ds.get_table("cvat_files").delete_by_idx(missing_cvat_files)
    ds.get_table("cvat_task_sync_table").delete_by_idx(missing_cvat_task)

    run_steps(ds, steps)

    assert len(project.get_tasks()) == 2
    assert len(ds.get_table("cvat_task").get_data()) == 2
    assert missing_task_id not in ds.get_table("cvat_task").get_data()["task_id"].tolist()


def test_cvat_pipeline_specific_updating_scenario(cvat_pipeline_delete_case):
    ds, steps, _, project, tmp_dir = cvat_pipeline_delete_case
    df1 = _make_image_df(tmp_dir, ids=[f"image_{idx}" for idx in range(4)])
    df2 = pd.concat(
        [
            df1.iloc[[0, 1]].copy(),
            _make_image_df(tmp_dir, ids=["image_4", "image_5"]),
        ],
        ignore_index=True,
    )
    df2.loc[0, "image_path"] = _make_image_files(tmp_dir, ["image_0_changed"])["image_0_changed"]

    _run_with_df(ds, steps, df1)
    task_ids_before = set(ds.get_table("cvat_task").get_data()["task_id"].tolist())
    tasks_before_count = len(project.get_tasks())
    _run_with_df(ds, steps, df2)

    assert len(ds.get_table("image_raw").get_data()) == 4
    assert len(ds.get_table("cvat_files").get_data()) == 4
    assert len(project.get_tasks()) == tasks_before_count
    assert set(ds.get_table("cvat_task").get_data()["task_id"].tolist()) == task_ids_before


def test_cvat_pipeline_moderate_then_delete_task(cvat_pipeline_delete_case):
    ds, steps, client, project, tmp_dir = cvat_pipeline_delete_case
    df = _make_image_df(tmp_dir)

    _run_with_df(ds, steps, df)
    _set_first_task_completed(client, ds, project)
    run_steps(ds, steps)
    assert len(ds.get_table("cvat_annotation").get_data()) == 2

    ds.get_table("image_raw").delete_by_idx(
        pd.DataFrame({"image_id": ["image_0", "image_1"], "task_queue_id": ["queue1", "queue1"]})
    )
    run_steps(ds, steps)

    assert len(ds.get_table("image_raw").get_data()) == 2
    assert len(ds.get_table("cvat_files").get_data()) == 2
    assert len(project.get_tasks()) == 2


def test_annotations_to_cvat_image_xml_handles_sdk_shape_type_objects(tmp_dir):
    class ShapeTypeLike:
        def __init__(self, value):
            self.value = value

    image_path = tmp_dir / "image.jpg"
    Image.new("RGB", (100, 50), color="white").save(image_path)
    xml = _annotations_to_cvat_image_xml(
        _Annotations(
            [
                _Shape(ShapeTypeLike("rectangle"), 1, [10, 5, 30, 25]),
                _Shape(ShapeTypeLike("polygon"), 2, [40, 10, 80, 10, 80, 40, 40, 40]),
                _Shape(ShapeTypeLike("points"), 3, [15, 10]),
            ]
        ),
        {1: "cat", 2: "dog", 3: "keypoint"},
        image_path,
    )
    image_element = ET.fromstring(xml)

    assert image_element.find("box").attrib["label"] == "cat"
    assert image_element.find("polygon").attrib["label"] == "dog"
    assert image_element.find("points").attrib["label"] == "keypoint"


def test_real_cvat_annotations_roundtrip(tmp_dir, cvat_url, cvat_credentials):
    _require_cvat(cvat_url)
    image_path = tmp_dir / "image_1.jpg"
    Image.new("RGB", (100, 50), color="white").save(image_path)

    client = create_cvat_client(cvat_url, "", cvat_credentials)
    project = client.projects.create(
        ProjectWriteRequest(
            name=f"datapipe-cvat-test-{uuid4().hex}",
            labels=[
                PatchedLabelRequest(name="cat"),
                PatchedLabelRequest(name="dog"),
                PatchedLabelRequest(name="keypoint"),
            ],
        )
    )
    task = None
    try:
        task = client.tasks.create_from_data(
            spec={"name": f"datapipe-cvat-task-{uuid4().hex}", "project_id": project.id},
            resources=[str(image_path)],
        )
        labels_by_name = {label.name: label.id for label in task.get_labels()}

        task.set_annotations(
            LabeledDataRequest(
                version=0,
                tags=[],
                shapes=[
                    LabeledShapeRequest(
                        type="rectangle",
                        label_id=labels_by_name["cat"],
                        frame=0,
                        points=[10, 5, 30, 25],
                        occluded=False,
                        outside=False,
                        z_order=0,
                    ),
                    LabeledShapeRequest(
                        type="polygon",
                        label_id=labels_by_name["dog"],
                        frame=0,
                        points=[40, 10, 80, 10, 80, 40, 40, 40],
                        occluded=False,
                        outside=False,
                        z_order=0,
                    ),
                    LabeledShapeRequest(
                        type="points",
                        label_id=labels_by_name["keypoint"],
                        frame=0,
                        points=[15, 10],
                        occluded=False,
                        outside=False,
                        z_order=0,
                    ),
                ],
                tracks=[],
            )
        )

        annotations = task.get_annotations()
        assert len(annotations.shapes) == 3

        labels_by_id = {label.id: label.name for label in task.get_labels()}
        cvat_xml = _annotations_to_cvat_image_xml(annotations, labels_by_id, image_path)
        image_element = ET.fromstring(cvat_xml)
        boxes = image_element.findall("box")
        polygons = image_element.findall("polygon")
        points = image_element.findall("points")

        assert len(boxes) == 1
        assert boxes[0].attrib["label"] == "cat"
        np.testing.assert_allclose(
            [float(boxes[0].attrib["xtl"]), float(boxes[0].attrib["ytl"]), float(boxes[0].attrib["xbr"]), float(boxes[0].attrib["ybr"])],
            [10, 5, 30, 25],
        )
        assert len(polygons) == 1
        assert polygons[0].attrib["label"] == "dog"
        assert polygons[0].attrib["points"] == "40.0,10.0;80.0,10.0;80.0,40.0;40.0,40.0"
        assert len(points) == 1
        assert points[0].attrib["label"] == "keypoint"
        assert points[0].attrib["points"] == "15.0,10.0"
    finally:
        if task is not None:
            client.tasks.remove_by_ids([task.id])
        client.projects.remove_by_ids([project.id])
