import os
import time
import xml.etree.ElementTree as ET
from uuid import uuid4

import numpy as np
import pytest
import requests
from cvat_sdk.models import LabeledDataRequest, LabeledShapeRequest, PatchedLabelRequest, ProjectWriteRequest
from PIL import Image

from datapipe_cvat.cvat_step import create_cvat_client


pytestmark = pytest.mark.cvat


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
        if shape.type == "rectangle":
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
        elif shape.type == "polygon":
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
        elif shape.type == "points":
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


def test_real_cvat_annotations_roundtrip_with_image_data_converter(tmp_dir, cvat_url, cvat_credentials):
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
