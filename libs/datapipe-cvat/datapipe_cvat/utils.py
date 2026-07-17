import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import pandas as pd
from cvat_sdk import Client as CVATClient


def get_cloud_storage(cvat_client: CVATClient, cloud_storage_bucket: str) -> int:
    """
    Ищет cloud storage по имени бакета в CVAT и возвращает его ID.

    :param cvat_client: Активный клиент CVAT.
    :param cloud_storage_bucket: Имя бакета вида 's3://bucket-name/...'.
    :return: ID найденного cloud storage.
    :raises ValueError: Если бакет не найден или найдено более одного совпадения.
    """

    cloud_storage_bucket = cloud_storage_bucket.split("://")[1].split("/")[0]
    cloud_storages_api, _ = cvat_client.api_client.cloudstorages_api.list()
    cloud_storages = [cs for cs in cloud_storages_api["results"] if cs["resource"] == cloud_storage_bucket]

    if len(cloud_storages) == 0:
        raise ValueError(f"Cloud storage with bucket {cloud_storage_bucket} not found")

    elif len(cloud_storages) > 1:
        raise ValueError(f"Multiple cloud storages with bucket {cloud_storage_bucket} found")

    return int(cloud_storages[0]["id"])


def extract_key(raw_path: str) -> str:
    """
    Преобразует абсолютный путь (локальный или cloud URI) в ключ (относительный путь) для CVAT.

    Примеры:
    'gs://my-bucket/folder/img.jpg' → 'folder/img.jpg'
    's3://bucket/dir/file.png'      → 'dir/file.png'
    '/home/user/data/img.jpg'       → 'home/user/data/img.jpg'

    :param raw_path: Путь к файлу.
    :return: Относительный путь внутри хранилища.
    """

    parsed = urlparse(raw_path)
    return parsed.path.lstrip("/") if parsed.scheme else raw_path.lstrip("/")


def export_job_annotations(cvat_client: CVATClient, task_id: int, file_type: Literal["image", "video"]) -> pd.DataFrame:
    """
    Экспортирует аннотации из задачи CVAT в формате XML → DataFrame.

    Для `image` возвращаются строки по кадрам.
    Для `video` — вся аннотация (tracks) возвращается одной строкой.

    :param cvat_client: Инициализированный клиент CVAT.
    :param task_id: ID задачи в CVAT.
    :param file_type: Тип данных — 'image' или 'video'.
    :return: DataFrame с колонками: ['cvat__file_path', 'annotations']
    """

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / f"task{task_id}.zip"
        format_name = "CVAT for video 1.1" if file_type == "video" else "CVAT for images 1.1"

        # Экспорт аннотаций в архив.
        task = cvat_client.tasks.retrieve(task_id)
        task.export_dataset(
            format_name=format_name,
            filename=str(zip_path),
            include_images=False,
        )

        # Распаковка и чтение XML.
        with zipfile.ZipFile(zip_path, "r") as zfile:
            xml_root = ET.fromstring(zfile.read("annotations.xml"))

        cvat__file_paths = []
        annotations = []

        if file_type == "image":
            for image_element in xml_root.findall("image"):
                cvat__file_paths.append(image_element.attrib["name"])
                # remove id and name attributes
                for attr in list(image_element.attrib):
                    if attr in ["id", "name"]:
                        del image_element.attrib[attr]
                annotations.append(ET.tostring(image_element, encoding="unicode"))

        elif file_type == "video":
            cvat__file_paths.append(task.get_meta()["frames"][0]["name"])

            video_annotation = ""
            for track_element in xml_root.findall("track"):
                # remove id and name attributes
                for attr in list(track_element.attrib):
                    if attr in ["id", "name"]:
                        del track_element.attrib[attr]
                video_annotation += ET.tostring(track_element, encoding="unicode")

            annotations.append(video_annotation)

        else:
            raise ValueError(f"Unsupported file_type: {file_type}")

        return pd.DataFrame(
            {
                "cvat__file_path": cvat__file_paths,
                "annotations": annotations,
            }
        )
