import os
import json
import logging
import shutil
import time
from pathlib import Path
from typing import Dict, List


import numpy as np
import pandas as pd
from PIL import Image

from cv_pipeliner.core.data import ImageData, BboxData
from c12n_pipe.io.data_catalog import AbstractDataTable, DataCatalog
from c12n_pipe.io.node import StoreNode, PythonNode, LabelStudioNode, Pipeline
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import JSON, String


logger = logging.getLogger(__name__)
DBCONNSTR = f'postgresql://postgres:password@{os.getenv("POSTGRES_HOST", "localhost")}:{os.getenv("POSTGRES_PORT", 5432)}/postgres'
# DBCONNSTR = 'postgresql://postgres:qwertyisALICE666@localhost/postgres'  # FIXME
DETECTION_CONFIG_XML = Path(__file__).parent / 'detection_config.xml'
CLASSIFICATION_CONFIG_XML = Path(__file__).parent / 'classification_config.xml'


def parse_rectangle_labels(
    image_path: str,
    result: Dict
) -> BboxData:
    original_height = result['original_height']
    original_width = result['original_width']
    height = result['value']['height']
    width = result['value']['width']
    xmin = result['value']['x']
    ymin = result['value']['y']
    angle = int(result['value']['rotation'])
    label = result['value']['rectanglelabels'][0]
    if len(result['value']['rectanglelabels']) == 2:
        additional_info = json.loads(result['value']['rectanglelabels'][1])
    else:
        additional_info = {}
    xmax = xmin + width
    ymax = ymin + height
    xmin = xmin / 100 * original_width
    ymin = ymin / 100 * original_height
    xmax = xmax / 100 * original_width
    ymax = ymax / 100 * original_height
    bbox = np.array([xmin, ymin, xmax, ymax])
    bbox = bbox.round().astype(int)
    xmin, ymin, xmax, ymax = bbox
    bbox_data = BboxData(
        image_path=image_path,
        xmin=xmin,
        ymin=ymin,
        xmax=xmax,
        ymax=ymax,
        angle=angle,
        label=label,
        additional_info=additional_info
    )
    return bbox_data


def parse_detection_completion(completions_json: Dict) -> Dict:
    image_path = Path(completions_json['data']['src_image_path'])
    image_data = ImageData(image_path=image_path, bboxes_data=[])

    if 'completions' not in completions_json:
        return image_data.asdict()

    if len(completions_json['completions']) > 1:
        logger.warning(
            f"Find a task with two or more completions, fix it. Task_id: {completions_json['id']}"
        )
    completion = completions_json['completions'][-1]

    if 'skipped' in completion and completion['skipped']:
        logger.warning(f"Task {completions_json['id']} was skipped.")
        return image_data.asdict()
    else:
        image_data.bboxes_data = [
            parse_rectangle_labels(image_path=image_path, result=result)
            for result in completion['result']
            if result['from_name'] == 'bbox'
        ]

    return image_data.asdict()


def parse_classification_completion(completions_json: Dict) -> Dict:
    bbox_data = BboxData.from_dict(completions_json['data']['src_bbox_data'])

    if len(completions_json['completions']) > 1:
        logger.warning(
            f"Find a task with two or more completions, fix it. Task_id: {completions_json['id']}"
        )

    completion = completions_json['completions'][-1]

    if 'skipped' in completion and completion['skipped']:
        return bbox_data.asdict()
    else:
        label = None
        for result in completion['result']:
            from_name = result['from_name']
            if from_name == 'label':
                label = result['value']['choices'][0]
        bbox_data.label = label
    return bbox_data.asdict()


def get_source_images(images_dir: str) -> pd.DataFrame:
    image_paths = sorted(Path(images_dir).glob('*.jp*g'))
    df_source = pd.DataFrame(
        {
            'image_path': [str(image_path) for image_path in image_paths]
        },
        index=[str(id) for id in range(len(image_paths))]
    )
    return df_source


def add_tasks_to_detection_project(
    df_source: pd.DataFrame,
    project_path_detection: str,
) -> pd.DataFrame:
    project_path_detection = Path(project_path_detection)
    image_paths = df_source['image_path'].apply(Path)
    (project_path_detection / 'upload').mkdir(exist_ok=True)
    for image_path in image_paths:
        uploaded_image_path = project_path_detection / 'upload' / image_path.name
        if not uploaded_image_path.exists():
            logger.info(f'Copy {image_path} -> {uploaded_image_path}')
            shutil.copy(image_path, uploaded_image_path)

    df_source['data'] = [{
        'id': int(id),
        'data': {
            'image': f'http://localhost:8080/data/upload/{Path(image_path).name}',
            'src_image_path': image_path
        }
    } for id, image_path in zip(df_source.index, df_source['image_path'])]
    return df_source.loc[:, ['data']]


def parse_detection_annotation(df_annotation: pd.DataFrame) -> pd.DataFrame:
    df_annotation['data'] = df_annotation['data'].apply(parse_detection_completion)
    return df_annotation


# TODO: Refactor it to one-to-many transformation later
def add_tasks_to_classification_project(
    df_detection_annotation_parsed: pd.DataFrame,
    project_path_classification: str,
    bboxes_images_dir: str
) -> pd.DataFrame:
    project_path_classification = Path(project_path_classification)
    bboxes_images_dir = Path(bboxes_images_dir)
    bboxes_images_dir.mkdir(exist_ok=True)
    images_data: List[ImageData] = list(
        df_detection_annotation_parsed['data'].apply(
            lambda imaga_data_json: ImageData.from_dict(imaga_data_json)
        )
    )
    (project_path_classification / 'upload').mkdir(exist_ok=True)
    tasks_ids, src_bboxes_data, bbox_src_image_paths, bboxes_images_urls = [], [], [], []
    for task_id, image_data in zip(df_detection_annotation_parsed.index, images_data):
        image_path = image_data.image_path
        for id, bbox_data in enumerate(image_data.bboxes_data):
            bbox_task_id = str(10000 + 3000 * int(task_id) + int(id))
            bbox = (bbox_data.xmin, bbox_data.ymin, bbox_data.xmax, bbox_data.ymax)
            bbox_image_path = bboxes_images_dir / f'{task_id}_{image_path.stem}_{bbox}{image_path.suffix}'
            bbox_image_path_in_task = project_path_classification / 'upload' / bbox_image_path.name
            bbox_image_url = f'http://localhost:8090/data/upload/{Path(bbox_image_path_in_task).name}'
            if not bbox_image_path.exists():
                bbox_data_image = bbox_data.open_cropped_image()
                Image.fromarray(bbox_data_image).save(bbox_image_path)
            if not bbox_image_path_in_task.exists():
                bbox_data_image_in_task = bbox_data.open_cropped_image(
                    xmin_offset=150, ymin_offset=150, xmax_offset=150, ymax_offset=150,
                    draw_rectangle_with_color=[0, 255, 0]
                )
                Image.fromarray(bbox_data_image_in_task).save(bbox_image_path_in_task)
            tasks_ids.append(bbox_task_id)
            src_bboxes_data.append(bbox_data)
            bbox_src_image_paths.append(bbox_image_path)
            bboxes_images_urls.append(bbox_image_url)
    df_tasks = pd.DataFrame(
        {
            'data': [{
                'id': int(id),
                'data': {
                    'image': bbox_image_url,
                    'src_bbox_image_path': str(src_bbox_image_path),
                    'src_bbox_data': src_bbox_data.asdict()
                }
            } for id, src_bbox_data, bbox_image_url, src_bbox_image_path in zip(
                tasks_ids, src_bboxes_data, bboxes_images_urls, bbox_src_image_paths
            )]
        },
        index=[str(id) for id in tasks_ids]
    )
    return df_tasks


def parse_classification_annotation(df_annotation: pd.DataFrame) -> pd.DataFrame:
    df_annotation['data'] = df_annotation['data'].apply(parse_classification_completion)

    return df_annotation


def main(
    images_dir: str,
    project_path: str,
    bboxes_images_dir: str,
    connstr: str = DBCONNSTR,
    schema: str = 'detection_cls'
):
    with open(Path(__file__).absolute().parent.parent / 'logger.json', 'r') as f:
        logging.config.dictConfig(json.load(f))
    logging.root.setLevel('INFO')
    # eng = create_engine(connstr)
    # if eng.dialect.has_schema(eng, schema=schema):
    #     eng.execute(f'DROP SCHEMA {schema} CASCADE;')

    images_dir = Path(images_dir)
    project_path = Path(project_path)
    project_path_detection = project_path / 'detection'
    project_path_classification = project_path / 'classification'
    bboxes_images_dir = Path(bboxes_images_dir)
    data_catalog = DataCatalog(
        catalog={
            'input_images': AbstractDataTable([
                Column('image_path', String)
            ]),
            'detection_tasks': AbstractDataTable([
                Column('data', JSON)
            ]),
            'detection_annotation': AbstractDataTable([
                Column('data', JSON)
            ]),
            'detection_annotation_parsed': AbstractDataTable([
                Column('data', JSON)
            ]),
            'classification_tasks': AbstractDataTable([
                Column('data', JSON)
            ]),
            'classification_annotation': AbstractDataTable([
                Column('data', JSON)
            ]),
            'classification_annotation_parsed': AbstractDataTable([
                Column('data', JSON)
            ]),
        },
        connstr=connstr,
        schema=schema
    )
    pipeline = Pipeline(
        data_catalog=data_catalog,
        pipeline=[
            StoreNode(
                proc_func=get_source_images,
                kwargs={
                    'images_dir': str(images_dir)
                },
                outputs=['input_images']
            ),
            LabelStudioNode(
                project_path=project_path_detection,
                input='detection_tasks',
                output='detection_annotation',
                port=8080,
                label_config=str(DETECTION_CONFIG_XML)
            ),
            PythonNode(
                proc_func=add_tasks_to_detection_project,
                inputs=['input_images'],
                outputs=['detection_tasks'],
                kwargs={
                    'project_path_detection': str(project_path_detection)
                }
            ),
            PythonNode(
                proc_func=parse_detection_annotation,
                inputs=['detection_annotation'],
                outputs=['detection_annotation_parsed']
            ),
            LabelStudioNode(
                project_path=project_path_classification,
                input='classification_tasks',
                output='classification_annotation',
                port=8090,
                label_config=str(CLASSIFICATION_CONFIG_XML)
            ),
            PythonNode(
                proc_func=add_tasks_to_classification_project,
                inputs=['detection_annotation_parsed'],
                outputs=['classification_tasks'],
                kwargs={
                    'project_path_classification': str(project_path_classification),
                    'bboxes_images_dir': str(bboxes_images_dir)
                }
            ),
            PythonNode(
                proc_func=parse_classification_annotation,
                inputs=['classification_annotation'],
                outputs=['classification_annotation_parsed']
            )
        ]
    )

    while True:
        pipeline.run()
        time.sleep(5)


if __name__ == '__main__':
    main(
        images_dir='dataset/',
        bboxes_images_dir='dataset/bboxes',
        project_path='ls_project_detection_cls/'
    )
