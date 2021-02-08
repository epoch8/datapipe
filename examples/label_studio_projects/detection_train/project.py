import os
import json
import logging
import shutil
import time
from pathlib import Path
from typing import Dict
from c12n_pipe.object_detection_api_utils.object_detection_node import ObjectDetectionTrainNode


import numpy as np
import pandas as pd

from cv_pipeliner.core.data import ImageData, BboxData
from c12n_pipe.io.data_catalog import AbstractDataTable, DataCatalog
from c12n_pipe.io.node import StoreNode, PythonNode, Pipeline
from c12n_pipe.label_studio_utils.label_studio_node import LabelStudioNode
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import JSON, Numeric, String


logger = logging.getLogger(__name__)
DBCONNSTR = f'postgresql://postgres:password@{os.getenv("POSTGRES_HOST", "localhost")}:{os.getenv("POSTGRES_PORT", 5432)}/postgres'
DETECTION_CONFIG_XML = Path(__file__).parent / 'detection_config.xml'


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
    image_paths = sorted(
        Path(images_dir).glob('*.jp*g'),
        key=lambda image_path: image_path.stat().st_mtime
    )
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


class Counter:
    value = 0


def parse_detection_annotation_and_counts(
    df_annotation: pd.DataFrame,
    counter: Counter
) -> pd.DataFrame:
    df_annotation['data'] = df_annotation['data'].apply(parse_detection_completion)
    counter.value += 1
    return df_annotation


def main(
    images_dir: str,
    project_path_detection: str,
    connstr: str = DBCONNSTR,
    schema: str = 'detection_train'
):
    with open(Path(__file__).absolute().parent.parent / 'logger.json', 'r') as f:
        logging.config.dictConfig(json.load(f))
    logging.root.setLevel('INFO')
    eng = create_engine(connstr)
    if eng.dialect.has_schema(eng, schema=schema):
        eng.execute(f'DROP SCHEMA {schema} CASCADE;')

    counter = Counter()

    images_dir = Path(images_dir)
    project_path_detection = Path(project_path_detection)
    output_model_directory = project_path_detection / 'detection_models'
    data_catalog = DataCatalog(
        catalog={
            'input_images': AbstractDataTable([
                Column('image_path', String)
            ]),
            'counter': AbstractDataTable([
                Column('counter', Numeric)
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
            'input_images_data': AbstractDataTable([
                Column('data', JSON)
            ]),
            'detection_models': AbstractDataTable([
                Column('model_directory', JSON)
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
                proc_func=parse_detection_annotation_and_counts,
                inputs=['detection_annotation'],
                outputs=['input_images_data'],
                kwargs={
                    'counter': counter
                }
            ),
            ObjectDetectionTrainNode(
                dt_input_images_data='input_images_data',
                dt_output_models='detection_models',
                class_names=['bbox'],
                output_model_directory=output_model_directory,
                start_train_every_n=2,
                zoo_model_url=(
                    'http://download.tensorflow.org/models/object_detection/tf2/20200711/ssd_mobilenet_v2_320x320_coco17_tpu-8.tar.gz'
                ),
                train_batch_size=2,
                train_num_steps=50,
                checkpoint_every_n=10,
            )
        ]
    )

    while True:
        pipeline.run(
            chunksize=1000,
            object_detection_node_count_value=counter.value
        )
        time.sleep(5)


if __name__ == '__main__':
    main(
        images_dir='dataset/',
        project_path_detection='ls_project/'
    )
