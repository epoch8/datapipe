import os
import json
import logging
import shutil
import time
from pathlib import Path
from typing import Dict, Tuple
from c12n_pipe.datatable import DataTable
from c12n_pipe.label_studio_utils.label_studio_ml_node import LabelStudioMLNode
from c12n_pipe.object_detection_api_utils.object_detection_node import ObjectDetectionTrainNode
from cv_pipeliner.batch_generators.image_data import BatchGeneratorImageData
from cv_pipeliner.inference_models.detection.object_detection_api import ObjectDetectionAPI_ModelSpec
from cv_pipeliner.inferencers.detection import DetectionInferencer


import numpy as np
import pandas as pd

from cv_pipeliner.core.data import ImageData, BboxData
from c12n_pipe.io.data_catalog import AbstractDataTable, DataCatalog
from c12n_pipe.io.node import StoreNode, PythonNode, Pipeline
from c12n_pipe.label_studio_utils.label_studio_node import LabelStudioNode
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Float, JSON, Numeric, String


logger = logging.getLogger(__name__)
DBCONNSTR = f'postgresql://postgres:password@{os.getenv("POSTGRES_HOST", "localhost")}:{os.getenv("POSTGRES_PORT", 5432)}/postgres'
DETECTION_CONFIG_XML = Path(__file__).parent / 'detection_config.xml'
DETECTION_BACKEND_SCRIPT = Path(__file__).parent / 'detection_backend.py'
CURRENT_URL = 'https://k8s-ml.epoch8.co:31390/notebook/research/bobokvsky7/proxy/8080'


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
            'image': f'{CURRENT_URL}/data/upload/{Path(image_path).name}',
            'src_image_path': image_path
        }
    } for id, image_path in zip(df_source.index, df_source['image_path'])]
    return df_source.loc[:, ['data']]


def parse_detection_annotation_and_counts(df_annotation: pd.DataFrame) -> pd.DataFrame:
    df_annotation['data'] = df_annotation['data'].apply(parse_detection_completion)
    return df_annotation


def split_train_test(
    df_input_images_data: pd.DataFrame,
    test_size: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    train_indexes, test_indexes = [], []
    for idx in df_input_images_data.index:
        if np.random.rand() <= test_size:
            test_indexes.append(idx)
        else:
            train_indexes.append(idx)
    return df_input_images_data.loc[train_indexes], df_input_images_data.loc[test_indexes]


def inference_for_ml_backend(
    df_detection_models: pd.DataFrame,
    dt_detection_tasks: DataTable,
    data_catalog: DataCatalog,
    batch_size: int
):
    assert len(df_detection_models) == 1
    logger.info('Inference LS detection project data for ML backend')
    idx = df_detection_models.index[0]
    dt_detection_tasks = data_catalog.get_data_table(dt_detection_tasks)
    df_detection_tasks = dt_detection_tasks.get_data()
    images_data: ImageData = list(df_detection_tasks['data'].apply(
        lambda task_json: ImageData(image_path=task_json['data']['src_image_path'])
    ))
    detection_model_spec = ObjectDetectionAPI_ModelSpec(
        config_path=df_detection_models.loc[idx, 'config_path'],
        checkpoint_path=df_detection_models.loc[idx, 'checkpoint_path']
    )
    score_threshold = df_detection_models.loc[idx, 'score_threshold']
    detection_model = detection_model_spec.load()
    inferencer = DetectionInferencer(detection_model)
    images_data_gen = BatchGeneratorImageData(
        data=images_data, batch_size=batch_size, use_not_caught_elements_as_last_batch=True
    )
    pred_images_data = inferencer.predict(images_data_gen, score_threshold=score_threshold)
    for idx, pred_image_data in zip(df_detection_tasks.index, pred_images_data):
        df_detection_tasks.loc[idx, 'data']['pred_image_data'] = pred_image_data.asdict()
    chunk = dt_detection_tasks.store_chunk(df_detection_tasks)
    dt_detection_tasks.sync_meta(
        chunks=[chunk],
        processed_idx=df_detection_tasks.index
    )
    return df_detection_models


def main(
    images_dir: str,
    project_path: str,
    connstr: str = DBCONNSTR,
    schema: str = 'detection_train'
):
    with open(Path(__file__).absolute().parent.parent / 'logger.json', 'r') as f:
        logging.config.dictConfig(json.load(f))
    logging.root.setLevel('INFO')
    eng = create_engine(connstr)
    if eng.dialect.has_schema(eng, schema=schema):
        eng.execute(f'DROP SCHEMA {schema} CASCADE;')

    images_dir = Path(images_dir)
    project_path_base = Path(project_path)
    project_path_detection = project_path_base / 'main'
    project_path_ml = project_path_base / 'backend'
    output_model_directory = project_path_base / 'detection_models'
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
            'train_images_data': AbstractDataTable([
                Column('data', JSON)
            ]),
            'test_images_data': AbstractDataTable([
                Column('data', JSON)
            ]),
            'detection_models': AbstractDataTable([
                Column('config_path', String),
                Column('checkpoint_path', String),
                Column('score_threshold', Float)
            ]),
            'detection_models_processed': AbstractDataTable([
                Column('config_path', String),
                Column('checkpoint_path', String),
                Column('score_threshold', Float)
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
                label_config=str(DETECTION_CONFIG_XML),
                ml_backends=['http://localhost:9090/']
            ),
            LabelStudioMLNode(
                project_path=project_path_ml,
                script=str(DETECTION_BACKEND_SCRIPT),
                port=9090
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
                outputs=['input_images_data']
            ),
            PythonNode(
                proc_func=split_train_test,
                inputs=['input_images_data'],
                outputs=['train_images_data', 'test_images_data'],
                kwargs={
                    'test_size': 0.2
                }
            ),
            ObjectDetectionTrainNode(
                dt_train_images_data='train_images_data',
                dt_test_images_data='test_images_data',
                dt_output_models='detection_models',
                class_names=['bbox'],
                output_model_directory=output_model_directory,
                start_train_every_n=5,
                zoo_model_url=(
                    'http://download.tensorflow.org/models/object_detection/tf2/20200711/centernet_resnet50_v1_fpn_512x512_coco17_tpu-8.tar.gz'
                ),
                train_batch_size=4,
                train_num_steps=400,
                checkpoint_every_n=200,
            ),
            PythonNode(
                proc_func=inference_for_ml_backend,
                inputs=['detection_models'],
                outputs=['detection_models_processed'],
                kwargs={
                    'data_catalog': data_catalog,
                    'dt_detection_tasks': 'detection_tasks',
                    'batch_size': 16
                }
            ),
        ]
    )

    step = 20
    current_counter = len(data_catalog.get_data_table('detection_annotation').get_indexes()) % step

    pipeline.run_services()
    while True:
        pipeline.run(chunksize=1000)
        time.sleep(10)

        # Training run
        detection_annotation_images_len = len(data_catalog.get_data_table('detection_annotation').get_indexes())
        if detection_annotation_images_len >= current_counter + step:
            pipeline.heavy_run()
            current_counter += step
        else:
            logger.info(f"---> Current step: {detection_annotation_images_len - current_counter}/{step}")

    pipeline.terminate_services()


if __name__ == '__main__':
    main(
        images_dir='dataset/',
        project_path='ls_project/'
    )
