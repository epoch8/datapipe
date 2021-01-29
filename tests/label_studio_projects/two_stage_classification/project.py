import json
import logging
import time
from pathlib import Path
from typing import Dict, List
from c12n_pipe.datatable import DataTable
from cv_pipeliner.data_converters.brickit import BrickitDataConverter


import pandas as pd
from PIL import Image

from cv_pipeliner.core.data import BboxData
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import JSON, String
from tqdm import tqdm
from c12n_pipe.io.catalog import AbstractDataTable, DataCatalog
from c12n_pipe.io.node import StoreNode, PythonNode, LabelStudioNode, Pipeline


logger = logging.getLogger(__name__)
DBCONNSTR = f'postgresql://postgres:password@{os.getenv("POSTGRES_HOST", "localhost")}:{os.getenv("POSTGRES_PORT", 5432)}/postgres'
# DBCONNSTR = 'postgresql://postgres:qwertyisALICE666@localhost/postgres'  # FIXME
STAGE1_CONFIG_XML = Path(__file__).parent / 'stage1_config.xml'
STAGE2_CONFIG_XML = Path(__file__).parent / 'stage2_config.xml'


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


def get_source_bboxes(
    images_dir: str,
    src_annotation_path: str
) -> pd.DataFrame:
    images_dir = Path(images_dir)
    src_annotation_path = Path(src_annotation_path)
    image_paths = sorted(images_dir.glob('*.jp*g'))
    images_data = BrickitDataConverter().get_images_data_from_annots(
        image_paths=image_paths,
        annots=src_annotation_path
    )
    task_id = 0
    tasks_ids, bboxes_data = [], []
    for image_data in images_data:
        for bbox_data in image_data.bboxes_data:
            tasks_ids.append(task_id)
            bboxes_data.append(bbox_data)
            task_id += 1

    df_source = pd.DataFrame(
        {
            'data': [json.dumps(bbox_data.asdict()) for bbox_data in bboxes_data]
        },
        index=[str(id) for id in tasks_ids]
    )
    df_source.index.name = 'id'
    return df_source


def add_tasks_to_project(
    df_source: pd.DataFrame,
    bboxes_images_dir: str,
    project_path: str,
    port: int
) -> pd.DataFrame:
    bboxes_data: List[BboxData] = list(
        df_source['data'].apply(
            lambda bbox_data_json: BboxData.from_dict(json.loads(bbox_data_json))
        )
    )
    bboxes_images_dir = Path(bboxes_images_dir)
    project_path = Path(project_path)
    bboxes_images_dir.mkdir(exist_ok=True)
    (project_path / 'upload').mkdir(exist_ok=True)
    tasks_ids, src_bboxes_data, bbox_src_image_paths, bboxes_images_urls = [], [], [], []
    logger.info('Adding tasks to stage1 project')
    for task_id, bbox_data in tqdm(zip(df_source.index, bboxes_data), total=len(bboxes_data)):
        image_path = bbox_data.image_path
        bbox = (bbox_data.xmin, bbox_data.ymin, bbox_data.xmax, bbox_data.ymax)
        bbox_image_path = bboxes_images_dir / f'{task_id}_{image_path.stem}_{bbox}{image_path.suffix}'
        bbox_image_path_in_task = project_path / 'upload' / bbox_image_path.name
        bbox_image_url = f'http://localhost:{port}/data/upload/{Path(bbox_image_path_in_task).name}'
        if not bbox_image_path.exists():
            bbox_data_image = bbox_data.open_cropped_image()
            Image.fromarray(bbox_data_image).save(bbox_image_path)
        if not bbox_image_path_in_task.exists():
            bbox_data_image_in_task = bbox_data.open_cropped_image(
                xmin_offset=150, ymin_offset=150, xmax_offset=150, ymax_offset=150,
                draw_rectangle_with_color=[0, 255, 0]
            )
            Image.fromarray(bbox_data_image_in_task).save(bbox_image_path_in_task)
        tasks_ids.append(task_id)
        src_bboxes_data.append(bbox_data)
        bbox_src_image_paths.append(bbox_image_path)
        bboxes_images_urls.append(bbox_image_url)

    df_source['data'] = [json.dumps({
        'id': int(id),
        'data': {
            'image': bbox_image_url,
            'src_bbox_image_path': str(src_bbox_image_path),
            'src_bbox_data': src_bbox_data.asdict()
        }
    }) for id, src_bbox_data, bbox_image_url, src_bbox_image_path in zip(
        tasks_ids, src_bboxes_data, bboxes_images_urls, bbox_src_image_paths
    )]

    return df_source[['data']]


def parse_annotation(df_annotation: pd.DataFrame) -> pd.DataFrame:
    df_annotation['data'] = df_annotation['data'].apply(parse_classification_completion)
    df_annotation['data'] = df_annotation['data'].apply(json.dumps)
    return df_annotation


def get_stage1_good_and_bad_annotation(df_annotation: pd.DataFrame) -> pd.DataFrame:
    bboxes_data: List[BboxData] = list(
        df_annotation['data'].apply(
            lambda bbox_data_json: BboxData.from_dict(json.loads(bbox_data_json))
        )
    )
    good_idxs = [
        idx for idx, bbox_data in zip(df_annotation.index, bboxes_data)
        if bbox_data.label != 'to_be_annotated_(hard)'
    ]
    bad_idxs = [
        idx for idx, bbox_data in zip(df_annotation.index, bboxes_data)
        if bbox_data.label == 'to_be_annotated_(hard)'
    ]
    df_annotation_good = df_annotation.copy()
    df_annotation_good.drop(index=bad_idxs, inplace=True)
    df_annotation_bad = df_annotation.copy()
    df_annotation_bad.drop(index=good_idxs, inplace=True)
    return df_annotation_good, df_annotation_bad


def get_df_total_annotation(
    df_stage1_good_annotation: pd.DataFrame,
    df_stage2_parsed_annotation: pd.DataFrame
) -> pd.DataFrame:
    assert len(set(df_stage1_good_annotation.index) & set(df_stage2_parsed_annotation.index)) == 0
    df_annotation = pd.concat([df_stage1_good_annotation, df_stage2_parsed_annotation])
    return df_annotation


def main(
    src_annotation_path: str,
    bboxes_images_dir: str,
    images_dir: str,
    project_path: str,
    connstr: str = DBCONNSTR,
    schema: str = 'two_stage_cls'
):
    with open(Path(__file__).absolute().parent.parent / 'logger.json', 'r') as f:
        logging.config.dictConfig(json.load(f))
    logging.root.setLevel('INFO')
    # eng = create_engine(connstr)
    # if eng.dialect.has_schema(eng, schema=schema):
    #     eng.execute(f'DROP SCHEMA {schema} CASCADE;')

    images_dir = Path(images_dir)
    src_annotation_path = str(src_annotation_path)
    bboxes_images_dir = Path(bboxes_images_dir)
    images_dir = Path(images_dir)
    project_path = Path(project_path)
    project_path_stage1 = project_path / 'stage1'
    project_path_stage2 = project_path / 'stage2'
    catalog = DataCatalog(
        catalog={
            'input_bboxes': AbstractDataTable([
                Column('data', String)
            ]),
            'stage1_tasks': AbstractDataTable([
                Column('data', String)
            ]),
            'stage1_annotation': AbstractDataTable([
                Column('data', JSON)
            ]),
            'stage1_annotation_parsed': AbstractDataTable([
                Column('data', JSON)
            ]),
            'stage1_annotation_parsed_good': AbstractDataTable([
                Column('data', JSON)
            ]),
            'stage1_annotation_parsed_bad': AbstractDataTable([
                Column('data', JSON)
            ]),
            'stage2_tasks': AbstractDataTable([
                Column('data', String)
            ]),
            'stage2_annotation': AbstractDataTable([
                Column('data', JSON)
            ]),
            'stage2_annotation_parsed': AbstractDataTable([
                Column('data', JSON)
            ]),
            'total_annotation': AbstractDataTable([
                Column('data', JSON)
            ]),
        },
        connstr=connstr,
        schema=schema
    )
    pipeline = Pipeline(
        catalog=catalog,
        pipeline=[
            StoreNode(
                proc_func=get_source_bboxes,
                kwargs={
                    'images_dir': str(images_dir),
                    'src_annotation_path': str(src_annotation_path)
                },
                outputs=['input_bboxes']
            ),
            LabelStudioNode(
                project_path=project_path_stage1,
                input='stage1_tasks',
                output='stage1_annotation',
                port=8080,
                label_config=str(STAGE1_CONFIG_XML)
            ),
            PythonNode(
                proc_func=add_tasks_to_project,
                inputs=['input_bboxes'],
                outputs=['stage1_tasks'],
                kwargs={
                    'bboxes_images_dir': str(bboxes_images_dir),
                    'project_path': str(project_path_stage1),
                    'port': 8080
                }
            ),
            PythonNode(
                proc_func=parse_annotation,
                inputs=['stage1_annotation'],
                outputs=['stage1_annotation_parsed']
            ),
            PythonNode(
                proc_func=get_stage1_good_and_bad_annotation,
                inputs=['stage1_annotation_parsed'],
                outputs=['stage1_annotation_parsed_good', 'stage1_annotation_parsed_bad']
            ),
            LabelStudioNode(
                project_path=project_path_stage2,
                input='stage2_tasks',
                output='stage2_annotation',
                port=8090,
                label_config=str(STAGE2_CONFIG_XML)
            ),
            PythonNode(
                proc_func=add_tasks_to_project,
                inputs=['stage1_annotation_parsed_bad'],
                outputs=['stage2_tasks'],
                kwargs={
                    'bboxes_images_dir': str(bboxes_images_dir),
                    'project_path': str(project_path_stage2),
                    'port': 8090
                }
            ),
            PythonNode(
                proc_func=parse_annotation,
                inputs=['stage2_annotation'],
                outputs=['stage2_annotation_parsed']
            ),
            PythonNode(
                proc_func=get_df_total_annotation,
                inputs=['stage1_annotation_parsed_good', 'stage2_annotation'],
                outputs=['total_annotation']
            ),
        ]
    )

    while True:
        pipeline.run()
        time.sleep(5)


if __name__ == '__main__':
    main(
        images_dir='dataset/',
        src_annotation_path='dataset/annotations.json',
        bboxes_images_dir='dataset/bboxes/',
        project_path='ls_project_test_two_stage/'
    )
