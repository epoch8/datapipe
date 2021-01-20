import json
import logging
import shutil
import time

from multiprocessing import Process
from pathlib import Path
from typing import Callable, Dict, List, Literal, Union
from c12n_pipe.datatable import inc_process
from c12n_pipe.label_studio_utils.filesystem_modified import (
    DATA_JSON_SQL_SCHEMA, DATA_STORE, TASKS_JSON_SQL_SCHEMA, get_data_table_name_from_project
)
import numpy as np
from PIL import Image
from sqlalchemy.sql.sqltypes import String
import sqlalchemy as sql

import pandas as pd

from label_studio.project import Project
from cv_pipeliner.core.data import ImageData, BboxData
from cv_pipeliner.data_converters.brickit import BrickitDataConverter
from c12n_pipe.label_studio_utils.label_studio_c12n import LabelStudioConfig, run_app


logger = logging.getLogger(__name__)
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


def parse_detection_completion(completions_json: Dict) -> ImageData:
    image_path = Path(completions_json['data']['src_image_path'])
    print(f"parse_detection_completion: {image_path=}")
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


def parse_classification_completion(
    completions_json: Dict
):
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


class LabelStudioProject_Detection_Classification:
    def __init__(
        self,
        images_dir: str,
        bboxes_images_dir: str,
        annotation_path: str,
        project_path: Union[str, Path],
        log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR'] = None
    ):
        self.images_dir = Path(images_dir)
        self.bboxes_images_dir = Path(bboxes_images_dir)
        self.bboxes_images_dir.mkdir(parents=True, exist_ok=True)
        self.annotation_path = annotation_path
        self.project_path = Path(project_path)
        self.log_level = log_level

        self.project_path_detection = self.project_path / 'detection'
        self.project_path_classification = self.project_path / 'classification'
        self.label_studio_config_detection = LabelStudioConfig(
            project_name=str(self.project_path_detection),
            source='tasks-json-modified',
            source_path='tasks.json',
            target='completions-dir-modified',
            target_path='completions',
            port=8080,
            label_config=str(DETECTION_CONFIG_XML),
            log_level=log_level
        )
        detection_project = Project.get_or_create(
            self.label_studio_config_detection.project_name,
            self.label_studio_config_detection,
            context={'multi_session': False}
        )
        detection_project_config = Project.get_config(
            self.label_studio_config_detection.project_name,
            self.label_studio_config_detection
        )
        self.dt_tasks_source_name = get_data_table_name_from_project(
            project=detection_project,
            base_data_table_name='tasks_json_source'
        )
        self.dt_detection_tasks_name = detection_project_config['data_table_name_tasks']
        self.dt_detection_annotation_name = detection_project_config['data_table_name_annotation']
        self.dt_detection_annotation_parsed_name = get_data_table_name_from_project(
            project=detection_project,
            base_data_table_name='annotation_parsed'
        )

        self.label_studio_config_classification = LabelStudioConfig(
            project_name=str(self.project_path_classification),
            source='tasks-json-modified-no-set-no-remove',
            source_path='tasks.json',
            target='completions-dir-modified',
            target_path='completions',
            port=8090,
            label_config=str(CLASSIFICATION_CONFIG_XML),
            log_level=log_level
        )
        Project.get_or_create(
            self.label_studio_config_classification.project_name,
            self.label_studio_config_classification,
            context={'multi_session': False}
        )
        classification_project_config = Project.get_config(
            self.label_studio_config_classification.project_name,
            self.label_studio_config_classification
        )
        self.dt_classification_tasks_name = classification_project_config['data_table_name_tasks']
        self.dt_classification_annotation_name = classification_project_config['data_table_name_annotation']
        self.dt_classification_annotation_parsed_name = get_data_table_name_from_project(
            project=detection_project,
            base_data_table_name='annotation_parsed'
        )

        self._define_data_tables()

    def _define_data_tables(self):
        self.dt_tasks_source = DATA_STORE.get_table(
            name=self.dt_tasks_source_name,
            data_sql_schema=[sql.Column('image_path', String)]
        )
        self.dt_detection_tasks = DATA_STORE.get_table(
            name=self.dt_detection_tasks_name,
            data_sql_schema=TASKS_JSON_SQL_SCHEMA
        )
        self.dt_detection_annotation = DATA_STORE.get_table(
            name=self.dt_detection_annotation_name,
            data_sql_schema=DATA_JSON_SQL_SCHEMA
        )
        self.dt_detection_annotation_parsed = DATA_STORE.get_table(
            name=self.dt_detection_annotation_parsed_name,
            data_sql_schema=DATA_JSON_SQL_SCHEMA
        )
        self.dt_classification_tasks = DATA_STORE.get_table(
            name=self.dt_classification_tasks_name,
            data_sql_schema=TASKS_JSON_SQL_SCHEMA
        )
        self.dt_classification_annotation = DATA_STORE.get_table(
            name=self.dt_classification_annotation_name,
            data_sql_schema=DATA_JSON_SQL_SCHEMA
        )
        self.dt_classification_annotation_parsed = DATA_STORE.get_table(
            name=self.dt_classification_annotation_parsed_name,
            data_sql_schema=DATA_JSON_SQL_SCHEMA
        )

    def run_detection(self):
        run_app(
            label_studio_config=self.label_studio_config_detection
        )

    def run_classification(self):
        run_app(
            label_studio_config=self.label_studio_config_classification
        )

    def get_source_images(self):
        images_paths = sorted(self.images_dir.glob('*.jp*g'))
        df_source = pd.DataFrame(
            {
                'image_path': [str(image_path) for image_path in images_paths]
            },
            index=[str(id) for id in range(len(images_paths))]
        )
        df_source.index.name = 'id'
        self.dt_tasks_source.store(df=df_source)

    def add_tasks_to_detection_project(self):
        def proc_func(df_source: pd.DataFrame) -> pd.DataFrame:
            images_paths = df_source['image_path'].apply(Path)
            (self.project_path_detection / 'upload').mkdir(exist_ok=True)
            for image_path in images_paths:
                uploaded_image_path = self.project_path_detection / 'upload' / image_path.name
                if not uploaded_image_path.exists():
                    logger.info(f'Copy {image_path} -> {uploaded_image_path}')
                    shutil.copy(image_path, uploaded_image_path)

            df_source['data'] = [json.dumps({
                'id': int(id),
                'data': {
                    'image': f'http://localhost:8080/data/upload/{Path(image_path).name}',
                    'src_image_path': image_path
                }
            }) for id, image_path in zip(df_source.index, df_source['image_path'])]

            return df_source[['data']]

        inc_process(
            ds=DATA_STORE,
            input_dts=[self.dt_tasks_source],
            res_dt=self.dt_detection_tasks,
            proc_func=proc_func
        )

    def parse_detection_annotation(self):
        def proc_func(df_annotation: pd.DataFrame) -> pd.DataFrame:
            df_annotation['data'] = df_annotation['data'].apply(json.loads)
            df_annotation['data'] = df_annotation['data'].apply(parse_detection_completion)
            df_annotation['data'] = df_annotation['data'].apply(json.dumps)

            return df_annotation

        inc_process(
            ds=DATA_STORE,
            input_dts=[self.dt_detection_annotation],
            res_dt=self.dt_detection_annotation_parsed,
            proc_func=proc_func
        )

    # TODO: Refactor it to one-to-many transformation later
    def add_tasks_to_classification_project(self):
        df_detection_annotation_parsed = self.dt_detection_annotation_parsed.get_data()
        images_data: List[ImageData] = list(
            df_detection_annotation_parsed['data'].apply(
                lambda imaga_data_json: ImageData.from_dict(json.loads(imaga_data_json))
            )
        )
        tasks_ids, src_bboxes_data, bbox_src_images_paths, bboxes_images_urls = [], [], [], []
        for task_id, image_data in zip(df_detection_annotation_parsed.index, images_data):
            image_path = image_data.image_path
            for id, bbox_data in enumerate(image_data.bboxes_data):
                bbox_task_id = str(1000 + 3000 * int(task_id) + int(id))
                bbox = (bbox_data.xmin, bbox_data.ymin, bbox_data.xmax, bbox_data.ymax)
                bbox_image_path = self.bboxes_images_dir / f'{task_id}_{image_path.stem}_{bbox}{image_path.suffix}'
                (self.project_path_classification / 'upload').mkdir(exist_ok=True)
                bbox_image_path_in_task = self.project_path_classification / 'upload' / bbox_image_path.name
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
                bbox_src_images_paths.append(bbox_image_path)
                bboxes_images_urls.append(bbox_image_url)
        df_tasks = pd.DataFrame(
            {
                'data': [json.dumps({
                    'id': int(id),
                    'data': {
                        'image': bbox_image_url,
                        'src_bbox_image_path': str(src_bbox_image_path),
                        'src_bbox_data': src_bbox_data.asdict()
                    }
                }) for id, src_bbox_data, bbox_image_url, src_bbox_image_path in zip(
                    tasks_ids, src_bboxes_data, bboxes_images_urls, bbox_src_images_paths
                )]
            },
            index=[str(id) for id in tasks_ids]
        )
        df_tasks.index.name = 'id'
        self.dt_classification_tasks.store(df=df_tasks)

        # def proc_func(df_tasks: pd.DataFrame) -> pd.DataFrame:
        #     return df_tasks

        # inc_process(
        #     ds=DATA_STORE,
        #     input_dts=[self.dt_detection_tasks],
        #     res_dt=self.dt_classification_tasks,
        #     proc_func=proc_func
        # )

    def parse_classification_annotation(self):
        def proc_func(df_annotation: pd.DataFrame) -> pd.DataFrame:
            df_annotation['data'] = df_annotation['data'].apply(json.loads)
            df_annotation['data'] = df_annotation['data'].apply(parse_classification_completion)
            df_annotation['data'] = df_annotation['data'].apply(json.dumps)

            return df_annotation

        inc_process(
            ds=DATA_STORE,
            input_dts=[self.dt_classification_annotation],
            res_dt=self.dt_classification_annotation_parsed,
            proc_func=proc_func
        )

    def get_total_annotation_json(self):
        df_cls_annotation_parsed = self.dt_classification_annotation_parsed.get_data()
        bboxes_data: List[ImageData] = list(
            df_cls_annotation_parsed['data'].apply(
                lambda imaga_data_json: BboxData.from_dict(json.loads(imaga_data_json))
            )
        )
        print(f"{bboxes_data=}")
        image_path_to_bboxes_data = {}
        for bbox_data in bboxes_data:
            if bbox_data.image_path not in image_path_to_bboxes_data:
                image_path_to_bboxes_data[bbox_data.image_path] = [bbox_data]
            else:
                image_path_to_bboxes_data[bbox_data.image_path].append(bbox_data)
        images_data = [
            ImageData(image_path=image_path, bboxes_data=image_path_to_bboxes_data[image_path])
            for image_path in image_path_to_bboxes_data
        ]
        annotation_json = BrickitDataConverter().get_annot_from_images_data(images_data)
        with open(self.annotation_path, 'w') as out:
            json.dump(annotation_json, out)
        logger.info(f"Annotation saved: {self.annotation_path}")

    def _callbacks(self) -> List[Callable[[], None]]:
        return [
            self.get_source_images,
            self.add_tasks_to_detection_project,
            self.parse_detection_annotation,
            self.add_tasks_to_classification_project,
            self.parse_classification_annotation,
            self.get_total_annotation_json
        ]

    def run_callbacks(
        self,
        sleep: int = 5
    ):
        if self.log_level is not None:
            logging.root.setLevel(self.log_level)
        while True:
            logging.info("Callback running...")
            for callback in self._callbacks():
                callback()
            time.sleep(sleep)

    def run_project(self):
        try:
            logger.info('Start project...')
            processes = [
                Process(target=self.run_detection),
                Process(target=self.run_classification),
                Process(target=self.run_callbacks)
            ]
            for p in processes:
                p.start()
        finally:
            for p in processes:
                p.join()


if __name__ == "__main__":
    project = LabelStudioProject_Detection_Classification(
        images_dir='/mnt/c/Users/bobokvsky/YandexDisk-bobok100500@yandex.ru/Job/images/dataset_example',
        bboxes_images_dir='/mnt/c/Users/bobokvsky/YandexDisk-bobok100500@yandex.ru/Job/images/dataset_example/bboxes',
        annotation_path='/mnt/c/Users/bobokvsky/YandexDisk-bobok100500@yandex.ru/Job/images/dataset_example/ls_annotation.json',
        project_path='/mnt/c/Users/bobokvsky/YandexDisk-bobok100500@yandex.ru/Job/temp/ls_project_test',
        log_level='INFO'
    )
    project.run_project()
