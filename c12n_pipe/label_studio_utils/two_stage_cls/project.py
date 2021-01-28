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
from tqdm import tqdm


logger = logging.getLogger(__name__)
STAGE1_CONFIG_XML = Path(__file__).parent / 'stage1_config.xml'
STAGE2_CONFIG_XML = Path(__file__).parent / 'stage2_config.xml'


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


class LabelStudioProject_TwoStageClassfication:
    def __init__(
        self,
        images_dir: str,
        src_annotation_path: str,
        bboxes_images_dir: str,
        total_annotation_path: str,
        project_path: Union[str, Path],
        log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR'] = None
    ):
        self.images_dir = Path(images_dir)
        self.src_annotation_path = Path(src_annotation_path)
        self.bboxes_images_dir = Path(bboxes_images_dir)
        self.bboxes_images_dir.mkdir(parents=True, exist_ok=True)
        self.total_annotation_path = total_annotation_path
        self.project_path = Path(project_path)
        self.log_level = log_level

        self.project_path_stage1 = self.project_path / 'stage1'
        self.project_path_stage2 = self.project_path / 'stage2'
        self.label_studio_config_stage1 = LabelStudioConfig(
            project_name=str(self.project_path_stage1),
            source='tasks-json-modified-no-set-no-remove',
            source_path='tasks.json',
            target='completions-dir-modified',
            target_path='completions',
            port=8080,
            label_config=str(STAGE1_CONFIG_XML),
            log_level=log_level
        )
        stage1_project = Project.get_or_create(
            self.label_studio_config_stage1.project_name,
            self.label_studio_config_stage1,
            context={'multi_session': False}
        )
        stage1_project_config = Project.get_config(
            self.label_studio_config_stage1.project_name,
            self.label_studio_config_stage1
        )
        self.dt_tasks_source_name = get_data_table_name_from_project(
            project=stage1_project,
            base_data_table_name='tasks_json_source'
        )
        self.dt_stage1_tasks_name = stage1_project_config['data_table_name_tasks']
        self.dt_stage1_annotation_name = stage1_project_config['data_table_name_annotation']
        self.dt_stage1_annotation_parsed_name = get_data_table_name_from_project(
            project=stage1_project,
            base_data_table_name='annotation_parsed'
        )
        self.dt_stage1_good_annotation_name = get_data_table_name_from_project(
            project=stage1_project,
            base_data_table_name='annotation_good'
        )

        self.label_studio_config_stage2 = LabelStudioConfig(
            project_name=str(self.project_path_stage2),
            source='tasks-json-modified-no-set-no-remove',
            source_path='tasks.json',
            target='completions-dir-modified',
            target_path='completions',
            port=8090,
            label_config=str(STAGE2_CONFIG_XML),
            log_level=log_level
        )
        stage2_project = Project.get_or_create(
            self.label_studio_config_stage2.project_name,
            self.label_studio_config_stage2,
            context={'multi_session': False}
        )
        stage2_project_config = Project.get_config(
            self.label_studio_config_stage2.project_name,
            self.label_studio_config_stage2
        )
        self.dt_stage2_tasks_name = stage2_project_config['data_table_name_tasks']
        self.dt_stage2_annotation_name = stage2_project_config['data_table_name_annotation']
        self.dt_stage2_annotation_parsed_name = get_data_table_name_from_project(
            project=stage2_project,
            base_data_table_name='annotation_parsed'
        )

        self.df_total_annotation_name = get_data_table_name_from_project(
            project=stage2_project,
            base_data_table_name='total_annotation'
        )

        self._define_data_tables()

    def _define_data_tables(self):
        self.dt_tasks_source = DATA_STORE.get_table(
            name=self.dt_tasks_source_name,
            data_sql_schema=[sql.Column('bbox_data', String)]
        )
        self.dt_stage1_tasks = DATA_STORE.get_table(
            name=self.dt_stage1_tasks_name,
            data_sql_schema=TASKS_JSON_SQL_SCHEMA
        )
        self.dt_stage1_annotation = DATA_STORE.get_table(
            name=self.dt_stage1_annotation_name,
            data_sql_schema=DATA_JSON_SQL_SCHEMA
        )
        self.dt_stage1_annotation_parsed = DATA_STORE.get_table(
            name=self.dt_stage1_annotation_parsed_name,
            data_sql_schema=DATA_JSON_SQL_SCHEMA
        )
        self.dt_stage1_good_annotation = DATA_STORE.get_table(
            name=self.dt_stage1_good_annotation_name,
            data_sql_schema=DATA_JSON_SQL_SCHEMA
        )
        self.dt_stage2_tasks = DATA_STORE.get_table(
            name=self.dt_stage2_tasks_name,
            data_sql_schema=TASKS_JSON_SQL_SCHEMA
        )
        self.dt_stage2_annotation = DATA_STORE.get_table(
            name=self.dt_stage2_annotation_name,
            data_sql_schema=DATA_JSON_SQL_SCHEMA
        )
        self.dt_stage2_annotation_parsed = DATA_STORE.get_table(
            name=self.dt_stage2_annotation_parsed_name,
            data_sql_schema=DATA_JSON_SQL_SCHEMA
        )
        self.df_total_annotation = DATA_STORE.get_table(
            name=self.df_total_annotation_name,
            data_sql_schema=DATA_JSON_SQL_SCHEMA
        )

    def run_stage1(self):
        run_app(
            label_studio_config=self.label_studio_config_stage1
        )

    def run_stage2(self):
        run_app(
            label_studio_config=self.label_studio_config_stage2
        )

    def get_source_bboxes(self):
        image_paths = sorted(self.images_dir.glob('*.jp*g'))
        images_data = BrickitDataConverter().get_images_data_from_annots(
            image_paths=image_paths,
            annots=self.src_annotation_path
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
                'bbox_data': [json.dumps(bbox_data.asdict()) for bbox_data in bboxes_data]
            },
            index=[str(id) for id in tasks_ids]
        )
        df_source.index.name = 'id'
        self.dt_tasks_source.store(df=df_source)

    def add_tasks_to_stage1_project(self):
        def proc_func(df_source: pd.DataFrame) -> pd.DataFrame:
            bboxes_data: List[BboxData] = list(
                df_source['bbox_data'].apply(
                    lambda bbox_data_json: BboxData.from_dict(json.loads(bbox_data_json))
                )
            )
            (self.project_path_stage1 / 'upload').mkdir(exist_ok=True)
            tasks_ids, src_bboxes_data, bbox_src_image_paths, bboxes_images_urls = [], [], [], []
            logging.info('Adding tasks to stage1 p')
            for task_id, bbox_data in tqdm(zip(df_source.index, bboxes_data), total=len(bboxes_data)):
                image_path = bbox_data.image_path
                bbox = (bbox_data.xmin, bbox_data.ymin, bbox_data.xmax, bbox_data.ymax)
                bbox_image_path = self.bboxes_images_dir / f'{task_id}_{image_path.stem}_{bbox}{image_path.suffix}'
                (self.project_path_stage2 / 'upload').mkdir(exist_ok=True)
                bbox_image_path_in_task = self.project_path_stage1 / 'upload' / bbox_image_path.name
                bbox_image_url = f'http://localhost:8080/data/upload/{Path(bbox_image_path_in_task).name}'
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

        inc_process(
            ds=DATA_STORE,
            input_dts=[self.dt_tasks_source],
            res_dt=self.dt_stage1_tasks,
            proc_func=proc_func
        )

    def parse_stage1_annotation(self):
        def proc_func(df_annotation: pd.DataFrame) -> pd.DataFrame:
            df_annotation['data'] = df_annotation['data'].apply(json.loads)
            df_annotation['data'] = df_annotation['data'].apply(parse_classification_completion)
            df_annotation['data'] = df_annotation['data'].apply(json.dumps)

            return df_annotation

        inc_process(
            ds=DATA_STORE,
            input_dts=[self.dt_stage1_annotation],
            res_dt=self.dt_stage1_annotation_parsed,
            proc_func=proc_func
        )

    def get_stage1_good_annotation(self):
        def proc_func(df_annotation: pd.DataFrame) -> pd.DataFrame:
            bboxes_data: List[BboxData] = list(
                df_annotation['data'].apply(
                    lambda bbox_data_json: BboxData.from_dict(json.loads(bbox_data_json))
                )
            )
            bad_idxs = [
                idx for idx, bbox_data in zip(df_annotation.index, bboxes_data)
                if bbox_data.label == 'to_be_annotated_(hard)'
            ]
            df_annotation.drop(index=bad_idxs, inplace=True)
            return df_annotation

        inc_process(
            ds=DATA_STORE,
            input_dts=[self.dt_stage1_annotation_parsed],
            res_dt=self.dt_stage1_good_annotation,
            proc_func=proc_func
        )

    # TODO: Refactor it to one-to-many transformation later
    def add_tasks_to_stage2_project(self):
        def proc_func(df_annotation: pd.DataFrame) -> pd.DataFrame:
            bboxes_data: List[BboxData] = list(
                df_annotation['data'].apply(
                    lambda bbox_data_json: BboxData.from_dict(json.loads(bbox_data_json))
                )
            )
            (self.project_path_stage2 / 'upload').mkdir(exist_ok=True)
            tasks_ids, src_bboxes_data, bbox_src_image_paths, bboxes_images_urls = [], [], [], []
            for task_id, bbox_data in zip(df_annotation.index, bboxes_data):
                if bbox_data.label != 'to_be_annotated_(hard)':
                    continue
                image_path = bbox_data.image_path
                bbox = (bbox_data.xmin, bbox_data.ymin, bbox_data.xmax, bbox_data.ymax)
                bbox_image_path = self.bboxes_images_dir / f'{task_id}_{image_path.stem}_{bbox}{image_path.suffix}'
                (self.project_path_stage2 / 'upload').mkdir(exist_ok=True)
                bbox_image_path_in_task = self.project_path_stage2 / 'upload' / bbox_image_path.name
                bbox_image_url = f'http://localhost:8090/data/upload/{Path(bbox_image_path_in_task).name}'
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
            df_stage2_tasks = pd.DataFrame(
                {
                    'data': [json.dumps({
                        'id': int(id),
                        'data': {
                            'image': bbox_image_url,
                            'src_bbox_image_path': str(src_bbox_image_path),
                            'src_bbox_data': src_bbox_data.asdict()
                        }
                    }) for id, src_bbox_data, bbox_image_url, src_bbox_image_path in zip(
                        tasks_ids, src_bboxes_data, bboxes_images_urls, bbox_src_image_paths
                    )]
                },
                index=[str(id) for id in tasks_ids]
            )
            return df_stage2_tasks

        inc_process(
            ds=DATA_STORE,
            input_dts=[self.dt_stage1_annotation_parsed],
            res_dt=self.dt_stage2_tasks,
            proc_func=proc_func
        )

    def parse_stage2_annotation(self):
        def proc_func(df_annotation: pd.DataFrame) -> pd.DataFrame:
            df_annotation['data'] = df_annotation['data'].apply(json.loads)
            df_annotation['data'] = df_annotation['data'].apply(parse_classification_completion)
            df_annotation['data'] = df_annotation['data'].apply(json.dumps)

            return df_annotation

        inc_process(
            ds=DATA_STORE,
            input_dts=[self.dt_stage2_annotation],
            res_dt=self.dt_stage2_annotation_parsed,
            proc_func=proc_func
        )

    def get_df_total_annotation(self):
        def proc_func(
            df_stage1_good_annotation: pd.DataFrame,
            df_stage2_parsed_annotation: pd.DataFrame
        ) -> pd.DataFrame:
            assert len(set(df_stage1_good_annotation.index) & set(df_stage2_parsed_annotation.index)) == 0
            df_annotation = pd.concat([df_stage1_good_annotation, df_stage2_parsed_annotation])
            return df_annotation

        inc_process(
            ds=DATA_STORE,
            input_dts=[self.dt_stage1_good_annotation, self.dt_stage2_annotation_parsed],
            res_dt=self.df_total_annotation,
            proc_func=proc_func
        )

    def get_total_annotation_json(self):
        df_total_annotation = self.df_total_annotation.get_data()
        bboxes_data: List[BboxData] = list(
            df_total_annotation['data'].apply(
                lambda imaga_data_json: BboxData.from_dict(json.loads(imaga_data_json))
            )
        )
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
        with open(self.total_annotation_path, 'w') as out:
            json.dump(annotation_json, out)
        logger.info(f"Annotation saved: {self.total_annotation_path}")

        return df_total_annotation

    def _callbacks(self) -> List[Callable[[], None]]:
        return [
            self.get_source_bboxes,
            self.add_tasks_to_stage1_project,
            self.parse_stage1_annotation,
            self.get_stage1_good_annotation,
            self.add_tasks_to_stage2_project,
            self.parse_stage2_annotation,
            self.get_df_total_annotation,
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
                Process(target=self.run_stage1),
                Process(target=self.run_stage2),
                Process(target=self.run_callbacks)
            ]
            for p in processes:
                p.start()
        finally:
            for p in processes:
                p.join()


if __name__ == "__main__":
    project = LabelStudioProject_TwoStageClassfication(
        images_dir='/mnt/c/Users/bobokvsky/YandexDisk-bobok100500@yandex.ru/Job/images/dataset_example_two_stage',
        src_annotation_path='/mnt/c/Users/bobokvsky/YandexDisk-bobok100500@yandex.ru/Job/images/dataset_example_two_stage/2020-12-10-annotations.json',
        bboxes_images_dir='/mnt/c/Users/bobokvsky/YandexDisk-bobok100500@yandex.ru/Job/images/dataset_example_two_stage/bboxes_two_stages',
        total_annotation_path='/mnt/c/Users/bobokvsky/YandexDisk-bobok100500@yandex.ru/Job/images/dataset_example_two_stage/ls_annotation_two_stages.json',
        project_path='/mnt/c/Users/bobokvsky/YandexDisk-bobok100500@yandex.ru/Job/temp/ls_project_test_two_stage',
        log_level='INFO'
    )
    project.run_project()
