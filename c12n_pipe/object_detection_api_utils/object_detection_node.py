import logging
import shutil
import tarfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List, Union

from multiprocessing import Process, Event

import pandas as pd

import requests
import tensorflow as tf
from tqdm import tqdm

from object_detection import model_lib_v2

from cv_pipeliner.core.data import ImageData
from cv_pipeliner.inference_models.detection.object_detection_api import ObjectDetectionAPI_ModelSpec
from cv_pipeliner.utils.object_detection_api import convert_to_tf_records, set_config
from cv_pipeliner.reporters.detection import DetectionReporter

from c12n_pipe.io.node import Node
from c12n_pipe.io.data_catalog import DataCatalog


logger = logging.getLogger(__name__)


def download_and_extract_tar_gz_to_directory(
    url: str,
    directory: Union[str, Path]
):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    filepath = directory / 'tempfile.tar.gz'

    logger.info(f"Downloading tar.gz archive from {url} ...")
    resp = requests.get(url, stream=True)
    total = int(resp.headers.get('content-length', 0))
    with open(filepath, 'wb') as file, tqdm(
            desc=str(filepath),
            total=total,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
    ) as tbar:
        for data_chunk in resp.iter_content(chunk_size=1024):
            size = file.write(data_chunk)
            tbar.update(size)

    logger.info(f"Extracting all to {directory}...")
    tar = tarfile.open(filepath)
    tar.extractall(path=directory)
    tar.close()

    filepath.unlink()
    logger.info(f"Saved to {directory}.")


class ObjectDetectionTrainNode(Node):
    def __init__(
        self,
        dt_train_images_data: str,
        dt_output_models: str,

        class_names: List[str],
        output_model_directory: Union[str, Path],
        start_train_every_n: int,
        zoo_model_url: str,
        train_batch_size: int,
        train_num_steps: int,
        checkpoint_every_n: int,

        dt_test_images_data: str = None,
        score_threshold: float = 0.5
    ):
        self.dt_train_images_data = dt_train_images_data
        self.dt_test_images_data = dt_test_images_data
        self.dt_output_models = dt_output_models
        self.class_names = class_names
        self.output_model_directory = Path(output_model_directory)
        self.zoo_model_url = zoo_model_url
        self.train_batch_size = train_batch_size
        self.start_train_every_n = start_train_every_n
        self.checkpoint_every_n = checkpoint_every_n
        self.train_num_steps = train_num_steps
        self.score_threshold = 0.3

        self.current_count = 0
        self.train_process = None
        self.training_process_event: Event = None

    def _get_images_data(
        self,
        data_catalog: DataCatalog,
        chunksize: int,
        dt_input_images_data: str
    ):
        dt_input_images_data = data_catalog.get_data_table(dt_input_images_data)
        images_data = []
        for df_input_images_data in dt_input_images_data.get_data_chunked(chunksize=chunksize):
            images_data.extend(list(
                df_input_images_data['data'].apply(
                    lambda imaga_data_json: ImageData.from_dict(imaga_data_json)
                )
            ))
        return images_data

    def prepare_data(
        self,
        train_images_data: List[ImageData],
        num_workers: int = 1,
        num_shards: int = 1,
        max_pictures_per_worker: int = 1000
    ) -> Path:
        self.output_model_directory.mkdir(parents=True, exist_ok=True)
        timestamp_directory = self.output_model_directory / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        download_and_extract_tar_gz_to_directory(
            url=self.zoo_model_url,
            directory=timestamp_directory
        )
        new_model_directory = next(timestamp_directory.glob('*'))  # TODO: FIX THIS CRUTCH
        shutil.move(new_model_directory / 'checkpoint', new_model_directory / 'fine_tune_checkpoint')
        (new_model_directory / 'checkpoint').mkdir()
        (new_model_directory / 'input_data').mkdir()
        tf_records_train_path = new_model_directory / 'input_data' / 'train_dataset.record'
        label_map = defaultdict(int)
        for id, class_name in enumerate(self.class_names):
            label_map[class_name] = id + 1
        convert_to_tf_records(
            images_data=train_images_data,
            label_map=label_map,
            filepath=tf_records_train_path,
            num_workers=num_workers,
            num_shards=num_shards,
            max_pictures_per_worker=max_pictures_per_worker
        )
        set_config(
            config_path=new_model_directory / 'pipeline.config',
            checkpoint_path=new_model_directory / 'fine_tune_checkpoint' / 'ckpt-0',
            tf_records_train_path=Path(f"{tf_records_train_path}-?????-of-{str(num_shards).zfill(5)}"),
            label_map=label_map,
            label_map_filepath=new_model_directory / 'input_data' / 'label_map.txt',
            batch_size=self.train_batch_size,
            augment_path=None
        )
        return new_model_directory

    def train_loop(
        self,
        new_model_directory: Path
    ) -> Path:
        strategy = tf.compat.v2.distribute.MirroredStrategy()
        with strategy.scope():
            model_lib_v2.train_loop(
                pipeline_config_path=str(new_model_directory / 'pipeline.config'),
                model_dir=str(new_model_directory / 'checkpoint'),
                train_steps=self.train_num_steps,
                checkpoint_every_n=self.checkpoint_every_n,
                checkpoint_max_to_keep=1000,
            )
        # Get last checkpoint
        checkpoint_path = max(
            (new_model_directory / 'checkpoint').glob('ckpt-*.index'),
            key=lambda ckpt_path: int(ckpt_path.stem.split('-')[1])
        )
        return checkpoint_path

    def write_report(
        self,
        new_model_directory: Path,
        checkpoint_path: Path,
        train_images_data: List[ImageData],
        test_images_data: List[ImageData],
    ):
        logger.info('Inference train/test data & writing report')
        detection_model_spec = ObjectDetectionAPI_ModelSpec(
            config_path=new_model_directory/'pipeline.config',
            checkpoint_path=checkpoint_path
        )
        detection_reporter = DetectionReporter()
        detection_reporter.report(
            models_specs=[detection_model_spec],
            tags=[new_model_directory.parent.name],
            scores_thresholds=[self.score_threshold],  # TODO: find best by test images data
            compare_tag=new_model_directory.parent.name,
            output_directory=new_model_directory / 'report_train',
            true_images_data=train_images_data,
            minimum_iou=self.score_threshold
        )
        if test_images_data is not None and len(test_images_data) > 0:
            detection_reporter.report(
                models_specs=[detection_model_spec],
                tags=[new_model_directory.parent.name],
                scores_thresholds=[self.score_threshold],  # TODO: find best by test images data
                compare_tag=new_model_directory.parent.name,
                output_directory=new_model_directory / 'report_test',
                true_images_data=test_images_data,
                minimum_iou=self.score_threshold,
            )

    def write_detection_model_to_output(
        self,
        data_catalog: DataCatalog,
        new_model_directory: Path,
        checkpoint_path: Path
    ):
        df_output_model = pd.DataFrame({
            'config_path': [str(new_model_directory / 'pipeline.config')],
            'checkpoint_path': [str(checkpoint_path)],
            'score_threshold': [self.score_threshold]  # TODO: find best by test images data
        }, index=[new_model_directory.name])
        dt_output_models = data_catalog.get_data_table(self.dt_output_models)
        chunk = dt_output_models.store_chunk(df_output_model)
        dt_output_models.sync_meta(chunks=[chunk], processed_idx=new_model_directory.name)

    def main_train_process(
        self,
        data_catalog: DataCatalog,
        chunksize: int,
        training_process_event: Event
    ):
        try:
            train_images_data = self._get_images_data(
                data_catalog=data_catalog,
                chunksize=chunksize,
                dt_input_images_data=self.dt_train_images_data
            )
            test_images_data = self._get_images_data(
                data_catalog=data_catalog,
                chunksize=chunksize,
                dt_input_images_data=self.dt_test_images_data
            ) if self.dt_test_images_data is not None else None
            new_model_directory = self.prepare_data(train_images_data=train_images_data)
            checkpoint_path = self.train_loop(new_model_directory=new_model_directory)
#             checkpoint_path = new_model_directory / 'fine_tune_checkpoint' / 'ckpt-0'
            self.write_report(
                new_model_directory=new_model_directory,
                checkpoint_path=checkpoint_path,
                train_images_data=train_images_data,
                test_images_data=test_images_data
            )
            self.write_detection_model_to_output(
                data_catalog=data_catalog,
                new_model_directory=new_model_directory,
                checkpoint_path=checkpoint_path
            )
        finally:
            logger.info("Training has been ended! (Child process)")
            training_process_event.set()

    def start_train_process(
        self,
        data_catalog: DataCatalog,
        chunksize: int,
        training_process_event: Event
    ):
        if self.train_process is None:
            self.train_process = Process(
                target=self.main_train_process,
                args=(data_catalog, chunksize, training_process_event)
            )
            self.train_process.start()
            
    def join_train_process(self, terminate: bool = False):
        if self.train_process is not None:
            if terminate:
                self.train_process.terminate()
            self.train_process.join()
            self.train_process = None

    def run(
        self,
        data_catalog: DataCatalog,
        object_detection_node_count_value: int,
        chunksize: int = 1000,
        **kwargs
    ):
        if self.training_process_event is not None and not self.training_process_event.is_set():
            logging.info("Training is still running...")
            return

        if self.train_process is not None:
            self.join_train_process()
            self.training_process_event = None
            logger.info("Training ended!")
            self.current_count += self.start_train_every_n
            return

        if object_detection_node_count_value >= self.current_count + self.start_train_every_n:
            logger.info("Train event start!")
            self.training_process_event = Event()
            self.start_train_process(
                data_catalog=data_catalog,
                chunksize=chunksize,
                training_process_event=self.training_process_event,
                **kwargs
            )
        else:
            logger.info(f"Train: waiting until {object_detection_node_count_value=}>={(self.current_count + self.start_train_every_n)=}")

    def __del__(self):
        self.join_train_process(terminate=True)

    @property
    def inputs(self):
        return [self.dt_train_images_data, self.dt_test_images_data]

    @property
    def outputs(self):
        return [self.dt_output_models]

    @property
    def name(self):
        return f"{type(self).__name__}"
