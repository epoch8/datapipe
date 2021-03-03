import logging
from pathlib import Path
from typing import List, Literal, Union
from multiprocessing import Process

from label_studio.project import Project
from c12n_pipe.io.data_catalog import DataCatalog
from c12n_pipe.io.node import Node
from c12n_pipe.label_studio_utils.label_studio_c12n import LabelStudioConfig, run_app

logger = logging.getLogger(__name__)


class LabelStudioNode(Node):
    def __init__(
        self,
        project_path: Union[str, Path],
        input: str,
        output: str,
        port: int,
        label_config: str = None,
        log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR'] = 'INFO',
        ml_backends: List[str] = None
    ):
        self.process = None
        self.project_path = Path(project_path)
        self.input = input
        self.output = output
        self.label_studio_config = LabelStudioConfig(
            project_name=str(self.project_path),
            source='tasks-json-modified',
            source_path='tasks.json',
            source_params={
                'data_table_name': input,
            },
            target='completions-dir-modified',
            target_path='completions',
            target_params={
                'data_table_name': output,
            },
            port=port,
            label_config=label_config,
            log_level=log_level,
            ml_backends=ml_backends
        )

    def change_config(self, data_catalog: DataCatalog):
        self.label_studio_config.source_params['connstr'] = data_catalog.connstr
        self.label_studio_config.source_params['schema'] = data_catalog.schema
        self.label_studio_config.target_params['connstr'] = data_catalog.connstr
        self.label_studio_config.target_params['schema'] = data_catalog.schema

    def process_data(self, data_catalog):
        self.change_config(data_catalog)
        Project._storage = {}  # Clear Project memory
        Project.get_or_create(
            self.label_studio_config.project_name,
            self.label_studio_config,
            context={'multi_session': False}
        )

    def _run_app(self, data_catalog: DataCatalog):
        self.process_data(data_catalog)
        run_app(label_studio_config=self.label_studio_config)

    def run_services(self, data_catalog: DataCatalog, **kwargs):
        if self.process is None:
            logger.info('Start project...')
            self.process = Process(target=self._run_app, args=(data_catalog, ))
            self.process.start()

    def terminate_services(self, **kwargs):
        if self.process is not None:
            self.process.terminate()
            self.process.join()
            self.process = None

    def __del__(self):
        self.terminate_services()

    @property
    def inputs(self):
        return [self.input]

    @property
    def outputs(self):
        return [self.output]

    @property
    def name(self):
        return f"{type(self).__name__}_{self.project_path.name}"
