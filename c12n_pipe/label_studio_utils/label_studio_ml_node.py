import logging
from pathlib import Path
from typing import Literal, Union
from multiprocessing import Process

from c12n_pipe.io.data_catalog import DataCatalog
from c12n_pipe.io.node import Node
from c12n_pipe.label_studio_utils.label_studio_ml_c12n import (
    LabelStudioMLConfig, run_app
)

logger = logging.getLogger(__name__)


class LabelStudioMLNode(Node):
    def __init__(
        self,
        project_path: Union[str, Path],
        script: Union[str, Path],
        port: int,
        log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR'] = 'INFO'
    ):
        self.process = None
        self.project_path = Path(project_path)
        self.label_studio_ml_config = LabelStudioMLConfig(
            command='start',
            project_name=str(self.project_path),
            script=str(script),
            port=port,
            log_level=log_level
        )

    def _run_app(self):
        run_app(label_studio_ml_config=self.label_studio_ml_config)

    def run_services(self, **kwargs):
        if self.process is None:
            logger.info('Start project...')
            self.process = Process(target=self._run_app)
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
        return []

    @property
    def outputs(self):
        return []

    @property
    def name(self):
        return f"{type(self).__name__}_{self.project_path.name}"
