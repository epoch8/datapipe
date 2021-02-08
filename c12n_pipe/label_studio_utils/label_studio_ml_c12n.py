from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Literal, List, Union

from label_studio.ml.server import create_dir, get_args, start_server


@dataclass
class LabelStudioMLConfig:
    command: Literal['start', 'init'] = None
    project_name: str = None  # Path to directory where project state will be initialized
    script: bool = False  # Machine learning script of the following format: /my/script/path:ModelClass
    host: str = None  # Server hostname for LS internal usage
    port: int = None  # Server port
    kwargs: dict = field(default_factory=dict)  # Additional LabelStudioMLBase model initialization kwargs
    debug_mode: bool = False  # Switch debug mode
    log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR'] = None  # Logging level
    model_dir: Union[str, Path] = None  # Directory where models are stored (relative to the project directory)
    check: bool = False  # Validate model instance before launching server

    @classmethod
    def parse_from_input_args(
        self, input_args, subargs: List[str] = None
    ):
        self = self()
        for f in fields(self):
            self.__setattr__(f.name, input_args.__getattribute__(f.name))
        if subargs is None:
            self.subargs = []
            for f in ['host', 'port', 'kwargs', 'debug_mode', 'log_level', 'model_dir', 'check']:
                if self.__getattribute__(f) is not None:
                    self.subargs.extend(f'--{f}', self.__getattribute__(f))
        else:
            self.subargs = subargs


def run_app(
    label_studio_ml_config: LabelStudioMLConfig
):
    if label_studio_ml_config.command == 'init':
        create_dir(args=label_studio_ml_config)
    elif label_studio_ml_config.command == 'start':
        if not Path(label_studio_ml_config.project_name).exists():
            create_dir(args=label_studio_ml_config)
        start_server(args=label_studio_ml_config, subprocess_params=label_studio_ml_config.subargs)


def main():
    args, subargs = get_args()
    label_studio_config = LabelStudioMLConfig.parse_from_input_args(args, subargs)
    run_app(
        label_studio_config=label_studio_config,
        subargs=subargs
    )
