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
    kwargs: str = None  # Additional LabelStudioMLBase model initialization kwargs in JSON format
    debug: bool = False  # Switch debug mode
    log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR'] = None  # Logging level
    model_dir: Union[str, Path] = None  # Directory where models are stored (relative to the project directory)
    check: bool = False  # Validate model instance before launching server
    subargs: List[str] = None
    force: bool = False

    def __post_init__(self):
        if self.project_name is not None:
            self.root_dir = str(Path(self.project_name).parent)
            self.project_name = Path(self.project_name).name
            self.project_path = Path(self.root_dir) / self.project_name
        if self.subargs is None:
            self.subargs = []
            for f in ['host', 'port', 'kwargs', 'debug', 'log-level', 'model-dir', 'check']:
                f_atr = f.replace('-', '_')
                if self.__getattribute__(f_atr) is not None:
                    if isinstance(self.__getattribute__(f_atr), bool):
                        self.subargs.append(f'--{f}')
                    else:
                        self.subargs.extend([f'--{f}', str(self.__getattribute__(f_atr))])

    @classmethod
    def parse_from_input_args(
        cls, input_args, subargs: List[str] = None
    ):
        self = cls()
        for f in fields(self):
            self.__setattr__(f.name, input_args.__getattribute__(f.name))
        self.__post_init__()

def run_app(
    label_studio_ml_config: LabelStudioMLConfig
):
    if label_studio_ml_config.command == 'init':
        create_dir(args=label_studio_ml_config)
    elif label_studio_ml_config.command == 'start':
        if not Path(label_studio_ml_config.project_path).exists():
            create_dir(args=label_studio_ml_config)
        start_server(args=label_studio_ml_config, subprocess_params=label_studio_ml_config.subargs)


def main():
    args, subargs = get_args()
    label_studio_config = LabelStudioMLConfig.parse_from_input_args(args, subargs)
    run_app(
        label_studio_config=label_studio_config,
        subargs=subargs
    )
