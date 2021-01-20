import logging
from pathlib import Path
from dataclasses import dataclass, field, fields
from typing import Literal, List

from label_studio.project import Project
from label_studio.utils.argparser import parse_input_args
from label_studio.utils.functions import (
    set_external_hostname, set_web_protocol, get_web_protocol
)
from label_studio.server import (
    setup_default_logging_config, check_for_the_latest_version
)
from label_studio.blueprint import create_app


@dataclass
class LabelStudioConfig:
    command: Literal['start', 'init', 'version'] = None
    project_name: str = None  # Path to directory where project state has been initialized
    version: bool = False  # Show Label Studio version
    no_browser: bool = False  # Do not open browser at label studio start
    debug: bool = None  # Debug mode for Flask
    force: bool = False  # Force overwrite existing files
    root_dir: str = '.'  # Project root directory
    verbose: bool = False  # Increase output verbosity
    template: str = None  # Choose from predefined project templates
    config_path: str = None  # Server config
    label_config: str = None  # Label config path
    input_path: str = None  # Input path for task file or directory with tasks

    source: str = 'tasks-modified'  # Source data storage type
    source_path: str = 'tasks.json'  # Source bucket name
    source_params: dict = field(default_factory=dict)  # JSON string representing source parameters
    target: str = 'completions-dir-modified'  # Target data storage type
    target_path: str = 'completions'  # Target bucket name
    target_params: dict = field(default_factory=dict)  # JSON string representing target parameters

    input_format: Literal[  # Input tasks format.
        'json', 'json-dir', 'text',
        'text-dir', 'image-dir', 'audio-dir'
    ] = 'json'  # Unless you are using "json" or "json-dir" format, --label-config option is required

    output_dir: str = None  # Output directory for completions (unless cloud storage is used)
    ml_backends: List[str] = None  # Machine learning backends URLs
    sampling: Literal['sequential', 'uniform'] = 'sequential'  # Sampling type that defines tasks order
    log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR'] = None  # Logging level
    host: str = None  # Server hostname for LS internal usage
    protocol: Literal['http://', 'https://'] = None  # Web protocol http:// or https://
    port: int = None  # Server port
    cert_file: str = None  # Certificate file for HTTPS (in PEM format)
    key_file: str = None  # Private key file for HTTPS (in PEM format)
    allow_serving_local_files: bool = True  # Allow serving local files
    project_desc: str = None  # Project description to identify project

    def __post_init__(self):
        self.input_args = self

    @classmethod
    def parse_from_input_args(self, input_args):
        self = self()
        for f in fields(self):
            if f.name not in ['source', 'source_path', 'target', 'target_path']:
                self.__setattr__(f.name, input_args.__getattribute__(f.name))
        return self


def run_app(
    label_studio_config: LabelStudioConfig
):
    # setup logging level
    if label_studio_config.log_level:
        logging.root.setLevel(label_studio_config.log_level)

    project_path = Path(label_studio_config.project_name)
    if not (project_path / 'config.json').exists():
        Project.create_project_dir(label_studio_config.project_name, label_studio_config)

    app = create_app(label_studio_config)
    config = Project.get_config(label_studio_config.project_name, label_studio_config)
    # set host name
    host = label_studio_config.host or config.get('host', 'localhost')
    port = label_studio_config.port or config.get('port', 8080)
    server_host = 'localhost' if host == 'localhost' else '0.0.0.0'  # web server host
    # ssl certificate and key
    cert_file = label_studio_config.cert_file or config.get('cert')
    key_file = label_studio_config.key_file or config.get('key')
    ssl_context = None
    if cert_file and key_file:
        config['protocol'] = 'https://'
        ssl_context = (cert_file, key_file)

    # external hostname is used for data import paths, they must be absolute always,
    # otherwise machine learning backends couldn't access them
    set_web_protocol(label_studio_config.protocol or config.get('protocol', 'http://'))
    external_hostname = get_web_protocol() + host.replace('0.0.0.0', 'localhost')
    if host in ['0.0.0.0', 'localhost', '127.0.0.1']:
        external_hostname += ':' + str(port)
    set_external_hostname(external_hostname)

    app.run(host=server_host, port=port, debug=label_studio_config.debug, ssl_context=ssl_context)


def main():
    setup_default_logging_config()
    check_for_the_latest_version()

    # this will avoid looped imports and will register deprecated endpoints in the blueprint
    import label_studio.deprecated

    prased_input_args = parse_input_args()
    label_studio_config = LabelStudioConfig.parse_from_input_args(prased_input_args)
    run_app(
        label_studio_config=label_studio_config
    )
