import json
import os
from pathlib import Path
from dataclasses import dataclass, fields
import sys
from typing import Literal, List
from django.core.management import call_command

from label_studio.core.argparser import parse_input_args
from label_studio.server import (
    _app_run, _apply_database_migrations, _get_config, _reset_password, _setup_env,
    logger as ls_logger, _get_free_port, _create_user
)
from label_studio.core.utils.io import find_file
from label_studio.core.utils.params import get_env


@dataclass
class LabelStudioConfig:
    command: Literal['start', 'init', 'version', 'reset_password', 'shell'] = None
    version: bool = False  # Show Label Studio version
    no_browser: bool = False  # Do not open browser at label studio start
    database: str = None  # Database file path for storing tasks and annotations
    data_dir: str = None  # Directory for storing all application related data
    debug: bool = None  # Debug mode for Flask
    config_path: str = find_file('default_config.json')  # Server config
    label_config: str = None  # Label config path

    ml_backends: List[str] = None  # Machine learning backends URLs
    sampling: Literal['sequential', 'uniform'] = 'sequential'  # Sampling type that defines tasks order
    log_level: Literal['DEBUG', 'INFO', 'WARNING', 'ERROR'] = 'INFO'  # Logging level
    internal_host: str = None  # Web server internal host, e.g.: "localhost" or "0.0.0.0"
    port: int = None  # Server port

    host: str = None  # 'Label Studio full hostname for generating imported task urls, sample task urls, static loading, etc.'
    # "Leave it empty to make all paths relative to the domain root, it's preferable for work for most cases."
    # 'Examples: "https://77.42.77.42:1234", "http://ls.domain.com/subdomain/"'

    cert_file: str = None  # Certificate file for HTTPS (in PEM format)
    key_file: str = None  # Private key file for HTTPS (in PEM format)
    project_desc: str = None  # Project description to identify project
    username: str = None  # Username for default user
    password: str = None  # Password for default user
    agree_fix_sqlite: bool = False  # Agree to fix SQLite issues on python 3.6-3.8 on Windows automatically

    base_data_dir: str = None

    def __post_init__(self):
        self.input_args = self

    @classmethod
    def parse_from_input_args(self, input_args):
        self = self()
        for f in fields(self):
            if f.name in ['base_data_dir']:
                continue
            self.__setattr__(f.name, input_args.__getattribute__(f.name))
        return self


def start_label_studio_app(
    label_studio_config: LabelStudioConfig,
):
    if label_studio_config.base_data_dir is not None:
        os.environ['LABEL_STUDIO_BASE_DATA_DIR'] = str(label_studio_config.base_data_dir)

    # setup logging level
    if label_studio_config.log_level:
        os.environ.setdefault("LOG_LEVEL", label_studio_config.log_level)

    if label_studio_config.database:
        database_path = Path(label_studio_config.database)
        os.environ.setdefault("DATABASE_NAME", str(database_path.absolute()))

    if label_studio_config.data_dir:
        data_dir_path = Path(label_studio_config.data_dir)
        os.environ.setdefault("LABEL_STUDIO_BASE_DATA_DIR", str(data_dir_path.absolute()))

    config = _get_config(label_studio_config.config_path)

    # set host name
    host = label_studio_config.host or config.get('host', '')
    if not get_env('HOST'):
        os.environ.setdefault('HOST', host)  # it will be passed to settings.HOSTNAME as env var

    _setup_env()
    _apply_database_migrations()

    from label_studio.core.utils.common import collect_versions
    versions = collect_versions()

    if label_studio_config.command == 'version':
        from label_studio import __version__
        print('\nLabel Studio version:', __version__, '\n')
        print(json.dumps(versions, indent=4))

    elif label_studio_config.command == 'reset_password':
        _reset_password(label_studio_config)
        return

    elif label_studio_config.command == 'shell':
        call_command('shell_plus')
        return

    elif label_studio_config.command == 'start' or label_studio_config.command is None:
        from label_studio.core.utils.common import start_browser

        _create_user(input_args=label_studio_config, config={})

        # ssl not supported from now
        cert_file = label_studio_config.cert_file or config.get('cert')
        key_file = label_studio_config.key_file or config.get('key')
        if cert_file or key_file:
            ls_logger.error(
                "Label Studio doesn't support SSL web server with cert and key.\n"
                'Use nginx or other servers for it.'
            )
            return

        # internal port and internal host for server start
        internal_host = label_studio_config.internal_host or config.get('internal_host', '0.0.0.0')
        internal_port = label_studio_config.port or get_env('PORT') or config.get('port', 8080)
        internal_port = int(internal_port)
        internal_port = _get_free_port(internal_port, label_studio_config.debug)

        # browser
        url = ('http://localhost:' + str(internal_port)) if not host else host
        start_browser(url, label_studio_config.no_browser)

        # Write
        _app_run(host=internal_host, port=internal_port)


def main():
    parsed_input_args = parse_input_args()
    label_studio_config = LabelStudioConfig.parse_from_input_args(parsed_input_args)
    start_label_studio_app(
        label_studio_config=label_studio_config,
    )


if __name__ == "__main__":
    sys.exit(main())
