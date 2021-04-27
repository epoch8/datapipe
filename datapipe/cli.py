import click

from datapipe.metastore import MetaStore
from datapipe.dsl import Catalog, Pipeline


def main(ms: MetaStore, catalog: Catalog, pipeline: Pipeline):
    import logging
    logging.basicConfig(level=logging.INFO)

    @click.group()
    def cli():
        pass

    @cli.command()
    def run():
        from .compute import run_pipeline
        run_pipeline(ms, catalog, pipeline)

    @cli.command()
    def ui():
        from .debug_ui import ui_main
        app = ui_main(ms, catalog, pipeline)
        app.run_server(host='0.0.0.0')

    cli()

# def run_label_studio():
#     import logging
#     logging.basicConfig(level=logging.INFO)

#     @click.group()
#     def cli():
#         pass

#     @cli.command()
#     @click.option('--no_browser', default=False, help='Do not open browser at label studio start')
#     @click.option('--database', default=None, help='Do not open browser at label studio start')
#     @click.option('--internal_host', default='0.0.0.0', help='Web server internal host, e.g.: "localhost" or "0.0.0.0"')
#     @click.option('--port', default='8080', help='Server port')
#     @click.option('--username', default='admin@epoch8.co', help='Username for default user')
#     @click.option('--password', default='qwerty', help='Password for default user')
#     def run(
#         no_browser: bool,
#         database: str,
#         internal_host: str,
#         port: str,
#         username: str,
#         password: str
#     ):
#         from datapipe.label_studio.server import LabelStudioConfig, start_label_studio_app
#         start_label_studio_app(
#             label_studio_config=LabelStudioConfig(
#                 no_browser=no_browser,
#                 database=database,
#                 internal_host=internal_host,
#                 port=port,
#                 username=username,
#                 password=password
#             )
#         )

#     cli()
