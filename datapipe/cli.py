import click

from datapipe.datatable import DataStore
from datapipe.dsl import Catalog, Pipeline


def main(ds: DataStore, catalog: Catalog, pipeline: Pipeline):
    @click.group()
    @click.option('--debug', is_flag=True, help='Log debug output')
    def cli(debug):
        import logging
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

    @cli.command()
    def run():
        from .compute import run_pipeline
        run_pipeline(ds, catalog, pipeline)

    @cli.command()
    def ui():
        from .compute import build_compute
        from .debug_ui import ui_main
        build_compute(ds, catalog, pipeline)
        app = ui_main(ds, catalog, pipeline)
        app.run_server(host='0.0.0.0')

    cli()
