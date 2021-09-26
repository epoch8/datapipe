import click

from datapipe.datatable import DataStore
from datapipe.dsl import Catalog, Pipeline


def main(ds: DataStore, catalog: Catalog, pipeline: Pipeline):
    import logging
    logging.basicConfig(level=logging.INFO)

    @click.group()
    def cli():
        pass

    @cli.command()
    def run():
        from .compute import run_pipeline
        run_pipeline(ds, catalog, pipeline)

    @cli.command()
    def ui():
        from .debug_ui import ui_main
        app = ui_main(ds, catalog, pipeline)
        app.run_server(host='0.0.0.0')

    cli()
