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
