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
    def ui_pipeline_overview():
        from .debug_ui import ui_pipeline_overview
        app = ui_pipeline_overview(ms, catalog, pipeline)
        app.run_server(host='0.0.0.0')

    @cli.command()
    def ui_table_view():
        from .debug_ui import ui_table_view
        app = ui_table_view(ms, catalog, pipeline)
        app.run_server(host='0.0.0.0')

    cli()