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
    def debug_ui():
        from .debug_ui import main
        main(ms, catalog, pipeline)

    cli()