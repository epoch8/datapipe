import click

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)

from datapipe.datatable import DataStore
from datapipe.compute import Catalog, Pipeline
from datapipe.run_config import RunConfig


def main(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    run_config: RunConfig = None,
) -> None:
    @click.group()
    @click.option('--debug', is_flag=True, help='Log debug output')
    @click.option('--debug-sql', is_flag=True, help='Log SQL queries VERY VERBOSE')
    @click.option('--trace-stdout', is_flag=True, help='Log traces to console')
    @click.option('--trace-jaeger', is_flag=True, help='Enable tracing to Jaeger')
    @click.option('--trace-jaeger-host', type=click.STRING, default='localhost', help='Jaeger host')
    @click.option('--trace-jaeger-port', type=click.INT, default=14268, help='Jaeger port')
    def cli(
        debug: bool,
        debug_sql: bool,
        trace_stdout: bool,
        trace_jaeger: bool,
        trace_jaeger_host: str,
        trace_jaeger_port: int
    ) -> None:
        import logging
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

        if debug_sql:
            logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

        if trace_stdout:
            provider = TracerProvider()
            processor = BatchSpanProcessor(ConsoleSpanExporter())
            provider.add_span_processor(processor)
            trace.set_tracer_provider(provider)

        if trace_jaeger:
            from opentelemetry import trace
            from opentelemetry.exporter.jaeger.thrift import JaegerExporter
            from opentelemetry.sdk.resources import SERVICE_NAME, Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            trace.set_tracer_provider(
                TracerProvider(
                    resource=Resource.create({SERVICE_NAME: "datapipe"})
                )
            )

            # create a JaegerExporter
            jaeger_exporter = JaegerExporter(
                # configure agent
                # agent_host_name='localhost',
                # agent_port=6831,
                # optional: configure also collector
                collector_endpoint=f'http://{trace_jaeger_host}:{trace_jaeger_port}/api/traces?format=jaeger.thrift',
                # username=xxxx, # optional
                # password=xxxx, # optional
                # max_tag_value_length=None # optional
            )

            # Create a BatchSpanProcessor and add the exporter to it
            span_processor = BatchSpanProcessor(jaeger_exporter)

            # add to the tracer
            trace.get_tracer_provider().add_span_processor(span_processor)  # type: ignore

    @cli.command()
    def run():
        from .compute import run_pipeline
        run_pipeline(ds, catalog, pipeline, run_config)

    @cli.command()
    def ui():
        from .compute import build_compute
        from .debug_ui import ui_main
        build_compute(ds, catalog, pipeline)
        app = ui_main(ds, catalog, pipeline)
        app.run_server(host='0.0.0.0')

    cli()
