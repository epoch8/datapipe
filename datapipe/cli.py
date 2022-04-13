from typing import List, Optional

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)

from datapipe.datatable import DataStore
from datapipe.compute import Catalog, Pipeline
from datapipe.run_config import RunConfig

import argparse
import simple_parsing  # With dataclass parsing (https://github.com/lebrice/SimpleParsing/)


def debug(
    traceback_with_variables: bool,
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


def run(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    run_config: Optional[RunConfig] = None
):
    from datapipe.compute import run_pipeline
    run_pipeline(ds, catalog, pipeline, run_config)


def ui(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    run_config: Optional[RunConfig] = None
):
    from datapipe.compute import build_compute
    from datapipe.debug_ui import ui_main
    build_compute(ds, catalog, pipeline)
    app = ui_main(ds, catalog, pipeline)
    app.run_server(host='0.0.0.0')


def get_argument_parser() -> simple_parsing.ArgumentParser:
    parser = simple_parsing.ArgumentParser()
    group = parser.add_argument_group('debug')
    group.add_argument('--debug', action='store_true', help='Log debug output')
    group.add_argument('--debug-sql', action='store_true', help='Log SQL queries VERY VERBOSE')
    group.add_argument('--trace-stdout', action='store_true', help='Log traces to console')
    group.add_argument('--trace-jaeger', action='store_true', help='Enable tracing to Jaeger')
    group.add_argument('--trace-jaeger-host', type=str, default='localhost', help='Jaeger host')
    group.add_argument('--trace-jaeger-port', type=int, default=14268, help='Jaeger port')
    subparsers = parser.add_subparsers(dest='command', required=True)
    subparsers.add_parser('run')
    subparsers.add_parser('ui')
    return parser


def run_main(
    arguments: argparse.Namespace,
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    run_config: Optional[RunConfig] = None
):
    debug(
        traceback_with_variables=arguments.traceback_with_variables,
        debug=arguments.debug,
        debug_sql=arguments.debug_sql,
        trace_stdout=arguments.trace_stdout,
        trace_jaeger=arguments.trace_jaeger,
        trace_jaeger_host=arguments.trace_jaeger_host,
        trace_jaeger_port=arguments.trace_jaeger_port
    )
    if arguments.command == 'run':
        run(ds, catalog, pipeline, run_config)
    if arguments.command == 'ui':
        ui(ds, catalog, pipeline, run_config)


def main(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    run_config: Optional[RunConfig] = None
) -> None:
    parser = get_argument_parser()
    arguments = parser.parse_args()
    run_main(arguments, ds, catalog, pipeline, run_config)
