"""OpenTelemetry setup for the `datapipe` CLI.

`setup_tracing()` runs once per process, before any subcommand. It installs the
global `TracerProvider`, attaches the exporters, instruments SQLAlchemy, and
continues a trace passed in the environment.

Configuration follows the standard OpenTelemetry environment variables:

- `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) turns
  on OTLP/HTTP export. Headers come from `OTEL_EXPORTER_OTLP_HEADERS`. Needs the
  `otlp` extra (`datapipe-core[otlp]`).
- `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES` set the service name;
  `datapipe` is the default.
- `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` pick the sampler.
- `OTEL_SDK_DISABLED=true` turns OTLP export off.
- `TRACEPARENT` / `TRACESTATE` make the process part of the caller's trace.

The `--trace-stdout`, `--trace-jaeger` and `--trace-gcp` CLI flags add their
exporters on top.
"""

import logging
import os

from opentelemetry import context as otel_context
from opentelemetry import propagate, trace
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

logger = logging.getLogger("datapipe.telemetry")

DEFAULT_SERVICE_NAME = "datapipe"

_OTLP_ENDPOINT_VARS = ("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")


def otlp_export_requested() -> bool:
    """OTLP export is on when a standard OTLP endpoint is set and the SDK is not disabled."""
    if os.environ.get("OTEL_SDK_DISABLED", "").strip().lower() == "true":
        return False
    return any(os.environ.get(var, "").strip() for var in _OTLP_ENDPOINT_VARS)


def tracing_resource() -> Resource:
    """Resource from the standard OTel env, with `datapipe` as the fallback service name.

    `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES` win when set; otherwise the SDK
    would report `unknown_service`.
    """
    resource = Resource.create()
    if str(resource.attributes.get(SERVICE_NAME, "")).startswith("unknown_service"):
        resource = resource.merge(Resource({SERVICE_NAME: DEFAULT_SERVICE_NAME}))
    return resource


def context_from_env() -> otel_context.Context | None:
    """Parent context passed by the caller in `TRACEPARENT` / `TRACESTATE`.

    Lets whatever starts a `datapipe` process (an orchestrator, a job runner) make
    the process part of its trace. A malformed value yields a context without a
    span, i.e. a fresh root.
    """
    traceparent = os.environ.get("TRACEPARENT", "").strip()
    if not traceparent:
        return None

    carrier = {"traceparent": traceparent}
    tracestate = os.environ.get("TRACESTATE", "").strip()
    if tracestate:
        carrier["tracestate"] = tracestate

    return propagate.extract(carrier)


def build_tracer_provider(
    *,
    trace_stdout: bool = False,
    trace_jaeger: bool = False,
    trace_jaeger_host: str = "localhost",
    trace_jaeger_port: int = 14268,
    trace_gcp: bool = False,
) -> TracerProvider:
    # Sampler comes from OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG.
    provider = TracerProvider(resource=tracing_resource())

    if otlp_export_requested():
        try:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        except ImportError:
            # Telemetry must never fail a pipeline run.
            logger.warning(
                "OTLP trace export requested but opentelemetry-exporter-otlp-proto-http is not installed "
                "(install datapipe-core[otlp]); traces are not exported"
            )
        else:
            # Endpoint and headers come from the standard OTEL_EXPORTER_OTLP_* env.
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))

    if trace_stdout:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    if trace_jaeger:
        from opentelemetry.exporter.jaeger.thrift import JaegerExporter  # type: ignore

        # create a JaegerExporter
        jaeger_exporter = JaegerExporter(
            # configure agent
            # agent_host_name='localhost',
            # agent_port=6831,
            # optional: configure also collector
            collector_endpoint=f"http://{trace_jaeger_host}:{trace_jaeger_port}/api/traces?format=jaeger.thrift",
            # username=xxxx, # optional
            # password=xxxx, # optional
            # max_tag_value_length=None # optional
        )

        provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))

    if trace_gcp:
        try:
            from opentelemetry.exporter.cloud_trace import (  # type: ignore
                CloudTraceSpanExporter,
            )
        except ImportError:
            raise ImportError("Please install opentelemetry-exporter-cloud-trace to use GCP Trace")

        cloud_trace_exporter = CloudTraceSpanExporter(
            resource_regex=r".*",
        )
        provider.add_span_processor(BatchSpanProcessor(cloud_trace_exporter))

    return provider


def setup_tracing(
    *,
    trace_stdout: bool = False,
    trace_jaeger: bool = False,
    trace_jaeger_host: str = "localhost",
    trace_jaeger_port: int = 14268,
    trace_gcp: bool = False,
) -> None:
    """Install the global tracer provider and instrumentation for this process."""
    trace.set_tracer_provider(
        build_tracer_provider(
            trace_stdout=trace_stdout,
            trace_jaeger=trace_jaeger,
            trace_jaeger_host=trace_jaeger_host,
            trace_jaeger_port=trace_jaeger_port,
            trace_gcp=trace_gcp,
        )
    )

    SQLAlchemyInstrumentor().instrument()

    parent_context = context_from_env()
    if parent_context is not None:
        # Attached for the life of the process: the CLI's `init` span and the
        # subcommand's spans become children of the caller's span.
        otel_context.attach(parent_context)
