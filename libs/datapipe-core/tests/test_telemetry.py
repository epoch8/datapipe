import logging
import sys

import pytest
from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider

from datapipe.telemetry import (
    build_tracer_provider,
    context_from_env,
    otlp_export_requested,
    tracing_resource,
)

TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
SPAN_ID = "00f067aa0ba902b7"

_OTEL_ENV = [
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_SDK_DISABLED",
    "OTEL_SERVICE_NAME",
    "OTEL_RESOURCE_ATTRIBUTES",
    "TRACEPARENT",
    "TRACESTATE",
]


@pytest.fixture(autouse=True)
def clean_otel_env(monkeypatch):
    for var in _OTEL_ENV:
        monkeypatch.delenv(var, raising=False)


def _processor_exporters(provider: TracerProvider) -> list[str]:
    processors = provider._active_span_processor._span_processors  # type: ignore[attr-defined]
    return [type(p.span_exporter).__name__ for p in processors]


def test_otlp_off_by_default():
    assert not otlp_export_requested()
    assert _processor_exporters(build_tracer_provider()) == []


@pytest.mark.parametrize("var", ["OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"])
def test_otlp_on_with_endpoint(monkeypatch, var):
    pytest.importorskip("opentelemetry.exporter.otlp.proto.http.trace_exporter")
    monkeypatch.setenv(var, "http://localhost:4318")

    assert otlp_export_requested()
    assert _processor_exporters(build_tracer_provider()) == ["OTLPSpanExporter"]


def test_otlp_off_when_sdk_disabled(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")

    assert not otlp_export_requested()


def test_missing_otlp_exporter_warns_and_continues(monkeypatch, caplog):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    # A None entry in sys.modules makes the import raise ImportError.
    monkeypatch.setitem(sys.modules, "opentelemetry.exporter.otlp.proto.http.trace_exporter", None)

    with caplog.at_level(logging.WARNING, logger="datapipe.telemetry"):
        provider = build_tracer_provider()

    assert _processor_exporters(provider) == []
    assert "datapipe-core[otlp]" in caplog.text


def test_service_name_defaults_to_datapipe():
    assert tracing_resource().attributes[SERVICE_NAME] == "datapipe"


def test_service_name_from_env(monkeypatch):
    monkeypatch.setenv("OTEL_SERVICE_NAME", "my-pipeline")
    assert tracing_resource().attributes[SERVICE_NAME] == "my-pipeline"


def test_service_name_from_resource_attributes(monkeypatch):
    monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=from-attrs")
    assert tracing_resource().attributes[SERVICE_NAME] == "from-attrs"


def test_no_parent_without_traceparent():
    assert context_from_env() is None


def test_parent_from_traceparent(monkeypatch):
    monkeypatch.setenv("TRACEPARENT", f"00-{TRACE_ID}-{SPAN_ID}-01")

    ctx = context_from_env()
    assert ctx is not None

    tracer = TracerProvider().get_tracer(__name__)
    with tracer.start_as_current_span("child", context=ctx) as span:
        assert span.get_span_context().trace_id == int(TRACE_ID, 16)
        assert span.parent is not None
        assert span.parent.span_id == int(SPAN_ID, 16)


def test_malformed_traceparent_gives_fresh_root(monkeypatch):
    monkeypatch.setenv("TRACEPARENT", "not-a-traceparent")

    ctx = context_from_env()
    assert ctx is not None
    assert not trace.get_current_span(ctx).get_span_context().is_valid

    tracer = TracerProvider().get_tracer(__name__)
    with tracer.start_as_current_span("root", context=ctx) as span:
        assert span.parent is None
