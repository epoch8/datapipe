from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import text

from datapipe.store.database import DBConn


def test_dbconn_statements_are_traced_after_instrumentation(tmp_dir):
    """The datapipe CLI instruments SQLAlchemy after datapipe is imported;
    engines DBConn creates afterwards must still trace their statements."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    instrumentor = SQLAlchemyInstrumentor()
    instrumentor.instrument(tracer_provider=provider)
    try:
        dbconn = DBConn(f"sqlite:///{tmp_dir}/db.sqlite")
        with dbconn.con.begin() as con:
            con.execute(text("SELECT 1"))
    finally:
        instrumentor.uninstrument()

    statements = [s for s in exporter.get_finished_spans() if s.name != "connect"]
    assert any(s.name.startswith("SELECT") for s in statements), [s.name for s in exporter.get_finished_spans()]
