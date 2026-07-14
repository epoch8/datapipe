from __future__ import annotations

from datetime import datetime

from typing import cast

from sqlalchemy import DateTime, String, Table, Text
from sqlalchemy.orm import Mapped, mapped_column

from datapipe_app.db_schema import ObservabilityBase
from datapipe_app.observability.db import utc_now
from datapipe_app_ml_ops.observability.tables import MLObservabilityTableConfig


class PipelineMetricsCandidateRow(ObservabilityBase):
    __tablename__ = "metrics_candidates"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    pipeline_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    model_id: Mapped[str] = mapped_column(String(512), nullable=False)
    model_source: Mapped[str] = mapped_column(String(64), nullable=False, default="manual")
    artifact_uri: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    dataset_id: Mapped[str] = mapped_column(String(512), nullable=False)
    subset: Mapped[str] = mapped_column(String(64), nullable=False, default="val")
    task_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    metrics_state: Mapped[str] = mapped_column(String(32), nullable=False, default="not_computed")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


def apply_ml_table_config(*, tables: MLObservabilityTableConfig, schema: str | None) -> None:
    table = cast(Table, PipelineMetricsCandidateRow.__table__)
    table.name = tables.metrics_candidates
    table.schema = schema


def list_metrics_candidates(store, pipeline_id: str) -> list[PipelineMetricsCandidateRow]:
    from sqlalchemy import select

    with store.session() as session:
        return list(
            session.scalars(
                select(PipelineMetricsCandidateRow)
                .where(PipelineMetricsCandidateRow.pipeline_id == pipeline_id)
                .order_by(PipelineMetricsCandidateRow.created_at.desc())
            ).all()
        )


def add_metrics_candidate(store, row: PipelineMetricsCandidateRow) -> PipelineMetricsCandidateRow:
    with store.session() as session:
        session.add(row)
        session.commit()
        session.refresh(row)
        return row


def delete_metrics_candidate(store, pipeline_id: str, candidate_id: str) -> bool:
    with store.session() as session:
        row = session.get(PipelineMetricsCandidateRow, candidate_id)
        if row is None or row.pipeline_id != pipeline_id:
            return False
        session.delete(row)
        session.commit()
        return True


__all__ = [
    "PipelineMetricsCandidateRow",
    "add_metrics_candidate",
    "apply_ml_table_config",
    "delete_metrics_candidate",
    "list_metrics_candidates",
    "utc_now",
]
