from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional, cast

from sqlalchemy import (
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    select,
    text,
    update,
)
from sqlalchemy.engine import CursorResult, Engine
from sqlalchemy.orm import Mapped, Session, mapped_column, sessionmaker

from datapipe_app.db_schema import ObservabilityBase
from datapipe_app.db_schema import apply_observability_table_config
from datapipe_app.observability.run_triggers import (
    pipeline_trigger_values,
    stage_from_trigger,
    stage_trigger_values,
)
from datapipe_app.observability.tables import ObservabilityTableConfig
from datapipe_app_ml_ops.observability.tables import MLObservabilityTableConfig

logger = logging.getLogger(__name__)

Base = ObservabilityBase


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PipelineRegistryRow(Base):
    __tablename__ = "pipeline_registry"

    pipeline_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(512), nullable=False)
    task_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    registered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    agent_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)


class PipelineRunRow(Base):
    __tablename__ = "pipeline_runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    pipeline_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    trigger: Mapped[str | None] = mapped_column(String(64), nullable=True)
    labels_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class PipelineRunStepRow(Base):
    __tablename__ = "pipeline_run_steps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    step_name: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    processed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class PipelineRunLogRow(Base):
    __tablename__ = "pipeline_run_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    logged_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    level: Mapped[str] = mapped_column(String(16), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (UniqueConstraint("run_id", "seq", name="uq_run_log_seq"),)


class PipelineScheduleRow(Base):
    __tablename__ = "pipeline_schedules"

    pipeline_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    cron_expression: Mapped[str | None] = mapped_column(String(128), nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    timezone: Mapped[str | None] = mapped_column(String(64), nullable=True)


class ObservabilityStore:
    def __init__(
        self,
        engine: Engine,
        *,
        schema: Optional[str] = None,
        tables: Optional[ObservabilityTableConfig] = None,
        ml_tables: Optional[MLObservabilityTableConfig] = None,
    ):
        self.engine = engine
        self.schema = schema
        self.tables = tables or ObservabilityTableConfig()
        self.ml_tables = ml_tables or MLObservabilityTableConfig()
        self._Session = sessionmaker(bind=engine, expire_on_commit=False)
        apply_observability_table_config(self.tables, self.schema, ml_tables=self.ml_tables)

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        schema: Optional[str] = None,
        tables: Optional[ObservabilityTableConfig] = None,
    ) -> "ObservabilityStore":
        engine = create_engine(url, future=True)
        if schema and not url.startswith("sqlite"):
            from datapipe.store.database import DBConn, ensure_db_schema

            ensure_db_schema(DBConn(url, schema))
        store = cls(engine, schema=schema, tables=tables)
        store.create_all()
        return store

    def create_all(self) -> None:
        Base.metadata.create_all(self.engine)
        from datapipe_app_ml_ops.observability.analytics_views import ensure_analytics_tables

        ensure_analytics_tables(self.engine, tables=self.ml_tables, schema=self.schema)

    def session(self) -> Session:
        return self._Session()

    def register_pipeline(
        self,
        pipeline_id: str,
        *,
        display_name: Optional[str] = None,
        task_type: Optional[str] = None,
        agent_url: Optional[str] = None,
    ) -> None:
        now = utc_now()
        with self.session() as session:
            row = session.get(PipelineRegistryRow, pipeline_id)
            if row is None:
                row = PipelineRegistryRow(
                    pipeline_id=pipeline_id,
                    display_name=display_name or pipeline_id,
                    task_type=task_type,
                    registered_at=now,
                    last_heartbeat_at=now,
                    agent_url=agent_url,
                )
                session.add(row)
            else:
                row.display_name = display_name or row.display_name
                row.task_type = task_type or row.task_type
                row.last_heartbeat_at = now
                if agent_url:
                    row.agent_url = agent_url
            session.commit()

    def list_pipelines(self) -> list[PipelineRegistryRow]:
        with self.session() as session:
            return list(session.scalars(select(PipelineRegistryRow)).all())

    def get_pipeline(self, pipeline_id: str) -> Optional[PipelineRegistryRow]:
        with self.session() as session:
            return session.get(PipelineRegistryRow, pipeline_id)

    def create_run(
        self,
        pipeline_id: str,
        *,
        trigger: Optional[str] = None,
        labels_json: Optional[str] = None,
    ) -> str:
        run_id = str(uuid.uuid4())
        now = utc_now()
        with self.session() as session:
            session.add(
                PipelineRunRow(
                    run_id=run_id,
                    pipeline_id=pipeline_id,
                    status="running",
                    started_at=now,
                    trigger=trigger,
                    labels_json=labels_json,
                )
            )
            session.commit()
        return run_id

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        error: Optional[str] = None,
    ) -> None:
        now = utc_now()
        with self.session() as session:
            cursor_result = cast(
                CursorResult[Any],
                session.execute(
                    update(PipelineRunRow)
                    .where(PipelineRunRow.run_id == run_id)
                    .values(status=status, finished_at=now, error=error)
                ),
            )
            if cursor_result.rowcount == 0:
                logger.warning(
                    "finish_run: pipeline_runs row not found for run_id=%s (schema=%s)",
                    run_id,
                    self.schema,
                )
            session.commit()

    def upsert_run_step(
        self,
        run_id: str,
        step_name: str,
        *,
        status: str,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        processed: Optional[int] = None,
        total: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        with self.session() as session:
            existing = session.scalars(
                select(PipelineRunStepRow).where(
                    PipelineRunStepRow.run_id == run_id,
                    PipelineRunStepRow.step_name == step_name,
                )
            ).first()
            if existing is None:
                session.add(
                    PipelineRunStepRow(
                        run_id=run_id,
                        step_name=step_name,
                        status=status,
                        started_at=started_at or utc_now(),
                        finished_at=finished_at,
                        processed=processed,
                        total=total,
                        error=error,
                    )
                )
            else:
                existing.status = status
                if started_at:
                    existing.started_at = started_at
                if finished_at:
                    existing.finished_at = finished_at
                if processed is not None:
                    existing.processed = processed
                if total is not None:
                    existing.total = total
                if error is not None:
                    existing.error = error
            session.commit()

    def get_run(self, run_id: str) -> Optional[PipelineRunRow]:
        with self.session() as session:
            return session.get(PipelineRunRow, run_id)

    def get_run_steps(self, run_id: str) -> list[PipelineRunStepRow]:
        with self.session() as session:
            return list(
                session.scalars(
                    select(PipelineRunStepRow)
                    .where(PipelineRunStepRow.run_id == run_id)
                    .order_by(PipelineRunStepRow.id)
                ).all()
            )

    def list_running_runs(self, pipeline_id: str) -> list[PipelineRunRow]:
        with self.session() as session:
            return list(
                session.scalars(
                    select(PipelineRunRow)
                    .where(
                        PipelineRunRow.pipeline_id == pipeline_id,
                        PipelineRunRow.status == "running",
                    )
                    .order_by(PipelineRunRow.started_at)
                ).all()
            )

    def finish_running_steps(
        self,
        run_id: str,
        *,
        status: str,
        error: Optional[str] = None,
    ) -> int:
        now = utc_now()
        with self.session() as session:
            cursor_result = cast(
                CursorResult[Any],
                session.execute(
                    update(PipelineRunStepRow)
                    .where(
                        PipelineRunStepRow.run_id == run_id,
                        PipelineRunStepRow.status == "running",
                    )
                    .values(status=status, finished_at=now, error=error)
                ),
            )
            session.commit()
            return cursor_result.rowcount or 0

    def append_run_log_line(self, run_id: str, level: str, message: str) -> None:
        seq = self.get_last_log_seq(run_id) + 1
        self.append_run_logs(
            [
                {
                    "run_id": run_id,
                    "seq": seq,
                    "logged_at": utc_now(),
                    "level": level,
                    "message": message,
                }
            ]
        )

    def append_run_logs(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self.session() as session:
            for row in rows:
                session.add(PipelineRunLogRow(**row))
            session.commit()

    def get_run_logs(
        self,
        run_id: str,
        *,
        after: int = 0,
        limit: int = 500,
    ) -> list[PipelineRunLogRow]:
        with self.session() as session:
            return list(
                session.scalars(
                    select(PipelineRunLogRow)
                    .where(
                        PipelineRunLogRow.run_id == run_id,
                        PipelineRunLogRow.seq > after,
                    )
                    .order_by(PipelineRunLogRow.seq)
                    .limit(limit)
                ).all()
            )

    def get_last_log_seq(self, run_id: str) -> int:
        with self.session() as session:
            row = session.scalars(
                select(PipelineRunLogRow.seq)
                .where(PipelineRunLogRow.run_id == run_id)
                .order_by(PipelineRunLogRow.seq.desc())
                .limit(1)
            ).first()
            return row or 0

    def get_last_run(self, pipeline_id: str) -> Optional[PipelineRunRow]:
        with self.session() as session:
            return session.scalars(
                select(PipelineRunRow)
                .where(PipelineRunRow.pipeline_id == pipeline_id)
                .order_by(PipelineRunRow.started_at.desc())
                .limit(1)
            ).first()

    def list_runs(
        self,
        *,
        pipeline_id: Optional[str] = None,
        status: Optional[str] = None,
        stage: Optional[str] = None,
        trigger: Optional[str] = None,
        search: Optional[str] = None,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        sort_by: str = "started_at",
        sort_dir: str = "desc",
        limit: int = 25,
        offset: int = 0,
    ) -> tuple[list[PipelineRunRow], int, dict[str, list[str]], dict[str, int]]:
        with self.session() as session:
            query = select(PipelineRunRow)
            if pipeline_id:
                query = query.where(PipelineRunRow.pipeline_id == pipeline_id)
            if status:
                query = query.where(PipelineRunRow.status == status)
            if trigger:
                query = query.where(PipelineRunRow.trigger == trigger)
            if stage:
                if stage == "all labels":
                    query = query.where(PipelineRunRow.trigger.in_(pipeline_trigger_values()))
                else:
                    query = query.where(PipelineRunRow.trigger.in_(stage_trigger_values(stage)))
            if search:
                query = query.where(PipelineRunRow.run_id.contains(search))
            if from_dt:
                query = query.where(PipelineRunRow.started_at >= from_dt)
            if to_dt:
                query = query.where(PipelineRunRow.started_at <= to_dt)

            rows = list(session.scalars(query).all())

            filter_query = select(PipelineRunRow)
            if pipeline_id:
                filter_query = filter_query.where(PipelineRunRow.pipeline_id == pipeline_id)
            all_for_filters = list(session.scalars(filter_query).all())

        statuses = sorted({r.status for r in all_for_filters})
        triggers = sorted({trigger for r in all_for_filters if (trigger := r.trigger)})
        stages: set[str] = set()
        for row in all_for_filters:
            stage = stage_from_trigger(row.trigger)
            if stage:
                stages.add(stage)
            elif row.trigger in pipeline_trigger_values():
                stages.add("all labels")

        counts_by_status: dict[str, int] = {}
        for row in all_for_filters:
            counts_by_status[row.status] = counts_by_status.get(row.status, 0) + 1

        reverse = sort_dir != "asc"

        def _duration(row: PipelineRunRow) -> float:
            if row.started_at and row.finished_at:
                return (row.finished_at - row.started_at).total_seconds()
            return -1.0

        if sort_by == "duration":
            rows.sort(key=_duration, reverse=reverse)
        elif sort_by == "status":
            rows.sort(key=lambda row: row.status, reverse=reverse)
        elif sort_by == "stage":
            rows.sort(key=lambda row: row.trigger or "", reverse=reverse)
        else:
            rows.sort(key=lambda row: row.started_at, reverse=reverse)

        total = len(rows)
        page = rows[offset : offset + limit]
        filters = {
            "statuses": statuses,
            "stages": sorted(stages),
            "triggers": triggers,
        }
        return page, total, filters, counts_by_status

    def list_recent_runs(self, pipeline_id: str, limit: int = 10) -> list[PipelineRunRow]:
        with self.session() as session:
            return list(
                session.scalars(
                    select(PipelineRunRow)
                    .where(PipelineRunRow.pipeline_id == pipeline_id)
                    .order_by(PipelineRunRow.started_at.desc())
                    .limit(limit)
                ).all()
            )

    def list_recent_runs_for_stage(
        self,
        pipeline_id: str,
        stage_step_names: list[str],
        *,
        stage_name: Optional[str] = None,
        limit: int = 10,
    ) -> list[PipelineRunRow]:
        """Runs that executed at least one step belonging to the stage."""
        if not stage_step_names and not stage_name:
            return []
        with self.session() as session:
            runs: list[PipelineRunRow] = []
            seen: set[str] = set()

            if stage_step_names:
                run_ids = select(PipelineRunStepRow.run_id).where(
                    PipelineRunStepRow.step_name.in_(stage_step_names)
                )
                for row in session.scalars(
                    select(PipelineRunRow)
                    .where(
                        PipelineRunRow.pipeline_id == pipeline_id,
                        PipelineRunRow.run_id.in_(run_ids),
                    )
                    .order_by(PipelineRunRow.started_at.desc())
                    .limit(limit)
                ).all():
                    if row.run_id not in seen:
                        seen.add(row.run_id)
                        runs.append(row)

            if stage_name:
                for row in session.scalars(
                    select(PipelineRunRow)
                    .where(
                        PipelineRunRow.pipeline_id == pipeline_id,
                        PipelineRunRow.status == "running",
                        PipelineRunRow.trigger.in_(stage_trigger_values(stage_name)),
                    )
                    .order_by(PipelineRunRow.started_at.desc())
                    .limit(limit)
                ).all():
                    if row.run_id not in seen:
                        seen.add(row.run_id)
                        runs.append(row)

            runs.sort(key=lambda row: row.started_at, reverse=True)
            return runs[:limit]

    def get_schedule(self, pipeline_id: str) -> Optional[PipelineScheduleRow]:
        with self.session() as session:
            return session.get(PipelineScheduleRow, pipeline_id)
