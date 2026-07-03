from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

metadata = MetaData()
Base = declarative_base(metadata=metadata)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PipelineRegistryRow(Base):
    __tablename__ = "pipeline_registry"

    pipeline_id = Column(String(255), primary_key=True)
    display_name = Column(String(512), nullable=False)
    task_type = Column(String(64), nullable=True)
    registered_at = Column(DateTime(timezone=True), nullable=False)
    last_heartbeat_at = Column(DateTime(timezone=True), nullable=True)
    agent_url = Column(String(1024), nullable=True)


class PipelineRunRow(Base):
    __tablename__ = "pipeline_runs"

    run_id = Column(String(64), primary_key=True)
    pipeline_id = Column(String(255), nullable=False, index=True)
    status = Column(String(32), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    error = Column(Text, nullable=True)
    trigger = Column(String(64), nullable=True)


class PipelineRunStepRow(Base):
    __tablename__ = "pipeline_run_steps"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(64), nullable=False, index=True)
    step_name = Column(String(512), nullable=False)
    status = Column(String(32), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    processed = Column(Integer, nullable=True)
    total = Column(Integer, nullable=True)
    error = Column(Text, nullable=True)


class PipelineRunLogRow(Base):
    __tablename__ = "pipeline_run_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(64), nullable=False, index=True)
    seq = Column(Integer, nullable=False)
    logged_at = Column(DateTime(timezone=True), nullable=False)
    level = Column(String(16), nullable=False)
    message = Column(Text, nullable=False)

    __table_args__ = (UniqueConstraint("run_id", "seq", name="uq_run_log_seq"),)


class PipelineScheduleRow(Base):
    __tablename__ = "pipeline_schedules"

    pipeline_id = Column(String(255), primary_key=True)
    cron_expression = Column(String(128), nullable=True)
    next_run_at = Column(DateTime(timezone=True), nullable=True)
    timezone = Column(String(64), nullable=True)


class ObservabilityStore:
    def __init__(self, engine: Engine):
        self.engine = engine
        self._Session = sessionmaker(bind=engine, expire_on_commit=False)

    @classmethod
    def from_url(cls, url: str) -> "ObservabilityStore":
        engine = create_engine(url, future=True)
        store = cls(engine)
        store.create_all()
        return store

    def create_all(self) -> None:
        Base.metadata.create_all(self.engine)

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
        with self.session() as session:
            row = session.get(PipelineRunRow, run_id)
            if row is None:
                return
            row.status = status
            row.finished_at = utc_now()
            row.error = error
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
                        PipelineRunRow.trigger == f"api:stage:{stage_name}",
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

    def list_metrics(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    def upsert_pipeline_metric(self, row: dict[str, Any]) -> None:
        return None

    def list_training_epoch_metrics(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    def upsert_training_epoch_metric(self, row: dict[str, Any]) -> None:
        return None

    def has_metrics(self, pipeline_id: Optional[str] = None) -> bool:
        return False
