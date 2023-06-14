import logging
from enum import Enum
from typing import TYPE_CHECKING, Optional

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.sql.schema import Column, Table
from sqlalchemy.sql.sqltypes import JSON, DateTime, Integer, String
from traceback_with_variables import format_exc

from datapipe.run_config import RunConfig

logger = logging.getLogger("datapipe.event_logger")

if TYPE_CHECKING:
    from datapipe.metastore import DBConn


class EventTypes(Enum):
    STATE = "state"
    ERROR = "error"


class StepEventTypes(Enum):
    RUN_FULL_COMPLETE = "run_full_complete"


class EventLogger:
    def __init__(self, dbconn: "DBConn", create_table: bool = False):
        self.dbconn = dbconn

        self.events_table = Table(
            "datapipe_events",
            dbconn.sqla_metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("event_ts", DateTime, server_default=func.now()),
            Column("type", String(100)),
            Column("event", JSON if dbconn.con.name == "sqlite" else JSONB),
        )

        self.step_events_table = Table(
            "datapipe_step_events",
            dbconn.sqla_metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("step", String(100)),
            Column("event_ts", DateTime, server_default=func.now()),
            Column("event", String(100)),
            Column("event_payload", JSON if dbconn.con.name == "sqlite" else JSONB),
        )

        if create_table:
            self.events_table.create(self.dbconn.con, checkfirst=True)
            self.step_events_table.create(self.dbconn.con, checkfirst=True)

    def log_state(
        self,
        table_name,
        added_count,
        updated_count,
        deleted_count,
        processed_count,
        run_config: Optional[RunConfig] = None,
    ):
        logger.debug(
            f'Table "{table_name}": added = {added_count}; updated = {updated_count}; '
            f"deleted = {deleted_count}, processed_count = {deleted_count}"
        )

        if run_config is not None:
            meta = {
                "labels": run_config.labels,
                "filters": run_config.filters,
            }
        else:
            meta = {}

        ins = self.events_table.insert().values(
            type=EventTypes.STATE.value,
            event={
                "meta": meta,
                "data": {
                    "table_name": table_name,
                    "added_count": added_count,
                    "updated_count": updated_count,
                    "deleted_count": deleted_count,
                    "processed_count": processed_count,
                },
            },
        )

        with self.dbconn.con.begin() as con:
            con.execute(ins)

    def log_error(
        self,
        type,
        message,
        description,
        params,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        if run_config is not None:
            logger.debug(
                f'Error in step {run_config.labels.get("step_name")}: {type} {message}\n{description}'
            )
            meta = {
                "labels": run_config.labels,
                "filters": run_config.filters,
            }
        else:
            logger.debug(f"Error: {type} {message}\n{description}")
            meta = {}

        ins = self.events_table.insert().values(
            type=EventTypes.ERROR.value,
            event={
                "meta": meta,
                "data": {
                    "type": type,
                    "message": message,
                    "description": description,
                    "params": params,
                },
            },
        )

        with self.dbconn.con.begin() as con:
            con.execute(ins)

    def log_exception(
        self,
        exc: Exception,
        run_config: Optional[RunConfig] = None,
    ) -> None:
        self.log_error(
            type=type(exc).__name__,
            message=str(exc),
            description=format_exc(exc),
            params=[],  # exc.args, # Not all args can be serialized to JSON, dont really need them
            run_config=run_config,
        )

    def log_step_full_complete(
        self,
        step_name: str,
    ) -> None:
        logger.debug(f"Step {step_name} is marked complete")

        ins = self.step_events_table.insert().values(
            step=step_name,
            event=StepEventTypes.RUN_FULL_COMPLETE.value,
        )

        with self.dbconn.con.begin() as con:
            con.execute(ins)
