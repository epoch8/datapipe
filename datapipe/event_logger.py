from typing import TYPE_CHECKING
from enum import Enum

import logging
import traceback

from sqlalchemy.sql import func
from sqlalchemy.sql.schema import Column, Table
from sqlalchemy.sql.sqltypes import DateTime, Integer, String, JSON
from sqlalchemy.dialects.postgresql import JSONB

from datapipe.step import RunConfig


logger = logging.getLogger('datapipe.event_logger')

if TYPE_CHECKING:
    from datapipe.metastore import DBConn


class EventTypes(Enum):
    STATE = "state"
    ERROR = "error"


class EventLogger:
    def __init__(self, dbconn: 'DBConn'):
        self.dbconn = dbconn

        self.events_table = Table(
            'datapipe_events',
            dbconn.sqla_metadata,
            *self._make_table_schema(dbconn),
        )

        self.events_table.create(self.dbconn.con, checkfirst=True)

    def _make_table_schema(self, dbconn: 'DBConn'):
        return [
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('event_ts', DateTime, server_default=func.now()),
            Column('type', String(100)),
            Column('event', JSON if dbconn.con.name == 'sqlite' else JSONB)
        ]

    def log_state(
        self,
        table_name,
        added_count,
        updated_count,
        deleted_count,
        run_config: RunConfig,
    ):
        logger.debug(f'Table "{table_name}": added = {added_count}; updated = {updated_count}; deleted = {deleted_count}')

        ins = self.events_table.insert().values(
            type=EventTypes.STATE.value,
            event={
                "meta": {
                    **run_config.labels,
                    "filters": run_config.filters,
                },
                "data": {
                    "table_name": table_name,
                    "added_count": added_count,
                    "updated_count": updated_count,
                    "deleted_count": deleted_count
                }
            }
        )

        self.dbconn.con.execute(ins)

    def log_error(
        self,
        type,
        message,
        description,
        params,
        run_config: RunConfig,
    ) -> None:
        logger.debug(f'Error in step {run_config.labels.get("step_name")}: {type} {message}')

        ins = self.events_table.insert().values(
            type=EventTypes.ERROR.value,
            event={
                "meta": {
                    **run_config.labels,
                    "filters": run_config.filters,
                },
                "data": {
                    "type": type,
                    "message": message,
                    "description": description,
                    "params": params
                }
            }
        )

        self.dbconn.con.execute(ins)

    def log_exception(
        self,
        exc: Exception,
        run_config: RunConfig,
    ) -> None:
        self.log_error(
            type=type(exc).__name__,
            message=str(exc),
            description=traceback.format_exc(),
            params=exc.args,
            run_config=run_config,
        )
