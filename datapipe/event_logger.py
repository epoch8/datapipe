from typing import TYPE_CHECKING
from enum import Enum

import logging
import traceback

from sqlalchemy.sql import func
from sqlalchemy.sql.schema import Column, Table
from sqlalchemy.sql.sqltypes import DateTime, Integer, String, JSON


logger = logging.getLogger('datapipe.event_logger')

if TYPE_CHECKING:
    from datapipe.metastore import DBConn


EVENT_TABLE_SHCEMA = [
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('event_ts', DateTime, server_default=func.now()),
    Column('type', String(100)),
    Column('event', JSON)
]


class EventTypes(Enum):
    STATE = "state"
    ERROR = "error"


class EventLogger:
    def __init__(self, dbconn: 'DBConn'):
        self.dbconn = dbconn

        print(EVENT_TABLE_SHCEMA)
        self.events_table = Table(
            'datapipe_events',
            dbconn.sqla_metadata,
            *[i.copy() for i in EVENT_TABLE_SHCEMA]
        )

        self.events_table.create(self.dbconn.con, checkfirst=True)

    def log_state(self, table_name, added_count, updated_count, deleted_count):
        logger.debug(f'Table "{table_name}": added = {added_count}; updated = {updated_count}; deleted = {deleted_count}')

        ins = self.events_table.insert().values(
            type=EventTypes.STATE.value,
            event={
                "table_name": table_name,
                "added_count": added_count,
                "updated_count": updated_count,
                "deleted_count": deleted_count
            }
        )

        self.dbconn.con.execute(ins)

    def log_error(self, type, message, description, params):
        ins = self.events_table.insert().values(
            type=EventTypes.ERROR.value,
            event={
                "type": type,
                "message": message,
                "description": description,
                "params": params
            }
        )

        self.dbconn.con.execute(ins)

    def log_exception(self, exc: Exception):
        self.log_error(
            type(exc).__name__,
            str(exc),
            traceback.format_exc(),
            exc.args
        )
