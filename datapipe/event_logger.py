import logging


from sqlalchemy.sql import func
from sqlalchemy.sql.schema import Column, Table
from sqlalchemy.sql.sqltypes import DateTime, Integer, String


logger = logging.getLogger('datapipe.event_logger')

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from datapipe.metastore import DBConn

class EventLogger:
    def __init__(self, dbconn: 'DBConn'):
        self.dbconn = dbconn

        self.events_table =  Table(
            'datapipe_events',
            dbconn.sqla_metadata,

            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('event_ts', DateTime, server_default=func.now()),

            Column('table_name', String(100)),

            Column('added_count', Integer),
            Column('updated_count', Integer),
            Column('deleted_count', Integer),
        )

        self.events_table.create(self.dbconn.con, checkfirst=True)
    
    def log_event(self, table_name, added_count, updated_count, deleted_count):
        logger.debug(f'Table "{table_name}": added = {added_count}; updated = {updated_count}; deleted = {deleted_count}')

        ins = self.events_table.insert().values(
            table_name=table_name,
            added_count=added_count,
            updated_count=updated_count,
            deleted_count=deleted_count,
        )

        self.dbconn.con.execute(ins)
