import logging

from typing import Any
from sqlalchemy.engine import create_engine

from sqlalchemy.sql import func
from sqlalchemy.sql.schema import Column, MetaData, Table
from sqlalchemy.sql.sqltypes import DateTime, Integer, String


logger = logging.getLogger('c12n_pipe.event_logger')

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from c12n_pipe.datatable import DataStore

class EventLogger:
    def __init__(self, ds: 'DataStore'):
        self.ds = ds

        self.events_table =  Table(
            'datapipe_events',
            ds.sqla_metadata,

            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('event_ts', DateTime, server_default=func.now()),

            Column('table_name', String(100)),

            Column('added_count', Integer),
            Column('updated_count', Integer),
            Column('deleted_count', Integer),
        )

        self.events_table.create(self.ds.con, checkfirst=True)
    
    def log_event(self, table_name, added_count, updated_count, deleted_count):
        logger.info(f'Table "{table_name}": added = {added_count}; updated = {updated_count}; deleted = {deleted_count}')

        ins = self.events_table.insert().values(
            table_name=table_name,
            added_count=added_count,
            updated_count=updated_count,
            deleted_count=deleted_count,
        )

        self.ds.con.execute(ins)
