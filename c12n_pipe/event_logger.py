import logging

from typing import Any
from sqlalchemy.engine import create_engine

from sqlalchemy.sql import func
from sqlalchemy.sql.schema import Column, MetaData, Table
from sqlalchemy.sql.sqltypes import DateTime, Integer, String


logger = logging.getLogger('c12n_pipe.event_logger')


def make_event_table(schema, con):
    metadata = MetaData(schema=schema)
    tbl =  Table(
        'datapipe_events',
        metadata,

        Column('id', Integer, pirmary_key=True),
        Column('event_ts', DateTime, server_default=func.now()),

        Column('table_name', String(100)),

        Column('added_count', Integer),
        Column('updated_count', Integer),
        Column('deleted_count', Integer),
    )

    metadata.create_all(con)

    return tbl

class EventLogger:
    def __init__(self, constr: str, schema: str):
        self.constr = constr
        self.schema = schema

        self.events_table = make_event_table(schema, create_engine(constr))
    
    def log_event(self, table_name, added_count, updated_count, deleted_count):
        logger.info(f'Table "{table_name}": added = {added_count}; updated = {updated_count}; deleted = {deleted_count}')

        ins = self.events_table.insert().values(
            table_name=table_name,
            added_count=added_count,
            updated_count=updated_count,
            deleted_count=deleted_count,
        )

        create_engine(self.constr).execute(ins)

