from typing import Callable, Generator, Iterator, List, Dict, Any, Optional, Tuple, Union, TYPE_CHECKING
import logging

from sqlalchemy.engine import Engine

from sqlalchemy import create_engine, MetaData, Table, Column, Float, String, Numeric
from sqlalchemy.sql.expression import delete, and_, or_, select
import pandas as pd

from c12n_pipe.datatable import DataTable
from c12n_pipe.event_logger import EventLogger


logger = logging.getLogger('c12n_pipe.datatable')


if TYPE_CHECKING:
    from c12n_pipe.datatable import DataTable


class DataStore:
    def __init__(self, connstr: str, schema: str):
        self._init(connstr, schema)

    def _init(self, connstr: str, schema: str) -> None:
        self.connstr = connstr
        self.schema = schema

        self.con = create_engine(
            connstr,
        )

        self.sqla_metadata = MetaData(schema=schema)

        self.event_logger = EventLogger(self)

    def __getstate__(self):
        return {
            'connstr': self.connstr,
            'schema': self.schema
        }

    def __setstate__(self, state):
        self._init(state['connstr'], state['schema'])

    def get_table(self, name: str, data_sql_schema: List[Column], create_tables: bool = True) -> DataTable:
        return DataTable(
            ds=self,
            name=name,
            data_sql_schema=data_sql_schema,
            create_tables=create_tables,
        )

    def get_process_ids(
        self,
        inputs: List[DataTable],
        outputs: List[DataTable],
    ) -> pd.Index:
        idx = None

        def left_join(tbl_a, tbl_bbb):
            q = tbl_a.join(
                tbl_bbb[0],
                tbl_a.c.id == tbl_bbb[0].c.id,
                isouter=True
            )
            for tbl_b in tbl_bbb[1:]:
                q = q.join(
                    tbl_b,
                    tbl_a.c.id == tbl_b.c.id,
                    isouter=True
                )

            return q

        for inp in inputs:
            sql = select([inp.meta_table.c.id]).select_from(
                left_join(inp.meta_table, [out.meta_table for out in outputs])
            ).where(
                or_(
                    or_(
                        out.meta_table.c.process_ts < inp.meta_table.c.update_ts,
                        out.meta_table.c.process_ts.is_(None)
                    )
                    for out in outputs
                )
            )

            idx_df = pd.read_sql_query(
                sql,
                con=self.con,
                index_col='id',
            )

            if idx is None:
                idx = idx_df.index
            else:
                idx = idx.union(idx_df.index)

        for out in outputs:
            sql = select([out.meta_table.c.id]).select_from(
                left_join(out.meta_table, [inp.meta_table for inp in inputs])
            ).where(
                and_(inp.meta_table.c.id.is_(None) for inp in inputs)
            )

            idx_df = pd.read_sql_query(
                sql,
                con=self.con,
                index_col='id',
            )

            if idx is None:
                idx = idx_df.index
            else:
                idx = idx.union(idx_df.index)

        return idx

    def get_process_chunks(
        self,
        inputs: List[DataTable],
        outputs: List[DataTable],
        chunksize: int = 1000,
    ) -> Tuple[pd.Index, Iterator[List[pd.DataFrame]]]:
        idx = self.get_process_ids(
            inputs=inputs,
            outputs=outputs,
        )

        logger.info(f'Items to update {len(idx)}')

        def gen():
            if len(idx) > 0:
                for i in range(0, len(idx), chunksize):
                    yield [inp.get_data(idx[i:i+chunksize]) for inp in inputs]

        return idx, gen()

