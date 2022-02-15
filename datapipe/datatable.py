from typing import Dict, Iterator, List, Optional, Tuple, Set

import logging
from opentelemetry import trace

import pandas as pd
from sqlalchemy import alias, func, select, union, and_, or_, literal, Table, Column

from datapipe.types import DataDF, MetadataDF, IndexDF, data_to_index, index_difference
from datapipe.store.database import DBConn, sql_apply_runconfig_filter
from datapipe.metastore import MetaTable
from datapipe.store.table_store import TableStore
from datapipe.event_logger import EventLogger
from datapipe.run_config import RunConfig, LabelDict


logger = logging.getLogger('datapipe.datatable')
tracer = trace.get_tracer("datapipe.datatable")


class DataTable:
    def __init__(
        self,
        name: str,
        meta_dbconn: DBConn,
        meta_table: MetaTable,
        table_store: TableStore,
        event_logger: EventLogger,
    ):
        self.name = name
        self.meta_dbconn = meta_dbconn
        self.meta_table = meta_table
        self.table_store = table_store
        self.event_logger = event_logger

        self.primary_schema = meta_table.primary_schema
        self.primary_keys = meta_table.primary_keys

    def get_metadata(self, idx: Optional[IndexDF] = None) -> MetadataDF:
        return self.meta_table.get_metadata(idx)

    def get_data(self, idx: Optional[IndexDF] = None) -> DataDF:
        return self.table_store.read_rows(self.meta_table.get_existing_idx(idx))

    def store_chunk(
        self,
        data_df: DataDF,
        processed_idx: IndexDF = None,
        now: float = None,
        run_config: RunConfig = None,
    ) -> None:
        '''
        Записать новые данные в таблицу.

        При указанном `processed_idx` удалить те строки, которые находятся внутри `processed_idx`, но
        отсутствуют в `data_df`.
        '''

        with tracer.start_as_current_span(f"{self.name} store_chunk"):
            if not data_df.empty:
                logger.debug(f'Inserting chunk {len(data_df.index)} rows into {self.name}')

                with tracer.start_as_current_span("get_changes_for_store_chunk"):
                    (
                        new_df,
                        changed_df,
                        new_meta_df,
                        changed_meta_df
                    ) = self.meta_table.get_changes_for_store_chunk(data_df, now)

                self.event_logger.log_state(
                    self.name,
                    added_count=len(new_df),
                    updated_count=len(changed_df),
                    deleted_count=0,
                    processed_count=len(data_df),
                    run_config=run_config,
                )

                # TODO implement transaction meckanism
                with tracer.start_as_current_span("store data"):
                    self.table_store.insert_rows(new_df)
                    self.table_store.update_rows(changed_df)

                with tracer.start_as_current_span("store metadata"):
                    self.meta_table.insert_meta_for_store_chunk(new_meta_df)
                    self.meta_table.update_meta_for_store_chunk(changed_meta_df)
            else:
                data_df = pd.DataFrame(columns=self.primary_keys)

            with tracer.start_as_current_span("cleanup deleted rows"):
                data_idx = data_to_index(data_df, self.primary_keys)

                if processed_idx is not None:
                    existing_idx = self.meta_table.get_existing_idx(processed_idx)
                    deleted_idx = index_difference(existing_idx, data_idx)

                    self.delete_by_idx(deleted_idx, now=now, run_config=run_config)

    def delete_by_idx(
        self,
        idx: IndexDF,
        now: float = None,
        run_config: RunConfig = None,
    ) -> None:
        if len(idx) > 0:
            logger.debug(f'Deleting {len(idx.index)} rows from {self.name} data')
            self.event_logger.log_state(
                self.name,
                added_count=0,
                updated_count=0,
                deleted_count=len(idx),
                processed_count=len(idx),
                run_config=run_config,
            )

            self.table_store.delete_rows(idx)
            self.meta_table.mark_rows_deleted(idx, now=now)

    def delete_stale_by_process_ts(
        self,
        process_ts: float,
        now: float = None,
        run_config: RunConfig = None,
    ) -> None:
        for deleted_df in self.meta_table.get_stale_idx(process_ts, run_config=run_config):
            deleted_idx = data_to_index(deleted_df, self.primary_keys)

            self.delete_by_idx(deleted_idx, now=now, run_config=run_config)


class DataStore:
    def __init__(
        self,
        meta_dbconn: DBConn
    ) -> None:
        self.meta_dbconn = meta_dbconn
        self.event_logger = EventLogger(self.meta_dbconn)
        self.tables: Dict[str, DataTable] = {}

    def create_table(self, name: str, table_store: TableStore) -> DataTable:
        assert(name not in self.tables)

        primary_schema = table_store.get_primary_schema()

        res = DataTable(
            name=name,
            meta_dbconn=self.meta_dbconn,
            meta_table=MetaTable(
                dbconn=self.meta_dbconn,
                name=name,
                primary_schema=primary_schema,
            ),
            table_store=table_store,
            event_logger=self.event_logger,
        )

        self.tables[name] = res

        return res

    def get_or_create_table(self, name: str, table_store: TableStore) -> DataTable:
        if name in self.tables:
            return self.tables[name]
        else:
            return self.create_table(name, table_store)

    def get_table(self, name: str) -> DataTable:
        return self.tables[name]

    def _build_changed_idx_sql(
        self,
        inputs: List[DataTable],
        outputs: List[DataTable],
        run_config: RunConfig = None,
    ) -> Tuple[Set[str], select]:
        """
        Построить SQL запрос на получение измененных идентификаторов.

        Returns: [join_keys, select]
        """
        inp_p_keys = [set(inp.primary_keys) for inp in inputs]
        out_p_keys = [set(out.primary_keys) for out in outputs]
        join_keys = set.intersection(*inp_p_keys, *out_p_keys)

        # if not join_keys:
        #     raise ValueError("Impossible to carry out transformation. datatables do not contain intersecting ids")

        def left_join(tbl_left: Tuple[DataTable, Table], tbls_right: List[Tuple[DataTable, Table]]):
            dt_left, sql_left = tbl_left
            q = sql_left
            for dt_right, sql_right in tbls_right:
                join_keys = set(dt_left.primary_keys) & set(dt_right.primary_keys)
                q = q.join(
                    sql_right,
                    and_(True, *[sql_left.c[key] == sql_right.c[key] for key in join_keys]),
                    isouter=True
                )
            return q

        def get_inner_sql_fields(tables: List[Tuple[DataTable, Table]]) -> List[Column]:
            fields = [literal(1).label('_1')]
            # fields += [tables[0][1].c[key] for key in join_keys]
            used_fields: Set[str] = set()
            for dt, sql in tables:
                tbl_keys = join_keys & set(dt.primary_keys)
                for key in tbl_keys - used_fields:
                    fields.append(sql.c[key])
                used_fields |= tbl_keys
            for key in join_keys - used_fields:
                fields.append(literal(None).label(key))
            return sorted(fields, key=lambda x: x.name)

        inp_tbls = [(inp, inp.meta_table.sql_table.alias(f"inp_{inp.name}")) for inp in inputs]
        out_tbls = [(out, out.meta_table.sql_table.alias(f"out_{out.name}")) for out in outputs]
        sql_requests = []

        for inp_dt, inp in inp_tbls:
            fields = get_inner_sql_fields([(inp_dt, inp)] + out_tbls)
            sql = select(fields).select_from(
                left_join((inp_dt, inp), out_tbls)
            ).where(
                or_(
                    and_(
                        or_(
                            (
                                out.c.process_ts
                                <
                                inp.c.update_ts
                            ),
                            out.c.process_ts.is_(None)
                        ),
                        inp.c.delete_ts.is_(None)
                    )
                    for _, out in out_tbls
                )
            )

            sql = sql_apply_runconfig_filter(sql, inp, inp_dt.primary_keys, run_config)

            sql_requests.append(sql)

        for out_dt, out in out_tbls:
            fields = get_inner_sql_fields([(out_dt, out)] + inp_tbls)
            sql = select(fields).select_from(
                left_join((out_dt, out), inp_tbls)
            ).where(
                or_(
                    (
                        out.c.process_ts
                        <
                        inp.c.delete_ts
                    )
                    for _, inp in inp_tbls
                )
            )

            sql = sql_apply_runconfig_filter(sql, out, out_dt.primary_keys, run_config)

            sql_requests.append(sql)

        u1 = union(*sql_requests)

        return (join_keys, u1)

    def get_changed_idx_count(
        self,
        inputs: List[DataTable],
        outputs: List[DataTable],
        run_config: RunConfig = None,
    ) -> int:
        _, sql = self._build_changed_idx_sql(
            inputs=inputs,
            outputs=outputs,
            run_config=run_config
        )

        idx_count = self.meta_dbconn.con.execute(
            select([func.count()])
            .select_from(
                alias(sql.subquery(), name='union_select')
            )
        ).scalar()

        return idx_count

    def get_process_ids(
        self,
        inputs: List[DataTable],
        outputs: List[DataTable],
        chunksize: int = 1000,
        run_config: RunConfig = None,
    ) -> Tuple[int, Iterator[IndexDF]]:
        '''
        Метод для получения перечня индексов для обработки.

        Returns: (idx_size, iterator<idx_df>)
            idx_size - количество индексов требующих обработки
            idx_df - датафрейм без колонок с данными, только индексная колонка
        '''

        if len(inputs) == 0:
            return (0, iter([]))

        idx_count = self.get_changed_idx_count(
            inputs=inputs,
            outputs=outputs,
            run_config=run_config,
        )

        join_keys, u1 = self._build_changed_idx_sql(
            inputs=inputs,
            outputs=outputs,
            run_config=run_config,
        )

        # Список ключей из фильтров, которые нужно добавить в результат
        extra_filters: LabelDict
        if run_config is not None:
            extra_filters = {
                k: v
                for k, v in run_config.filters.items()
                if k not in join_keys
            }
        else:
            extra_filters = {}

        def alter_res_df():
            for df in pd.read_sql_query(
                u1,
                con=self.meta_dbconn.con,
                chunksize=chunksize
            ):
                for k, v in extra_filters.items():
                    df[k] = v

                # drop pseudo column
                yield df.drop(columns='_1')

        return idx_count, alter_res_df()
