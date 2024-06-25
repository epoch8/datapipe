from enum import Enum
from typing import Optional, Tuple

from sqlalchemy import and_, func, insert, literal, not_, or_, select, update

from datapipe.datatable import DataTable
from datapipe.store.database import TableStoreDB


class LintStatus(Enum):
    OK = "ok"
    SKIP = "skip"
    FAIL = "fail"


class Lint:
    desc: str

    def check(self, dt: DataTable) -> Tuple[LintStatus, Optional[str]]:
        query = self.check_query(dt)

        if query is None:
            return (LintStatus.SKIP, None)

        with dt.meta_table.dbconn.con.begin() as con:
            res = con.execute(query).fetchone()

        assert res is not None and len(res) == 1
        (cnt,) = res

        if cnt == 0:
            return (LintStatus.OK, None)
        else:
            return (LintStatus.FAIL, f"{cnt} rows")

    def check_query(self, dt: DataTable):
        raise NotImplementedError

    def fix(self, dt: DataTable) -> Tuple[LintStatus, Optional[str]]:
        raise NotImplementedError


class LintDeleteTSIsNewerThanUpdateOrProcess(Lint):
    lint_id = "DTP001"
    desc = "delete_ts is newer than update_ts or process_ts"

    def check_query(self, dt: DataTable):
        meta_tbl = dt.meta_table.sql_table
        sql = (
            select(func.count())
            .select_from(meta_tbl)
            .where(
                and_(
                    or_(
                        meta_tbl.c.update_ts < meta_tbl.c.delete_ts,
                        meta_tbl.c.process_ts < meta_tbl.c.delete_ts,
                    ),
                    meta_tbl.c.delete_ts != None,
                )
            )
        )

        return sql

    def fix(self, dt: DataTable):
        meta_tbl = dt.meta_table.sql_table

        sql = (
            update(meta_tbl)
            .where(
                and_(
                    or_(
                        meta_tbl.c.update_ts < meta_tbl.c.delete_ts,
                        meta_tbl.c.process_ts < meta_tbl.c.delete_ts,
                    ),
                    meta_tbl.c.delete_ts != None,
                )
            )
            .values(
                update_ts=meta_tbl.c.delete_ts,
                process_ts=meta_tbl.c.delete_ts,
            )
        )

        with dt.meta_table.dbconn.con.begin() as con:
            con.execute(sql)

        return (LintStatus.OK, None)


class LintDataWOMeta(Lint):
    desc = "data has rows without meta"

    def check_query(self, dt: DataTable):
        if not isinstance(dt.table_store, TableStoreDB):
            return None

        meta_tbl = dt.meta_table.sql_table
        data_tbl = dt.table_store.data_table

        exists_sql = (
            select(literal(1))
            .select_from(meta_tbl)
            .where(
                and_(
                    *[
                        meta_tbl.c[col.name] == data_tbl.c[col.name]
                        for col in meta_tbl.columns
                        if col.primary_key
                    ]
                )
            )
        )

        sql = (
            select(func.count()).select_from(data_tbl).where(not_(exists_sql.exists()))
        )

        return sql

    def fix(self, dt: DataTable):
        assert isinstance(dt.table_store, TableStoreDB)

        meta_tbl = dt.meta_table.sql_table
        data_tbl = dt.table_store.data_table

        exists_sql = (
            select(literal(1))
            .select_from(meta_tbl)
            .where(
                and_(
                    *[
                        meta_tbl.c[col.name] == data_tbl.c[col.name]
                        for col in meta_tbl.columns
                        if col.primary_key
                    ]
                )
            )
        )

        sql = insert(meta_tbl).from_select(
            [col.name for col in meta_tbl.columns if col.primary_key]
            + ["hash", "create_ts", "update_ts", "delete_ts"],
            select(  # type: ignore
                *[data_tbl.c[col.name] for col in meta_tbl.columns if col.primary_key]
                + [
                    literal(0).label("hash"),
                    literal(0).label("create_ts"),
                    literal(0).label("update_ts"),
                    literal(None).label("delete_ts"),
                ]
            )
            .select_from(data_tbl)
            .where(not_(exists_sql.exists())),
        )

        with dt.meta_table.dbconn.con.begin() as con:
            con.execute(sql)

        return (LintStatus.OK, None)
