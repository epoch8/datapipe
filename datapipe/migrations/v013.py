from rich import print as rprint
from sqlalchemy import insert, literal
from sqlalchemy.sql import and_, func, select

from datapipe.step.batch_transform import BaseBatchTransformStep


def migrate_transform_tables(app, steps):
    for batch_transform in steps:
        if not isinstance(batch_transform, BaseBatchTransformStep):
            continue

        rprint(f"Migrate '{batch_transform.get_name()}': ")
        size = batch_transform.meta.get_metadata_size()
        if size > 0:
            print(f"Skipping -- size of metadata is greater 0: {size=}")
            continue
        output_tbls = [output_dt.meta.sql_table for output_dt in batch_transform.output_dts]

        def make_ids_cte():
            ids_cte = (
                select(
                    *[
                        func.coalesce(*[tbl.c[k] for tbl in output_tbls]).label(k)
                        for k in batch_transform.transform_keys
                    ],
                )
                .distinct()
                .select_from(output_tbls[0])
                .where(and_(*[tbl.c.delete_ts.is_(None) for tbl in output_tbls]))
            )

            prev_tbl = output_tbls[0]
            for tbl in output_tbls[1:]:
                ids_cte = ids_cte.outerjoin(
                    tbl,
                    and_(*[prev_tbl.c[k] == tbl.c[k] for k in batch_transform.transform_keys]),
                    full=True,
                )

            return ids_cte.cte(name="ids")

        ids_cte = make_ids_cte()

        sql = (
            select(
                *[ids_cte.c[k] for k in batch_transform.transform_keys],
                func.max(app.ds.meta_dbconn.func_greatest(*[tbl.c["process_ts"] for tbl in output_tbls])).label(
                    "process_ts"
                ),
            )
            .select_from(ids_cte)
            .where(and_(*[tbl.c.delete_ts.is_(None) for tbl in output_tbls]))
        )

        for tbl in output_tbls:
            sql = sql.join(
                tbl,
                and_(*[ids_cte.c[k] == tbl.c[k] for k in batch_transform.transform_keys]),
                isouter=True,
            )

        sql = sql.group_by(*[ids_cte.c[k] for k in batch_transform.transform_keys])

        insert_stmt = insert(batch_transform.meta.sql_table).from_select(
            batch_transform.transform_keys + ["process_ts", "is_success", "error", "priority"],
            select(
                *[sql.c[k] for k in batch_transform.transform_keys],
                sql.c["process_ts"],
                literal(True),
                literal(None),
                literal(0),
            ),
        )
        app.ds.meta_dbconn.con.execute(insert_stmt)
        rprint("  [green] ok[/green]")
