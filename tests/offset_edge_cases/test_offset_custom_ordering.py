"""
Тесты для кастомной сортировки (order_by) с offset-оптимизацией.

Проблема:
При передаче кастомного order_by новый порядок по update_ts пропадает:
сортируется только по переданным колонкам. Это создает риск нехронологического
чтения данных, что может привести к пропуску записей при использовании offset.

Решение:
Всегда префиксовать order_by колонкой update_ts ASC NULLS LAST перед
пользовательским order_by для обеспечения хронологического порядка обработки.

Код исправления в sql_meta.py (строки 1194-1204):
- При наличии кастомного order_by всегда добавляется сортировка по update_ts перед пользовательской
"""
import time

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_custom_order_by_preserves_update_ts_ordering(dbconn: DBConn):
    """
    Проблема: При передаче кастомного order_by теряется сортировка по update_ts.

    Сценарий:
    1. Создать записи с разными update_ts в обратном порядке по id
       - rec_3 создается раньше (меньший update_ts = T1)
       - rec_2 создается позже (update_ts = T2)
       - rec_1 создается последним (больший update_ts = T3)
    2. Передать order_by=['id'] (сортировка по id)
    3. Проверить что записи обрабатываются в порядке update_ts (T1, T2, T3),
       а не по id (rec_1, rec_2, rec_3)

    Ожидание: order_by должен всегда префиксоваться update_ts ASC NULLS LAST
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Создаем входную таблицу
    input_store = TableStoreDB(
        dbconn,
        "order_test_input",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("order_test_input", input_store)

    # Создаем выходную таблицу для отслеживания порядка
    output_store = TableStoreDB(
        dbconn,
        "order_test_output",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
            Column("processing_order", Integer),  # Порядок обработки
        ],
        create_table=True,
    )
    output_dt = ds.create_table("order_test_output", output_store)

    # Счетчик для отслеживания порядка обработки
    processing_counter = {"count": 0}

    def tracking_func(df):
        result = df[["id", "value"]].copy()
        # Добавляем порядок обработки
        result["processing_order"] = list(range(processing_counter["count"], processing_counter["count"] + len(df)))
        processing_counter["count"] += len(df)
        return result

    step = BatchTransformStep(
        ds=ds,
        name="order_test",
        func=tracking_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=10,
        order_by=["id"],  # Кастомный порядок - сортировка по id
        order="asc",
    )

    # Создаем записи с update_ts в обратном порядке по id
    # id = "rec_3" создается раньше (меньший update_ts = T1)
    # id = "rec_1" создается позже (больший update_ts = T3)
    t1 = time.time()
    df1 = pd.DataFrame({"id": ["rec_3"], "value": [3]})
    input_dt.store_chunk(df1, now=t1)

    time.sleep(0.01)
    t2 = time.time()
    df2 = pd.DataFrame({"id": ["rec_2"], "value": [2]})
    input_dt.store_chunk(df2, now=t2)

    time.sleep(0.01)
    t3 = time.time()
    df3 = pd.DataFrame({"id": ["rec_1"], "value": [1]})
    input_dt.store_chunk(df3, now=t3)

    # Проверяем update_ts
    meta = input_dt.meta_table.get_metadata(pd.DataFrame({"id": ["rec_1", "rec_2", "rec_3"]}))
    meta_sorted = meta.sort_values("id")
    assert t3 > t2 > t1
    assert meta_sorted[meta_sorted["id"] == "rec_1"].iloc[0]["update_ts"] == t3
    assert meta_sorted[meta_sorted["id"] == "rec_2"].iloc[0]["update_ts"] == t2
    assert meta_sorted[meta_sorted["id"] == "rec_3"].iloc[0]["update_ts"] == t1

    # Обработать
    step.run_full(ds)

    # Проверить порядок обработки
    output_data = output_dt.get_data()
    assert len(output_data) == 3

    # Сортируем по порядку обработки
    output_sorted = output_data.sort_values("processing_order")

    # Ожидаем что записи обработаны в порядке update_ts: rec_3 (t1), rec_2 (t2), rec_1 (t3)
    # Несмотря на order_by=['id'], сортировка по update_ts должна быть приоритетнее
    expected_order = ["rec_3", "rec_2", "rec_1"]
    actual_order = output_sorted["id"].tolist()

    assert actual_order == expected_order, (
        f"Записи должны обрабатываться в порядке update_ts, а не id. "
        f"Ожидалось: {expected_order}, получено: {actual_order}. "
        f"Это означает что кастомный order_by=['id'] игнорирует сортировку по update_ts, "
        f"что может привести к пропуску данных при использовании offset."
    )
