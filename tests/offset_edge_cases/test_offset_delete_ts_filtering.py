"""
Тесты для обработки удалений (delete_ts) в offset-оптимизации.

Проблема:
При фильтрации повторной обработки (update_ts > process_ts) не учитывается delete_ts.
Если запись была обработана, а затем удалена (delete_ts установлен, update_ts не изменился),
то условие update_ts > process_ts будет ложным и удаление не попадет в выборку.

Решение:
Использовать coalesce(update_ts, delete_ts) > process_ts или greatest(update_ts, delete_ts) > process_ts
для учета обоих типов изменений.

Код исправления в sql_meta.py:
- В фильтре changed records добавлена проверка delete_ts >= offset
- В фильтре повторной обработки используется OR для учета удалений
"""
import time

import pandas as pd
from sqlalchemy import Column, Integer, String

from datapipe.compute import ComputeInput
from datapipe.datatable import DataStore
from datapipe.step.batch_transform import BatchTransformStep
from datapipe.store.database import DBConn, TableStoreDB


def test_delete_ts_not_processed_after_delete(dbconn: DBConn):
    """
    Проблема: delete_ts не учитывается при фильтрации повторной обработки.

    Сценарий:
    1. Создать запись с update_ts = T1
    2. Обработать (process_ts = T_proc)
    3. Удалить запись (delete_ts = T_del > T_proc), update_ts не меняется
    4. Повторно запустить - удаление должно попасть в выборку

    Ожидание: Фильтр должен проверять coalesce(update_ts, delete_ts) > process_ts
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # Создаем входную таблицу
    input_store = TableStoreDB(
        dbconn,
        "delete_test_input",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    input_dt = ds.create_table("delete_test_input", input_store)

    # Создаем выходную таблицу
    output_store = TableStoreDB(
        dbconn,
        "delete_test_output",
        [
            Column("id", String, primary_key=True),
            Column("value", Integer),
        ],
        create_table=True,
    )
    output_dt = ds.create_table("delete_test_output", output_store)

    def copy_func(df):
        return df[["id", "value"]]

    step = BatchTransformStep(
        ds=ds,
        name="delete_test_copy",
        func=copy_func,
        input_dts=[ComputeInput(dt=input_dt, join_type="full")],
        output_dts=[output_dt],
        transform_keys=["id"],
        use_offset_optimization=True,
        chunk_size=10,
    )

    # 1. Создать запись
    t1 = time.time()
    df = pd.DataFrame({"id": ["rec_1"], "value": [100]})
    input_dt.store_chunk(df, now=t1)

    # Проверяем метаданные
    meta = input_dt.meta_table.get_metadata(df[["id"]])
    assert len(meta) == 1
    assert meta.iloc[0]["update_ts"] == t1
    assert pd.isna(meta.iloc[0]["delete_ts"])

    # 2. Обработать запись
    time.sleep(0.01)
    step.run_full(ds)

    # Проверяем что запись обработана
    output_data = output_dt.get_data()
    assert len(output_data) == 1
    assert output_data.iloc[0]["id"] == "rec_1"

    # Записываем t_proc для дальнейшей проверки
    time.sleep(0.01)
    t_proc = time.time()

    # 3. Удалить запись (delete_ts устанавливается, update_ts НЕ меняется)
    time.sleep(0.01)
    t_del = time.time()
    input_dt.delete_by_idx(df[["id"]], now=t_del)

    # Проверяем метаданные - delete_ts установлен
    meta_after_delete = input_dt.meta_table.get_metadata(df[["id"]], include_deleted=True)
    assert len(meta_after_delete) == 1
    assert pd.notna(meta_after_delete.iloc[0]["delete_ts"]), "delete_ts должен быть установлен"
    delete_ts_value = meta_after_delete.iloc[0]["delete_ts"]
    update_ts_after_delete = meta_after_delete.iloc[0]["update_ts"]

    # Главная проблема: если delete_ts > process_ts, но update_ts <= process_ts,
    # то фильтр (update_ts > process_ts) не пропустит эту запись
    # Нужно использовать coalesce(update_ts, delete_ts) > process_ts

    # 4. Повторно запустить - удаление должно попасть в выборку
    time.sleep(0.01)

    # Проверяем что батчи для обработки есть (есть удаленная запись)
    idx_count, idx_gen = step.get_full_process_ids(ds=ds, run_config=None)
    assert idx_count > 0, (
        f"БАГ: После удаления записи выборка пустая (idx_count={idx_count}). "
        f"Это происходит потому что фильтр проверяет только update_ts > process_ts, "
        f"но не учитывает delete_ts. update_ts={update_ts_after_delete}, delete_ts={delete_ts_value}, t_proc={t_proc}"
    )
