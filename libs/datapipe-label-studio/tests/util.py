import time
from typing import List

import pandas as pd
from datapipe.datatable import DataTable
from datapipe.store.table_store import TableStore
from datapipe.types import DataDF, data_to_index
from label_studio_sdk import LabelStudio

from datapipe_label_studio.sdk_utils import get_tasks_iter, is_service_up


def assert_idx_equal(a, b):
    a = sorted(list(a))
    b = sorted(list(b))

    assert a == b


def assert_df_equal(a: pd.DataFrame, b: pd.DataFrame, index_cols=["id"]) -> bool:
    a = a.set_index(index_cols)
    b = b.set_index(index_cols)

    assert_idx_equal(a.index, b.index)

    eq_rows = (a.sort_index() == b.sort_index()).all(axis="columns")

    if eq_rows.all():
        return True

    else:
        print("Difference")
        print("A:")
        print(a.loc[-eq_rows])
        print("B:")
        print(b.loc[-eq_rows])

        raise AssertionError


def assert_datatable_equal(a: DataTable, b: DataDF) -> bool:
    return assert_df_equal(a.get_data(), b, index_cols=a.primary_keys)


def assert_ts_contains(ts: TableStore, df: DataDF):
    assert_df_equal(
        ts.read_rows(data_to_index(df, ts.primary_keys)),
        df,
        index_cols=ts.primary_keys,
    )


def wait_until_label_studio_is_up(ls: LabelStudio, timeout_seconds: int = 60):
    raise_exception = False
    counter = 0
    while not is_service_up(ls, raise_exception=raise_exception):
        time.sleep(1.0)
        counter += 1
        if counter >= timeout_seconds:
            raise_exception = True


def get_project_id(project) -> int:
    return getattr(project, "id", None) or project.get("id")


def get_project_tasks(ls_client: LabelStudio, project_id: int, page_size: int = 200) -> List[dict]:
    tasks = []
    for page_items in get_tasks_iter(
        ls_client,
        project_id,
        page_size=page_size,
    ):
        tasks.extend(page_items)
    return tasks
