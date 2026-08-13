from functools import partial, update_wrapper
from typing import List, Optional

import numpy as np
import pandas as pd
import pytest

PROJECT_LABEL_CONFIG_TEST = """<View>
  <Text name="text" value="$text"/>
  <Choices name="label" toName="text" choice="single" showInLine="true">
    <Choice value="Class1"/>
    <Choice value="Class2"/>
    <Choice value="Class1_annotation"/>
    <Choice value="Class2_annotation"/>
  </Choices>
</View>"""

TASKS_COUNT = 10

INCLUDE_PARAMS = [
    pytest.param({"include_preannotations": False, "include_prepredictions": False}, id=""),
    # pytest.param(
    #     {
    #         "include_preannotations": True,
    #         "include_prepredictions": False,
    #     },
    #     id="Preann",
    # ),
    # pytest.param(
    #     {
    #         "include_preannotations": False,
    #         "include_prepredictions": True,
    #     },
    #     id="Prepred",
    # ),
    # pytest.param(
    #     {
    #         "include_preannotations": True,
    #         "include_prepredictions": True,
    #     },
    #     id="PreannPrepred",
    # ),
]

INCLUDE_PREDICTIONS = [
    pytest.param(False, id="NoPredsStep"),
    pytest.param(True, id="WithPredStep"),
]

DELETE_UNANNOTATED_TASKS_ONLY_ON_UPDATE = [
    pytest.param(False, id="DelAllOnUpdate"),
    pytest.param(True, id="DelUnAnnOnUpdate"),
    # pytest.param(
    #     True,
    #     id="WithPredStep",
    # ),
]


def wrapped_partial(func, *args, **kwargs):
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    return partial_func


def _make_choice_result():
    return {
        "value": {"choices": [np.random.choice(["Class1", "Class2"])]},
        "from_name": "label",
        "to_name": "text",
        "type": "choices",
    }


def _make_result_item():
    return {"result": [_make_choice_result()]}


def convert_to_ls_input_data(data_df, include_preannotations: bool, include_prepredictions: bool, base_columns):
    columns = list(base_columns)
    for column, bool_ in [
        ("preannotations", include_preannotations),
        ("prepredictions", include_prepredictions),
    ]:
        if bool_:
            data_df[column] = [[_make_result_item()] for _ in range(len(data_df))]
            columns.append(column)
    return data_df[columns]


def add_predictions(data_df, base_columns):
    data_df["prediction"] = [_make_result_item() for _ in range(len(data_df))]
    data_df["model_version"] = "v1"
    return data_df[base_columns + ["model_version", "prediction"]]


def make_best_model_from_predictions(
    data_df,
    model_keys: Optional[List[str]] = None,
    group_by: Optional[List[str]] = None,
):
    model_keys = model_keys or ["model_version"]
    columns = (group_by or []) + model_keys
    if data_df.empty:
        return pd.DataFrame(columns=columns)
    if group_by:
        return data_df.groupby(group_by, as_index=False)[model_keys].first()[columns]
    versions = data_df[model_keys].drop_duplicates()
    if len(versions) != 1:
        raise ValueError(f"Expected one model version, got {len(versions)}")
    return versions.reset_index(drop=True)
