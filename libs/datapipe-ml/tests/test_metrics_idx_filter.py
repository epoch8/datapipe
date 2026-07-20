import pandas as pd
from sqlalchemy import Column, MetaData, String, Table

from datapipe_ml.metrics.common import idx_columns_present_on_table, idx_in_table_clause


def test_idx_columns_present_on_table_skips_run_config_extra_filters():
    meta = MetaData()
    tbl = Table(
        "metrics_on_image",
        meta,
        Column("detection_model_id", String),
        Column("tag_id", String),
        Column("subset_id", String),
    )
    idx = pd.DataFrame(
        [
            {
                "detection_model_id": "m1",
                "tag_id": "night",
                "subset_id": "train",
                "training_request_id": "request_abc",
            }
        ]
    )

    assert idx_columns_present_on_table(tbl, idx) == [
        "detection_model_id",
        "tag_id",
        "subset_id",
    ]
    clause = idx_in_table_clause(tbl, idx)
    compiled = str(clause.compile(compile_kwargs={"literal_binds": True}))
    assert "training_request_id" not in compiled
    assert "m1" in compiled
