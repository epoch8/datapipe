from __future__ import annotations

import logging

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

from datapipe.compute import Catalog, DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform

import data
import steps
from config import DBCONN, ENABLED_ENGINES

pipeline_steps = [
    BatchGenerate(
        steps.list_images,
        outputs=[data.images_tbl],
        labels=[("stage", "ingest")],
    ),
    BatchGenerate(
        steps.list_engines,
        outputs=[data.engines_tbl],
        labels=[("stage", "ingest")],
    ),
    BatchTransform(
        func=steps.ocr_inference,
        inputs=[data.images_tbl, data.engines_tbl],
        outputs=[data.ocr_results_tbl],
        transform_keys=["image_id", "engine_id"],
        chunk_size=8,
        labels=[("stage", "ocr")],
    ),
]

for engine_id in ENABLED_ENGINES:
    pipeline_steps.append(
        BatchTransform(
            func=steps.make_build_image_data(engine_id),
            inputs=[data.images_tbl, data.ocr_results_tbl],
            outputs=[data.fo_tables[engine_id]],
            transform_keys=["image_id"],
            labels=[("stage", "fiftyone")],
        )
    )

pipeline_steps.append(
    BatchTransform(
        func=steps.render_composite,
        inputs=[data.images_tbl, data.ocr_results_tbl],
        outputs=[data.composites_tbl],
        transform_keys=["engine_id", "image_id"],
        labels=[("stage", "composite")],
    )
)

pipeline = Pipeline(pipeline_steps)

ds = DataStore(DBCONN, create_meta_table=True)
app = DatapipeApp(ds, Catalog({}), pipeline)
