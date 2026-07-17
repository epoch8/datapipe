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
from config import DBCONN

pipeline = Pipeline(
    [
        # Stage source: load images and labels (local folder or FiftyOne zoo fallback).
        BatchGenerate(
            steps.list_images_and_labels,
            outputs=[data.local_images_tbl, data.image_labels_tbl],
            labels=[("stage", "source")],
        ),
        # Stage embedder: publish embedder model configs.
        BatchGenerate(
            steps.list_embedders,
            outputs=[data.embedder_tbl],
            labels=[("stage", "embedder")],
        ),
        # Stage fiftyone: publish images with optional classification to dataset.
        BatchTransform(
            func=steps.build_image_data,
            inputs=[data.local_images_tbl, data.image_labels_tbl],
            outputs=[data.fiftyone_samples_tbl],
            transform_keys=["image_name"],
            labels=[("stage", "fiftyone")],
        ),
        # Stage embeddings: compute and cache per-image vectors (one batch per embedder).
        BatchTransform(
            func=steps.infer_embeddings,
            inputs=[data.local_images_tbl, data.embedder_tbl],
            outputs=[data.embeddings_tbl],
            transform_keys=["image_name", "embedder_id"],
            labels=[("stage", "embeddings")],
            chunk_size=1000,
        ),
        # Stage fiftyone-brain: compute UMAP + similarity per embedder.
        BatchTransform(
            func=steps.run_brain,
            inputs=[data.embeddings_tbl, data.fiftyone_samples_tbl],
            outputs=[data.brain_status_tbl],
            transform_keys=["embedder_id"],
            labels=[("stage", "fiftyone-brain")],
        ),
    ]
)

ds = DataStore(DBCONN, create_meta_table=True)
app = DatapipeApp(ds, Catalog({}), pipeline)
