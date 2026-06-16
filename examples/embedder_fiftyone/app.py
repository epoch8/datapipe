from __future__ import annotations

import logging

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

from datapipe.compute import DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform

import steps
from config import DBCONN
from data import catalog

pipeline = Pipeline(
    [
        # Stage source: discover local images.
        BatchGenerate(
            steps.list_local_images,
            outputs=["local_images"],
            labels=[("stage", "source")],
        ),
        # Stage source: load optional classification labels.
        BatchGenerate(
            steps.list_image_labels,
            outputs=["image_labels"],
            labels=[("stage", "source")],
        ),
        # Stage embedder: publish embedder model configs.
        BatchGenerate(
            steps.list_embedders,
            outputs=["embedder"],
            labels=[("stage", "embedder")],
        ),
        # Stage fiftyone: publish images with optional classification to dataset.
        BatchTransform(
            func=steps.build_image_data,
            inputs=["local_images", "image_labels"],
            outputs=["fiftyone_samples"],
            transform_keys=["image_name"],
            labels=[("stage", "fiftyone")],
        ),
        # Stage embeddings: compute and cache per-image vectors (one batch per embedder).
        BatchTransform(
            func=steps.infer_embeddings,
            inputs=["local_images", "embedder"],
            outputs=["embeddings"],
            transform_keys=["image_name", "embedder_id"],
            labels=[("stage", "embeddings")],
        ),
        # Stage fiftyone-brain: compute UMAP + similarity per embedder.
        BatchTransform(
            func=steps.run_brain,
            inputs=["embeddings", "fiftyone_samples"],
            outputs=["brain_status"],
            transform_keys=["embedder_id"],
            labels=[("stage", "fiftyone-brain")],
        ),
    ]
)

ds = DataStore(DBCONN, create_meta_table=True)
app = DatapipeApp(ds, catalog, pipeline)
