from dataclasses import dataclass
from typing import List, Optional

from datapipe.compute import Catalog, ComputeStep, DataStore
from datapipe.types import Labels

from datapipe_ml.datasets.freeze import FreezeDatasetStep


@dataclass
class KeypointsFreezeDataset:
    input__image: str
    input__image__ground_truth: str
    input__subset__has__image: str
    output__keypoints_frozen_dataset: str
    output__keypoints_frozen_dataset__has__image_gt: str
    working_dir: str
    primary_keys: List[str]
    min_delta: int = 10
    min_within_time: str = "1w"
    create_table: bool = False
    image__image_path__name: str = "image__image_path"
    labels: Optional[Labels] = None
    keypoints_frozen_dataset_primary_keys: Optional[List[str]] = None
    keypoints_frozen_dataset_id__name: str = "keypoints_frozen_dataset_id"
    bbox_id__name: Optional[str] = "bbox_id"

    def build_compute(self, ds: DataStore, catalog: Catalog) -> List[ComputeStep]:
        step = FreezeDatasetStep(
            model_type="keypoints",
            input__image=self.input__image,
            input__image__ground_truth=self.input__image__ground_truth,
            input__subset__has__image=self.input__subset__has__image,
            output__frozen_dataset=self.output__keypoints_frozen_dataset,
            output__frozen_dataset__has__image_gt=self.output__keypoints_frozen_dataset__has__image_gt,
            working_dir=self.working_dir,
            primary_keys=self.primary_keys,
            min_delta=self.min_delta,
            min_within_time=self.min_within_time,
            create_table=self.create_table,
            image__image_path__name=self.image__image_path__name,
            labels=self.labels,
            frozen_dataset_primary_keys=self.keypoints_frozen_dataset_primary_keys,
            frozen_dataset_id__name=self.keypoints_frozen_dataset_id__name,
            bbox_id__name=self.bbox_id__name,
            extra_gt_columns=["keypoints", "keypoints_visibility", "flip_idx"],
        )
        return step.build_compute(ds, catalog)
