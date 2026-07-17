from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, IO, List

import fsspec
from cv_pipeliner.core.data import ImageData
from datapipe.store.filedir import DataDF, HashDF, ItemStoreFileAdapter

from datapipe_ml.frameworks.yolo.dataset import _hash_rows


class StaticClassNamesYOLOLabelsFile(ItemStoreFileAdapter):
    """YOLO label adapter for stores with fixed class names (tests, simple filedir tables)."""

    mode = "b"

    def __init__(self, class_names: List[str], img_format: str):
        from cv_pipeliner.data_converters.yolo import YOLODataConverter

        self.yolo_converter = YOLODataConverter(class_names)
        self.img_format = img_format

    def load(self, f: fsspec.core.OpenFile) -> Dict[str, ImageData]:
        filepath = Path(f.path)
        assert filepath.parent.name == "labels"
        image_path = filepath.parent.parent / "images" / f"{filepath.stem}.{self.img_format}"
        if f.fs.protocol in ["file"] or f.fs.protocol is None:
            prefix = ""
        else:
            protocol = f.fs.protocol
            if isinstance(protocol, tuple):
                protocol = protocol[0]
            prefix = f"{protocol}://"
        image_data = self.yolo_converter.get_image_data_from_annot(image_path=f"{prefix}{image_path}", annot=f)
        return {"image_data": image_data}

    def dump(self, obj: Dict[str, Any], f: fsspec.core.OpenFile) -> None:
        image_data: ImageData = obj["image_data"]
        yolo_data = self.yolo_converter.get_annot_from_image_data(image_data)
        f.write("\n".join(yolo_data).encode())

    def hash_rows(self, df: DataDF, keys: List[str]) -> HashDF:
        return _hash_rows(df, keys)


# Backward-compatible alias used by image_data_stores and tests.
YOLOLabelsFile = StaticClassNamesYOLOLabelsFile
