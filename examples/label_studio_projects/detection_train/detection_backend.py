from typing import Dict
from label_studio.ml import LabelStudioMLBase

from cv_pipeliner.core.data import ImageData


def convert_to_rectangle_labels(
    image_data: ImageData
) -> Dict:
    image = image_data.open_image()
    original_height, original_width, _ = image.shape
    rectangle_labels = []
    for bbox_data in image_data.bboxes_data:
        xmin, ymin, xmax, ymax = (
            bbox_data.xmin, bbox_data.ymin, bbox_data.xmax, bbox_data.ymax
        )
        height = ymax - ymin
        width = xmax - xmin
        x = xmin / original_width * 100
        y = ymin / original_height * 100
        height = height / original_height * 100
        width = width / original_width * 100
        rectangle_labels.append({
            "from_name": "bbox",
            "to_name": "image",
            "type": "rectanglelabels",
            "original_width": original_width,
            "original_height": original_height,
            "value": {
                "x": x,
                "y": y,
                "width": width,
                "height": height,
                "rectanglelabels": [bbox_data.label],
                "rotation": bbox_data.angle,
            }
        })
    return rectangle_labels


class DetectionBackend(LabelStudioMLBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.from_name = 'bbox'
        self.info = self.parsed_label_config['bbox']
        self.to_name = 'image'

    def predict(self, tasks, **kwargs):
        # collect input images
        predictions = []
        for task in tasks:
            if 'pred_image_data' in task['data']:
                pred_image_data = ImageData.from_dict(task['data']['pred_image_data'])
                predictions.append(
                    {
                        'result': convert_to_rectangle_labels(pred_image_data),
                        'score': pred_image_data.detection_score
                    }
                )
            else:
                predictions.append({})
        return predictions

    def fit(self, completions, workdir=None, **kwargs):
        return {}
