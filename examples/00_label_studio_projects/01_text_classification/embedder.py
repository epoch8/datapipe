import json
from typing import List
import requests
import pandas as pd
from datapipe.dsl import BatchTransform


class TextEmbedder(BatchTransform):
    def __init__(self, url: str, inputs: List[str], outputs: List[str], chunk_size: int = 1000):
        super(TextEmbedder, self).__init__(func=self.apply_conversion,
                                           inputs=inputs,
                                           outputs=outputs,
                                           chunk_size=chunk_size)
        self.url = url

    def preprocess_inputs(self, texts: List[str]) -> List[str]:
        return [
            text.encode('unicode-escape').decode('ascii')
            for text in texts
        ]

    def apply_conversion(self, df: pd.DataFrame) -> pd.DataFrame:
        encoded_inputs = self.preprocess_inputs(df["text"].tolist())
        body = json.dumps({"instances": encoded_inputs}).encode("utf-8")
        response = requests.post(self.url, data=body)
        assert response.status_code == 200
        response_content = json.loads(response.content)
        assert "predictions" in response_content
        df["embedding"] = response_content["predictions"]
        return df
