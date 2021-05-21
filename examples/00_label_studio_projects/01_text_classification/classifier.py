import json
from typing import List, Callable
import requests
import pandas as pd


def get_embedder_conversion(url: str, request_count: int) -> Callable[[pd.DataFrame], pd.DataFrame]:
    def _preprocess_inputs(texts: List[str]) -> List[str]:
        return [
            text.encode('unicode-escape').decode('ascii')
            for text in texts
        ]

    def _func(df: pd.DataFrame) -> pd.DataFrame:
        encoded_inputs = _preprocess_inputs(df["text"].tolist())
        body = json.dumps({"instances": encoded_inputs}).encode("utf-8")
        for i in range(request_count):
            response = requests.post(url, data=body)
            if response.status_code == 500:
                continue
            assert response.status_code == 200
            response_content = json.loads(response.content)
            assert "predictions" in response_content
            df["embedding"] = response_content["predictions"]
            break
        assert response.status_code == 200, f"Got wrong answer {response.status_code}: {response.content}"

        return df

    return _func


def get_classifier_conversion(url: str, request_count: int) -> Callable[[pd.DataFrame], pd.DataFrame]:
    def _func(df: pd.DataFrame) -> pd.DataFrame:
        encoded_inputs = df["embedding"].tolist()
        body = json.dumps({"instances": encoded_inputs}).encode("utf-8")
        for i in range(request_count):
            response = requests.post(url, data=body)
            if response.status_code == 500:
                continue
            assert response.status_code == 200
            response_content = json.loads(response.content)
            assert "predictions" in response_content
            df["classification"] = response_content["classification"]
            break
        assert response.status_code == 200, f"Got wrong answer {response.status_code}: {response.content}"

        return df

    return _func