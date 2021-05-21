import json
import time
from typing import List, Callable
import requests
import pandas as pd


def request_retries(url: str, data: bytes, retries_count: int, sleep_timeout: int) -> requests.Response:
    assert retries_count > 0
    assert sleep_timeout > 0
    for i in range(retries_count):
        response = requests.post(url, data=data)
        if response.status_code == 500:
            time.sleep(sleep_timeout)
            continue
        assert response.status_code == 200
        return response
    raise ValueError(f"Got wrong answer after {retries_count} retries: {response.status_code}: {response.content}")


def get_embedder_conversion(url: str, request_count: int, sleep_timeout: int, input_field: str='text', output_field='embedding') -> Callable[[pd.DataFrame], pd.DataFrame]:
    def _preprocess_inputs(texts: List[str]) -> List[str]:
        return [
            text.encode('unicode-escape').decode('ascii')
            for text in texts
        ]

    def _func(df: pd.DataFrame) -> pd.DataFrame:
        encoded_inputs = _preprocess_inputs(df[input_field].tolist())
        body = json.dumps({"instances": encoded_inputs}).encode("utf-8")
        response = request_retries(url, body, request_count, sleep_timeout)
        response_content = json.loads(response.content)
        assert "predictions" in response_content
        df[output_field] = response_content["predictions"]

        return df

    return _func


def get_classifier_conversion(url: str, request_count: int, sleep_timeout: int, input_field: str='embedding', output_field='classification') -> Callable[[pd.DataFrame], pd.DataFrame]:
    def _func(df: pd.DataFrame) -> pd.DataFrame:
        encoded_inputs = df[input_field].tolist()
        body = json.dumps({"instances": encoded_inputs}).encode("utf-8")
        response = request_retries(url, body, request_count, sleep_timeout)
        response_content = json.loads(response.content)
        assert "predictions" in response_content
        df[output_field] = response_content["classification"]

        return df

    return _func
