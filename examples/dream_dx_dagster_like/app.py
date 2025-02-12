# FastAPI-like syntax for defining a pipeline

import dtp
import pandas as pd
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class InputData(Base):
    pass


class OutputData(Base):
    pass


ingest_pipeline = dtp.Pipeline()


@ingest_pipeline.generate(output=[InputData])
def generate_input() -> pd.DataFrame:
    return pd.DataFrame()


@ingest_pipeline.transform(inputs=[InputData], outputs=[OutputData])
def compute(self, input_df: pd.DataFrame) -> pd.DataFrame:
    return input_df


class CleanData(Base):
    pass


moderate_pipeline = dtp.Pipeline()


@moderate_pipeline.transform(inputs=[OutputData], outputs=[CleanData])
def moderate(self, input_df: pd.DataFrame) -> pd.DataFrame:
    return input_df


app = dtp.Pipeline()
app.add_pipeline(ingest_pipeline)
app.add_pipeline(moderate_pipeline)
