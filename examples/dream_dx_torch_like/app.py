# Pytorch-like syntax for defining a pipeline

import dtp
import pandas as pd
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class InputData(Base):
    pass


class OutputData(Base):
    pass


class IngestPipeline(dtp.Pipeline):
    def __init__(self) -> None:
        self.input_data = dtp.Table(InputData)
        self.output_data = dtp.Table(OutputData)

        self.generate(
            outputs=[self.input_data],
            func=self.generate_input,
        )

        self.transform(
            inputs=[self.input_data],
            outputs=[self.output_data],
            func=self.compute,
        )

    def generate_input(self) -> pd.DataFrame:
        return pd.DataFrame()

    def compute(self, input_df: pd.DataFrame) -> pd.DataFrame:
        return input_df


class CleanData(Base):
    pass


class ModeratePipeline(dtp.Pipeline):
    def __init__(self, input_data) -> None:
        self.input_data = input_data
        self.output = dtp.Table(OutputData)

        self.transform(
            inputs=[self.input_data],
            outputs=[self.output],
            func=self.moderate,
        )

    def moderate(self, input_df: pd.DataFrame) -> pd.DataFrame:
        return input_df


class ThePipeline(dtp.Pipeline):
    def __init__(self) -> None:
        self.ingest_pipeline = IngestPipeline()
        self.moderate_pipeline = ModeratePipeline(
            input_data=self.ingest_pipeline.output_data
        )


app = ThePipeline()
