import pandas as pd
from sqlalchemy import Column, String, ForeignKey, Boolean, relationship
from sqlalchemy.ext.declarative import declarative_base

from .datapipe_ng import Pipeline, MetaMixin, QdrantStore, compile_to_prefect


Base = declarative_base()


# https://docs.sqlalchemy.org/en/14/orm/basic_relationships.html#one-to-one


pipeline = Pipeline(Base)


class Image(Base, MetaMixin):  # type: ignore
    image_id = Column(String(), primary_key=True)
    image_safe = relationship("ImageIsSafe")


class ImageIsSafe(Base, MetaMixin):  # type: ignore
    image_id = Column(String(), primary_key=True)

    is_safe = Column(Boolean())


class ImageIsGreyscale(Base, MetaMixin):  # type: ignore
    image_id = Column(String(), primary_key=True)

    is_greyscale = Column(Boolean())


class ImageShouldPublish(Base, MetaMixin):  # type: ignore
    image_id = Column(String(), primary_key=True)

    should_publish = Column(Boolean())


class QdrantIndex(Base, MetaMixin):  # type: ignore
    __store__ = QdrantStore("image_emb")

    image_id = Column(String(), ForeignKey("image.image_id"))

    embedding: bytes


@pipeline.transform(ids=["image_id"])
def compute_should_publish(
    image_is_safe: ImageIsSafe,
    image_is_greyscale: ImageIsGreyscale,
) -> ImageShouldPublish:
    return ImageShouldPublish(
        item_id=image_is_safe.item_id,
        should_publish=image_is_safe.is_safe and not image_is_greyscale.is_greyscale,
    )


@pipeline.transform_pandas(batch=100)
def compute_should_publish_pd(
    image_is_safe: pd.DataFrame,
    image_is_greyscale: pd.DataFrame,
) -> pd.DataFrame:
    raise NotImplementedError()


@pipeline.transform_sql
def sql():
    pass


pipeline.run(
    dbconn="postgresql://..", connections={"qdrant": "...."}, selector={"stage": "fast"}
)


flow = compile_to_prefect(pipeline)
