"""
Key mapping example: joining tables whose primary keys don't share the same column name.

Schema
------
  authors (id PK, name)
  posts   (id PK, author_id, title, body)

Both tables use `id` as their primary key, so a plain BatchTransform would not
know how to join them — the transform engine would see two unrelated `id` columns.

InputSpec.keys lets you give each table's primary key a *transform-level* alias,
creating a common coordinate space ("post_id" / "author_id") that the engine uses
to schedule work and match rows.

OutputSpec.keys does the symmetric thing on the output side: it tells the engine
which output primary key column corresponds to which transform key, so cleanup
(deletes / invalidations) stays correct when posts or authors are removed.

Result tables
-------------
  post_cards (id PK, title, author_name)   — one enriched card per post
"""

import pandas as pd
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from datapipe.compute import Catalog, DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BatchTransform
from datapipe.store.database import DBConn
from datapipe.types import InputSpec, OutputSpec

from examples.datapipe_core._sqlite import sqlite_connstr


class Base(DeclarativeBase):
    pass


class Author(Base):
    __tablename__ = "authors"

    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]


class Post(Base):
    __tablename__ = "posts"

    # Composite PK: posts are scoped to an author.
    # Both columns must be PKs so the meta table can track them for scheduling.
    id: Mapped[str] = mapped_column(primary_key=True)
    author_id: Mapped[str] = mapped_column(primary_key=True)
    title: Mapped[str]
    body: Mapped[str]


class PostCard(Base):
    __tablename__ = "post_cards"

    # Primary key is named `id` here, but it stores the *post* id.
    # OutputSpec.keys tells the engine: transform key "post_id" → column "id".
    id: Mapped[str] = mapped_column(primary_key=True)
    title: Mapped[str]
    author_name: Mapped[str]


def generate_authors():
    yield pd.DataFrame(
        [
            {"id": "a1", "name": "Alice"},
            {"id": "a2", "name": "Bob"},
        ]
    )


def generate_posts():
    yield pd.DataFrame(
        [
            {"id": "p1", "author_id": "a1", "title": "Hello World", "body": "My first post."},
            {"id": "p2", "author_id": "a1", "title": "Python Tips", "body": "Use list comprehensions."},
            {"id": "p3", "author_id": "a2", "title": "Data Science 101", "body": "Start with the data."},
        ]
    )


def enrich_posts(posts_df: pd.DataFrame, authors_df: pd.DataFrame) -> pd.DataFrame:
    """
    Receives a chunk of posts (already filtered to the current task batch)
    and the matching authors (joined via author_id).

    Column names here are the *original* table column names, not the transform
    aliases — InputSpec.keys only affects the scheduling join, not what you see
    in the dataframe.
    """
    merged = posts_df.merge(authors_df, left_on="author_id", right_on="id", suffixes=("_post", "_author"))
    return pd.DataFrame(
        {
            "id": merged["id_post"],
            "title": merged["title"],
            "author_name": merged["name"],
        }
    )


pipeline = Pipeline(
    [
        BatchGenerate(generate_authors, outputs=[Author]),
        BatchGenerate(generate_posts, outputs=[Post]),
        BatchTransform(
            enrich_posts,
            # "post_id" and "author_id" are virtual transform-level keys.
            # Each unique (post_id, author_id) pair is one unit of incremental work.
            transform_keys=["post_id", "author_id"],
            inputs=[
                # Post.id    → transform key "post_id"
                # Post.author_id → transform key "author_id"  (direct match by name would work
                #                  here, but listed explicitly for clarity)
                InputSpec(Post, keys={"post_id": "id", "author_id": "author_id"}),
                # Author.id → transform key "author_id"
                # Without this mapping the engine would try to join on a column named
                # "author_id" in the authors meta table, which doesn't exist there.
                InputSpec(Author, keys={"author_id": "id"}),
            ],
            outputs=[
                # PostCard.id stores the post id, so transform key "post_id" → column "id".
                # This ensures that when a post is deleted, the corresponding post_card row
                # is also removed (the engine knows to look up post_card by id = post_id).
                OutputSpec(PostCard, keys={"post_id": "id"}),
            ],
        ),
    ]
)


dbconn = DBConn(sqlite_connstr(), sqla_metadata=Base.metadata)
ds = DataStore(dbconn)
app = DatapipeApp(ds, Catalog({}), pipeline)


if __name__ == "__main__":
    from datapipe.compute import run_steps

    ds.meta_dbconn.sqla_metadata.create_all(ds.meta_dbconn.con)
    run_steps(app.ds, app.steps)

    # Show result
    import sqlalchemy as sa

    with dbconn.con.begin() as con:
        rows = con.execute(sa.text("SELECT id, title, author_name FROM post_cards ORDER BY id")).fetchall()
    print("\npost_cards:")
    for row in rows:
        print(f"  {row.id}: '{row.title}' by {row.author_name}")
