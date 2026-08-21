# How to Map Mismatched Primary Keys

Join tables whose primary-key **column names** do not line up — without renaming every store schema.

## Goal

Tell Datapipe how input (and output) table columns map onto a shared **transform-key** space so multi-table transforms schedule and clean up correctly.

## When you need this

Plain `BatchTransform` joins on overlapping primary-key *names*. That fails when:

- Two tables both use `id` for different entities (post vs author).
- An FK column on one side (`author_id`) is the PK on the other (`id`).
- Output PK names differ from transform keys (output `id` stores a post id).

Use `InputSpec` / `OutputSpec` (or `Required`, a thin subclass of `InputSpec`) plus explicit `transform_keys`.

## Steps

### 1. Choose transform-level key names

Pick aliases that describe the join space, for example `post_id` and `author_id`. These names do **not** have to exist as columns in every table.

### 2. Map each input with `InputSpec.keys`

```python
from datapipe.types import InputSpec, OutputSpec

BatchTransform(
    enrich_posts,
    transform_keys=["post_id", "author_id"],
    inputs=[
        # Post.id → post_id; Post.author_id → author_id
        InputSpec(Post, keys={"post_id": "id", "author_id": "author_id"}),
        # Author.id → author_id
        InputSpec(Author, keys={"author_id": "id"}),
    ],
    outputs=[
        # PostCard.id stores the post id
        OutputSpec(PostCard, keys={"post_id": "id"}),
    ],
)
```

`keys` maps **transform key → table column**. Datapipe uses this for scheduling joins and delete/invalidation; your function still sees the **original** table column names in each DataFrame.

### 3. Join inside the function as usual

```python
def enrich_posts(posts_df: pd.DataFrame, authors_df: pd.DataFrame) -> pd.DataFrame:
    merged = posts_df.merge(
        authors_df, left_on="author_id", right_on="id", suffixes=("_post", "_author")
    )
    return pd.DataFrame({
        "id": merged["id_post"],
        "title": merged["title"],
        "author_name": merged["name"],
    })
```

### 4. Map outputs with `OutputSpec.keys`

So that when a transform key is deleted, Datapipe knows which output primary-key column to clean up.

## Expected result

- Each unique transform-key tuple is one incremental unit of work.
- Authors join to posts via `author_id` even though the authors table’s PK is named `id`.
- Deleting a post removes the matching `post_cards` row keyed by that post’s id.

## Example

Full pipeline: [`examples/datapipe_core/key_mapping/`](https://github.com/epoch8/datapipe/tree/master/examples/datapipe_core/key_mapping).

## See also

- [Primary Keys and Transform Keys](../concepts/primary-keys.md)
- [Run Model Inference](./model-inference.md) — product grain when names already align
- Design notes: `libs/datapipe-core/design-docs/2025-12-key-mapping.md`
