# Case: model inference on images

Imagine we have two input tables:

* `models` indexed by `model_id`
* `images` indexed by `image_id`

We need to run transform `model_infence` which result in table
`model_inference_for_image` indexed by `model_id,image_id`.

Transform (individual tasks to run) is indexed by `model_id,image_id`.

Query is built by the following strategy:
1. aggregate each input table by the intersection of it's keys and transform keys
1. for each input aggregate:
    1. outer join transform table with input aggregate by intersection of keys
    1. select rows where update_ts &gt; process_ts
1. union all results
1. select distinct rows

SQL query to find which tasks should be run looks like:

```sql
WITH models__update_ts AS (
    SELECT model_id, update_ts
    FROM models
),
images__update_ts AS (
    SELECT image_id, update_ts
    FROM images
)
SELECT
    COALESCE(i.image_id, t.image_id) image_id,
    COALESCE(i.model_id, t.model_id) model_id
FROM input__update_ts i
OUTER JOIN transform_meta t ON i.image_id = t.image_id AND i.model_id = t.model_id
WHERE i.update_ts > t.process_ts
```