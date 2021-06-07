# %%
import pandas as pd

# %%
image_df = pd.DataFrame(
    {
        'image_id': ['1', '2'],
        'data': [['a', 'b'], ['c', 'd']],
    }
)

bbox_df = pd.DataFrame(
    {
        'image_id': ['1', '1', '2', '2'],
        'bbox_id': ['a', 'b', 'c', 'd'],
        'data': ['a', 'b', 'c', 'd'],
    }
)

def image_to_bbox(image_df):
    