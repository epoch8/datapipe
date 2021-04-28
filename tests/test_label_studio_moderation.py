# flake8: noqa
import os
import distutils.util
from subprocess import Popen

import time
import pytest
import pandas as pd
import numpy as np
from PIL import Image

from datapipe.dsl import Catalog, ExternalTable, LabelStudioModeration, Pipeline, Table, BatchGenerate, BatchTransform
from datapipe.metastore import MetaStore
from datapipe.store.filedir import JSONFile, TableStoreFiledir, PILFile
from datapipe.compute import build_compute, run_steps
from datapipe.label_studio.session import LabelStudioModerationStep, LabelStudioSession

from .util import dbconn, tmp_dir


LABEL_CONFIG_TEST = '''<View>
<Text name="text" value="$unique_id"/>
<Image name="image" value="$image"/>
<RectangleLabels name="label" toName="image">
    <Label value="Class1" background="#6600ff"/>
    <Label value="Class2" background="#0000ff"/>
</RectangleLabels>
</View>'''

PROJECT_SETTING = {
    "title": "Detection Project",
    "description": "Detection project",
    "label_config": LABEL_CONFIG_TEST,
    "expert_instruction": "",
    "show_instruction": False,
    "show_skip_button": True,
    "enable_empty_annotation": True,
    "show_annotation_history": False,
    "organization": 1,
    "color": "#FFFFFF",
    "maximum_annotations": 1,
    "is_published": False,
    "model_version": "",
    "is_draft": False,
    "min_annotations_to_start_training": 10,
    "show_collab_predictions": True,
    "sampling": "Sequential sampling",
    "show_ground_truth_first": True,
    "show_overlap_first": True,
    "overlap_cohort_percentage": 100,
    "task_data_login": None,
    "task_data_password": None,
    "control_weights": {}
}


@pytest.fixture
def ls_url(tmp_dir):
    ls_host = os.environ.get('LABEL_STUDIO_HOST', 'localhost')
    ls_port = os.environ.get('LABEL_STUDIO_PORT', '8080')
    ls_url = f"http://{ls_host}:{ls_port}/"
    # Run the process manually
    if bool(distutils.util.strtobool(os.environ.get('TEST_ENABLE_LABEL_STUDIO_MANUALLY', 'True'))):
        label_studio_service = Popen([
            'label-studio',
            '--database', os.environ.get('LABEL_STUDIO_BASE_DATA_DIR', str(tmp_dir / 'ls.db')),
            '--internal-host', os.environ.get('LABEL_STUDIO_HOST', 'localhost'),
            '--port', os.environ.get('LABEL_STUDIO_PORT', '8080'),
            '--no-browser'
        ])
        yield ls_url
        label_studio_service.terminate()
    else:
        yield ls_url


def make_df():
    idx = [f'im_{i}' for i in range(10)]
    return pd.DataFrame(
        {
            'image': [Image.fromarray(np.random.randint(0, 256, (100, 100, 3)), 'RGB') for i in idx]
        },
        index=idx
    )

def gen_images():
    yield make_df()

def convert_to_ls_input_data(
    images_df,
):
    images_df['data'] = images_df.index.map(
        lambda id: {
            'unique_id': id,
            'image': f"00_dataset/{id}.jpg"  # For tests we do not see images, so it's like that
        }
    )
    return images_df[['data']]


def test_label_studio_moderation(dbconn, tmp_dir, ls_url):
    ms = MetaStore(dbconn)
    catalog = Catalog({
        'images': Table(
            store=TableStoreFiledir(tmp_dir / '00_images' / '{id}.jpg', PILFile('JPEG')),
        ),
        'data': Table(
            store=TableStoreFiledir(tmp_dir / '01_data' / '{id}.json', JSONFile()),
        ),
        'annotations': Table(  # Updates when someone is annotating
            store=TableStoreFiledir(tmp_dir / '02_annotations' / '{id}.json', JSONFile()),
        ),
    })

    pipeline = Pipeline([
        BatchGenerate(
            gen_images,
            outputs=['images'],
        ),
        BatchTransform(
            convert_to_ls_input_data,
            inputs=['images'],
            outputs=['data']
        ),
        LabelStudioModeration(
            ls_url=ls_url,
            project_setting=PROJECT_SETTING,
            inputs=['data'],
            outputs=['annotations'],
        ),
    ])

    steps = build_compute(ms, catalog, pipeline)
    label_studio_moderation_step : LabelStudioModerationStep = steps[-1]

    run_steps(ms, steps)

    assert len(catalog.get_datatable(ms, 'data').get_data()) == 10

    # Wait until Label Studio is up
    label_studio_session = LabelStudioSession(ls_url)
    raise_exception = False
    counter = 0
    while not label_studio_session.is_service_up(raise_exception=raise_exception):
        time.sleep(1.)
        counter += 1
        if counter >= 30:
            raise_exception = True

    # These steps should upload tasks
    run_steps(ms, steps)

    assert len(catalog.get_datatable(ms, 'annotations').get_data()) == 10

    # Person annotation imitation & incremental processing
    tasks = label_studio_session.get_all_tasks(project_id=label_studio_moderation_step.project_id)
    tasks = np.array(tasks)
    for idxs in [[0, 3, 6, 7, 9], [1, 2, 4, 5, 8]]:
        for task in tasks[idxs]:
            label_studio_session.add_annotation_to_task(
                task_id=task['id'],
                result=[
                    {
                        "original_width": 100,
                        "original_height": 100,
                        "image_rotation": 0,
                        "value": {
                            "x": np.random.random_sample(),
                            "y": np.random.random_sample(),
                            "width": 10 * np.random.random_sample(),
                            "height": 10 * np.random.random_sample(), 
                            "rotation": 0, 
                            "rectanglelabels": [np.random.choice(["Class1", "Class2"])]
                        },
                        "from_name": "label",
                        "to_name": "image",
                        "type": "rectanglelabels"
                    }
                ]
        )
        run_steps(ms, steps)
        idxs_df = [task['data']['unique_id'] for task in tasks[idxs]]
        df_annotation = catalog.get_datatable(ms, 'annotations').get_data(idx=idxs_df)
        for idx_df in idxs_df:
            assert len(df_annotation.loc[idx_df, 'annotations']) == 1
            assert df_annotation.loc[idx_df, 'annotations'][0]['result'][0]['value']['rectanglelabels'][0] in (
                ["Class1", "Class2"]
            )

    # Check if pipeline works after service is over
    run_steps(ms, steps)