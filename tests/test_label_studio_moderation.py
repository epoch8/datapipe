# flake8: noqa
from functools import partial, update_wrapper
import os
import distutils.util
from subprocess import Popen

import time
from datapipe.datatable import gen_process
import pytest
import pandas as pd
import numpy as np
from PIL import Image

from datapipe.dsl import Catalog, LabelStudioModeration, Pipeline, Table, BatchGenerate, BatchTransform
from datapipe.metastore import MetaStore
from datapipe.store.filedir import JSONFile, TableStoreFiledir, PILFile
from datapipe.compute import build_compute, run_steps
from datapipe.label_studio.session import LabelStudioModerationStep, LabelStudioSession

from .util import dbconn, tmp_dir


LABEL_STUDIO_AUTH = ('test@epoch8.co', 'qwerty123')

LABEL_CONFIG_TEST = '''<View>
<Image name="image" value="$image"/>
<RectangleLabels name="label" toName="image">
    <Label value="Class1" background="#6600ff"/>
    <Label value="Class2" background="#0000ff"/>
</RectangleLabels>
</View>'''

PROJECT_NAME_TEST1 = 'Detection Project Test 1'
PROJECT_NAME_TEST2 = 'Detection Project Test 2'
PROJECT_NAME_TEST3 = 'Detection Project Test 3'
PROJECT_DESCRIPTION_TEST = 'Detection project'
PROJECT_LABEL_CONFIG_TEST = LABEL_CONFIG_TEST


@pytest.fixture
def ls_url(tmp_dir):
    ls_host = os.environ.get('LABEL_STUDIO_HOST', 'localhost')
    ls_port = os.environ.get('LABEL_STUDIO_PORT', '8080')
    ls_url = f"http://{ls_host}:{ls_port}/"
    # Run the process manually
    if bool(distutils.util.strtobool(os.environ.get('TEST_START_LABEL_STUDIO', 'True'))):
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


def wait_until_label_studio_is_up(label_studio_session: LabelStudioSession):
    raise_exception = False
    counter = 0
    while not label_studio_session.is_service_up(raise_exception=raise_exception):
        time.sleep(1.)
        counter += 1
        if counter >= 60:
            raise_exception = True

def test_sign_up(ls_url):
    label_studio_session = LabelStudioSession(ls_url=ls_url, auth=('test_auth@epoch8.co', 'qwerty123'))
    wait_until_label_studio_is_up(label_studio_session)
    assert not label_studio_session.is_auth_ok(raise_exception=False)
    label_studio_session.sign_up()
    assert label_studio_session.is_auth_ok(raise_exception=False)


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


def wrapped_partial(func, *args, **kwargs):
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    return partial_func


def convert_to_ls_input_data(
    images_df,
    include_annotations: bool,
    include_predictions: bool
):
    images_df['image'] = images_df.index.map(
        lambda id: f"00_dataset/{id}.jpg"  # For tests we do not see images, so it's like that
    )

    columns = ['image']

    for column, bool_ in [
        ('annotations', include_annotations),
        ('predictions', include_predictions),
    ]:
        if bool_:
            images_df[column] = [[{
                'result': [{
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
                }]
            }] for i in range(10)]
            columns.append(column)

    return images_df[columns]


@pytest.mark.parametrize("include_annotations,include_predictions", [(False, False), (False, True)])
def test_label_studio_moderation(dbconn, tmp_dir, ls_url, include_annotations, include_predictions):
    ms = MetaStore(dbconn)
    catalog = Catalog({
        '00_images': Table(
            store=TableStoreFiledir(tmp_dir / '00_images' / '{id}.jpg', PILFile('JPEG')),
        ),
        '01_data': Table(
            store=TableStoreFiledir(tmp_dir / '01_data' / '{id}.json', JSONFile()),
        ),
        '02_annotations': Table(  # Updates when someone is annotating
            store=TableStoreFiledir(tmp_dir / '02_annotations' / '{id}.json', JSONFile()),
        ),
    })

    pipeline = Pipeline([
        BatchTransform(
            wrapped_partial(
                convert_to_ls_input_data,
                include_annotations=include_annotations,
                include_predictions=include_predictions
            ),
            inputs=['00_images'],
            outputs=['01_data']
        ),
        LabelStudioModeration(
            ls_url=ls_url,
            inputs=['01_data'],
            outputs=['02_annotations'],
            auth=LABEL_STUDIO_AUTH,
            project_title=PROJECT_NAME_TEST1,
            project_description=PROJECT_DESCRIPTION_TEST,
            project_label_config=PROJECT_LABEL_CONFIG_TEST,
            data=['image'],
            chunk_size=2,
            annotations='annotations' if include_annotations else None,
            predictions='predictions' if include_predictions else None,
        ),
    ])

    steps = build_compute(ms, catalog, pipeline)
    label_studio_moderation_step : LabelStudioModerationStep = steps[-1]

    label_studio_session = LabelStudioSession(ls_url=ls_url, auth=LABEL_STUDIO_AUTH)
    wait_until_label_studio_is_up(label_studio_session)

    # This should be ok (project will be created, but without data)
    run_steps(ms, steps)
    run_steps(ms, steps)

    # This step should upload tasks (it also can be BatchGenerate as first step of pipeline)
    gen_process(
        dt=catalog.get_datatable(ms, '00_images'),
        proc_func=gen_images
    )

    assert len(catalog.get_datatable(ms, '02_annotations').get_data()) == 10

    # Person annotation imitation & incremental processing
    tasks, _ = label_studio_session.get_tasks(
        project_id=label_studio_moderation_step.project_id,
        page_size=-1
    )
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
        idxs_df = [task['data']['LabelStudioModerationStep__unique_id'] for task in tasks[idxs]]
        df_annotation = catalog.get_datatable(ms, '02_annotations').get_data(idx=idxs_df)
        for idx_df in idxs_df:
            assert len(df_annotation.loc[idx_df, 'annotations']) == 1
            assert df_annotation.loc[idx_df, 'annotations'][0]['result'][0]['value']['rectanglelabels'][0] in (
                ["Class1", "Class2"]
            )

    # Check if pipeline works after service is over
    run_steps(ms, steps)

    # Delete the project after the test
    res = label_studio_session.delete_project(project_id=label_studio_moderation_step.project_id)


@pytest.mark.parametrize("include_annotations,include_predictions", [(True, False), (True, True)])
def test_label_studio_moderation_with_preannotations(dbconn, tmp_dir, ls_url, include_annotations, include_predictions):
    ms = MetaStore(dbconn)
    catalog = Catalog({
        '00_images': Table(
            store=TableStoreFiledir(tmp_dir / '00_images' / '{id}.jpg', PILFile('JPEG')),
        ),
        '01_data': Table(
            store=TableStoreFiledir(tmp_dir / '01_data' / '{id}.json', JSONFile()),
        ),
        '02_annotations': Table(
            store=TableStoreFiledir(tmp_dir / '02_annotations' / '{id}.json', JSONFile()),
        ),
    })
    pipeline = Pipeline([
        BatchGenerate(
            gen_images,
            outputs=['00_images'],
        ),
        BatchTransform(
            wrapped_partial(
                convert_to_ls_input_data,
                include_annotations=include_annotations,
                include_predictions=include_predictions
            ),
            inputs=['00_images'],
            outputs=['01_data']
        ),
        LabelStudioModeration(
            ls_url=ls_url,
            inputs=['01_data'],
            outputs=['02_annotations'],
            auth=LABEL_STUDIO_AUTH,
            project_title=PROJECT_NAME_TEST2,
            project_description=PROJECT_DESCRIPTION_TEST,
            project_label_config=PROJECT_LABEL_CONFIG_TEST,
            data=['image'],
            chunk_size=2,
            annotations='annotations' if include_annotations else None,
            predictions='predictions' if include_predictions else None,
        ),
        LabelStudioModeration(
            ls_url=ls_url,
            inputs=['01_data'],
            outputs=['02_annotations'],
            auth=LABEL_STUDIO_AUTH,
            project_title=PROJECT_NAME_TEST2,
            project_description=PROJECT_DESCRIPTION_TEST,
            project_label_config=PROJECT_LABEL_CONFIG_TEST,
            data=['image'],
            chunk_size=2,
            annotations='annotations'
        ),
    ])

    steps = build_compute(ms, catalog, pipeline)
    label_studio_moderation_step : LabelStudioModerationStep = steps[-1]

    label_studio_session = LabelStudioSession(ls_url=ls_url, auth=LABEL_STUDIO_AUTH)
    wait_until_label_studio_is_up(label_studio_session)

    # These steps should upload tasks with preannotations
    run_steps(ms, steps)

    assert len(catalog.get_datatable(ms, '02_annotations').get_data()) == 10
    df_annotation = catalog.get_datatable(ms, '02_annotations').get_data()
    for idx in df_annotation.index:
        assert len(df_annotation.loc[idx, 'annotations']) == 1
        assert df_annotation.loc[idx, 'annotations'][0]['result'][0]['value']['rectanglelabels'][0] in (
            ["Class1", "Class2"]
        )

    # Delete the project after the test
    label_studio_session.delete_project(project_id=label_studio_moderation_step.project_id)
