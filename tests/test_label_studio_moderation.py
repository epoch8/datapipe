import os
import distutils.util
import time
import string
from functools import partial, update_wrapper
from subprocess import Popen
from typing import List

import pytest
import pandas as pd
import numpy as np
from PIL import Image

from datapipe.dsl import Catalog, LabelStudioModeration, Pipeline, Table, BatchTransform
from datapipe.datatable import DataStore, DataTable
from datapipe.store.filedir import JSONFile, TableStoreFiledir, PILFile
from datapipe.compute import build_compute, run_steps
from datapipe.label_studio.session import LabelStudioSession
from datapipe.step import ComputeStep
from datapipe.datatable import gen_process

from pytest_cases import parametrize_with_cases, parametrize


LABEL_STUDIO_AUTH = ('test@epoch8.co', 'qwerty123')

PROJECT_LABEL_CONFIG_TEST = '''<View>
  <Text name="text" value="$text"/>
  <Choices name="label" toName="text" choice="single" showInLine="true">
    <Choice value="Class1"/>
    <Choice value="Class2"/>
    <Choice value="Class1_annotation"/>
    <Choice value="Class2_annotation"/>
  </Choices>
</View>'''


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

# def test_sign_up(ls_url):
#     label_studio_session = LabelStudioSession(ls_url=ls_url, auth=('test_auth@epoch8.co', 'qwerty123'))
#     wait_until_label_studio_is_up(label_studio_session)
#     assert not label_studio_session.is_auth_ok(raise_exception=False)
#     label_studio_session.sign_up()
#     assert label_studio_session.is_auth_ok(raise_exception=False)


def gen_data_df():
    yield pd.DataFrame(
        {
            'id': [f'task_{i}' for i in range(10)],
            'text': [np.random.choice([x for x in string.ascii_letters]) for i in range(10)]
        }
    )


def wrapped_partial(func, *args, **kwargs):
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    return partial_func


def convert_to_ls_input_data(
    images_df,
    include_annotations: bool,
    include_predictions: bool
):
    images_df['image'] = images_df['id'].apply(
        lambda id: f"00_dataset/{id}.jpg"  # For tests we do not see images, so it's like that
    )

    columns = ['id', 'text']

    for column, bool_ in [
        ('annotations', include_annotations),
        ('predictions', include_predictions),
    ]:
        classes = ["Class1", "Class2"] if not include_annotations else ["Class1_annotation", "Class2_annotation"]
        if bool_:
            images_df[column] = [[{
                'result': [{
                    "value": {
                        "choices": [np.random.choice(classes)]
                    },
                    "from_name": "label",
                    "to_name": "text",
                    "type": "choices"
                }]
            }] for i in range(10)]
            columns.append(column)

    return images_df[columns]


INCLUDE_PARAMS = [
    pytest.param(
        {
            'include_annotations': False,
            'include_predictions': False
        },
        id='default'
    ),
    # pytest.param(
    #     {
    #         'include_annotations': True,
    #         'include_predictions': False
    #     },
    #     id='with_annotations'
    # ),
    # pytest.param(
    #     {
    #         'include_annotations': False,
    #         'include_predictions': True
    #     },
    #     id='with_predictions'
    # ),
    # pytest.param(
    #     {
    #         'include_annotations': True,
    #         'include_predictions': True
    #     },
    #     id='with_annotations_and_predictions'
    # ),
]


class CasesLabelStudio:
    @parametrize('include_params', INCLUDE_PARAMS)
    def case_ls(self, include_params, dbconn, tmp_dir, ls_url, request):
        include_annotations, include_predictions = include_params['include_annotations'], include_params['include_predictions']
        ds = DataStore(dbconn)
        catalog = Catalog({
            '00_input_data': Table(
                store=TableStoreFiledir(tmp_dir / '00_input_data' / '{id}.json', JSONFile()),
            ),
            '01_ls_data': Table(
                store=TableStoreFiledir(tmp_dir / '01_ls_data' / '{id}.json', JSONFile()),
            ),
            '02_annotations': Table(
                store=TableStoreFiledir(tmp_dir / '02_annotations' / '{id}.json', JSONFile()),
            ),
        })
        project_title = f"Project [{request.node.callspec.id}]"
        pipeline = Pipeline([
            BatchTransform(
                wrapped_partial(
                    convert_to_ls_input_data,
                    include_annotations=include_annotations,
                    include_predictions=include_predictions
                ),
                inputs=['00_input_data'],
                outputs=['01_ls_data']
            ),
            LabelStudioModeration(
                ls_url=ls_url,
                inputs=['01_ls_data'],
                outputs=['02_annotations'],
                auth=LABEL_STUDIO_AUTH,
                project_title=project_title,
                project_label_config=PROJECT_LABEL_CONFIG_TEST,
                data=['text'],
                chunk_size=2,
                annotations='annotations' if include_annotations else None,
                predictions='predictions' if include_predictions else None,
            ),
        ])
        steps = build_compute(ds, catalog, pipeline)
        label_studio_session = LabelStudioSession(ls_url=ls_url, auth=LABEL_STUDIO_AUTH)
        wait_until_label_studio_is_up(label_studio_session)
        label_studio_session.login()

        yield ds, catalog, steps, project_title, include_annotations, label_studio_session

        label_studio_session.delete_project(project_id=label_studio_session.get_project_id_by_title(project_title))


# @parametrize_with_cases('ds, catalog, steps, project_title, include_annotations, label_studio_session', cases=CasesLabelStudio)
# def test_label_studio_moderation(
#     ds: DataStore, catalog: Catalog, steps: List[ComputeStep],
#     project_title: str, include_annotations: bool,
#     label_studio_session: LabelStudioSession
# ):
#     # This should be ok (project will be created, but without data)
#     run_steps(ds, steps)
#     run_steps(ds, steps)

#     # These steps should upload tasks
#     gen_process(
#         dt=catalog.get_datatable(ds, '00_input_data'),
#         proc_func=gen_data_df
#     )
#     run_steps(ds, steps)

#     assert len(catalog.get_datatable(ds, '02_annotations').get_data()) == 10

#     # Проверяем проверку на заливку уже размеченных данных
#     if include_annotations:
#         assert len(catalog.get_datatable(ds, '02_annotations').get_data()) == 10
#         df_annotation = catalog.get_datatable(ds, '02_annotations').get_data()
#         for idx in df_annotation.index:
#             assert len(df_annotation.loc[idx, 'annotations']) == 1
#             assert df_annotation.loc[idx, 'annotations'][0]['result'][0]['value']['choices'][0] in (
#                 ["Class1_annotation", "Class2_annotation"]
#             )

#     # Person annotation imitation & incremental processing
#     label_studio_session.login()
#     project_id = label_studio_session.get_project_id_by_title(project_title)
#     tasks = label_studio_session.get_tasks(project_id=project_id, page_size=-1)
#     tasks = np.array(tasks)
#     for idxs in [[0, 3, 6, 7, 9], [1, 2, 4, 5, 8]]:
#         for task in tasks[idxs]:
#             label_studio_session.add_annotation_to_task(
#                 task_id=task['id'],
#                 result=[
#                     {
#                         "value": {
#                             "choices": [np.random.choice(["Class1", "Class2"])]
#                         },
#                         "from_name": "label",
#                         "to_name": "text",
#                         "type": "choices"
#                     }
#                 ]
#             )
#         run_steps(ds, steps)
#         idxs_df = pd.DataFrame.from_records(
#             {
#                 'id': [task['data']['id'] for task in tasks[idxs]]
#             }
#         )
#         df_annotation = catalog.get_datatable(ds, '02_annotations').get_data(idx=idxs_df)
#         for idx in df_annotation.index:
#             assert len(df_annotation.loc[idx, 'annotations']) == (1 + include_annotations)
#             assert df_annotation.loc[idx, 'annotations'][0]['result'][0]['value']['choices'][0] in (
#                 ["Class1", "Class2"]
#             )


# @parametrize_with_cases('ds, catalog, steps, project_title, include_annotations, label_studio_session',
#                         cases=CasesLabelStudio)
# def test_label_studio_update_tasks_when_data_is_changed(
#     ds: DataStore, catalog: Catalog, steps: List[ComputeStep],
#     project_title: str, include_annotations: bool,
#     label_studio_session: LabelStudioSession
# ):
#     # These steps should upload tasks
#     gen_process(
#         dt=catalog.get_datatable(ds, '00_input_data'),
#         proc_func=gen_data_df
#     )
#     run_steps(ds, steps)

#     # Generate new data with same id
#     # images_df = catalog.get_datatable(ds, '00_input_data').get_data()
#     # for idx in images_df.index:
#     #     images_df.loc[idx]
#     gen_process(
#         dt=catalog.get_datatable(ds, '00_input_data'),
#         proc_func=gen_data_df
#     )
#     # These steps should update tasks with same id accordingly, as data input has changed
#     run_steps(ds, steps)

#     label_studio_session.login()
#     project_id = label_studio_session.get_project_id_by_title(project_title)
#     tasks = label_studio_session.get_tasks(project_id=project_id, page_size=-1)
#     assert len(tasks) == 10


@parametrize_with_cases('ds, catalog, steps, project_title, include_annotations, label_studio_session',
                        cases=CasesLabelStudio)
def test_label_studio_update_tasks_when_data_is_deleted(
    ds: DataStore, catalog: Catalog, steps: List[ComputeStep],
    project_title: str, include_annotations: bool,
    label_studio_session: LabelStudioSession
):
    # These steps should upload tasks
    gen_process(
        dt=catalog.get_datatable(ds, '00_input_data'),
        proc_func=gen_data_df
    )
    run_steps(ds, steps)

    # Remove 5 elements from df
    deleted_tasks = [f'task_{i}' for i in [0, 3, 5, 7, 9]]
    datatable: DataTable = catalog.get_datatable(ds, '00_input_data')
    datatable.table_store.delete_rows(pd.DataFrame({'id': deleted_tasks}))
    # These steps should delete tasks with same id accordingly, as data input has changed
    run_steps(ds, steps)

    label_studio_session.login()
    project_id = label_studio_session.get_project_id_by_title(project_title)
    tasks = label_studio_session.get_tasks(project_id=project_id, page_size=-1)
    assert len(tasks) == 5
