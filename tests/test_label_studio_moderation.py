from typing import List

import pytest
from pytest_cases import parametrize_with_cases, parametrize

import time
import string
from functools import partial, update_wrapper
import pandas as pd
import numpy as np

from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import JSON, String

from datapipe.compute import Catalog, Pipeline, Table, DatatableTransformStep
from datapipe.core_steps import batch_generate_wrapper
from datapipe.datatable import DataStore
from datapipe.store.database import TableStoreDB
from datapipe.compute import build_compute, run_steps
from datapipe.label_studio.session import LabelStudioSession
from datapipe.label_studio.pipeline import LabelStudioStep
from datapipe.types import data_to_index

from tests.conftest import TEST_LABEL_STUDIO, assert_df_equal


PROJECT_LABEL_CONFIG_TEST = '''<View>
  <Text name="text" value="$text"/>
  <Choices name="label" toName="text" choice="single" showInLine="true">
    <Choice value="Class1"/>
    <Choice value="Class2"/>
    <Choice value="Class1_annotation"/>
    <Choice value="Class2_annotation"/>
  </Choices>
</View>'''


def wait_until_label_studio_is_up(label_studio_session: LabelStudioSession):
    raise_exception = False
    counter = 0
    while not label_studio_session.is_service_up(raise_exception=raise_exception):
        time.sleep(1.)
        counter += 1
        if counter >= 60:
            raise_exception = True


TASKS_COUNT = 10
PAGE_CHUNK_SIZE = 2


def gen_data_df():
    yield pd.DataFrame(
        {
            'id': [f'task_{i}' for i in range(TASKS_COUNT)],
            'text': [np.random.choice([x for x in string.ascii_letters]) for i in range(TASKS_COUNT)]
        }
    )


def wrapped_partial(func, *args, **kwargs):
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    return partial_func


def convert_to_ls_input_data(
    data_df,
    include_preannotations: bool,
    include_predictions: bool
):
    data_df['image'] = data_df['id'].apply(
        lambda id: f"00_dataset/{id}.jpg"  # For tests we do not see images, so it's like that
    )

    columns = ['id', 'text']

    for column, bool_ in [
        ('preannotations', include_preannotations),
        ('predictions', include_predictions),
    ]:
        classes = ["Class1", "Class2"] if not include_preannotations else ["Class1_annotation", "Class2_annotation"]
        if bool_:
            data_df[column] = [[{
                'result': [{
                    "value": {
                        "choices": [np.random.choice(classes)]
                    },
                    "from_name": "label",
                    "to_name": "text",
                    "type": "choices"
                }]
            }] for i in range(len(data_df))]
            columns.append(column)

    return data_df[columns]


INCLUDE_PARAMS = [
    pytest.param(
        {
            'include_preannotations': False,
            'include_predictions': False
        },
        id='default'
    ),
    pytest.param(
        {
            'include_preannotations': True,
            'include_predictions': False
        },
        id='with_annotations'
    ),
    pytest.param(
        {
            'include_preannotations': False,
            'include_predictions': True
        },
        id='with_predictions'
    ),
    pytest.param(
        {
            'include_preannotations': True,
            'include_predictions': True
        },
        id='with_annotations_and_predictions'
    ),
]


class CasesLabelStudio:
    @pytest.mark.skipif(not TEST_LABEL_STUDIO, reason="env variable 'TEST_LABEL_STUDIO' is not set")
    @parametrize('include_params', INCLUDE_PARAMS)
    def case_ls(self, include_params, dbconn, ls_url_and_auth, request):
        ls_url, auth = ls_url_and_auth
        include_preannotations, include_predictions = (
            include_params['include_preannotations'], include_params['include_predictions']
        )
        project_title = f"Project [{request.node.callspec.id}]"
        ds = DataStore(dbconn)
        catalog = Catalog({
            '00_input_data': Table(
                store=TableStoreDB(
                    dbconn=dbconn, name='00_input_data',
                    data_sql_schema=[
                        Column('id', String(), primary_key=True),
                        Column('text', String())
                    ],
                )
            ),
            '01_labeled_data': Table(
                TableStoreDB(
                    dbconn=dbconn, name='01_labeled_data',
                    data_sql_schema=[
                        Column('id', String(), primary_key=True),
                        Column('text', String()),
                        Column('annotations', JSON()),
                    ]
                )
            )
        })
        pipeline = Pipeline([
            LabelStudioStep(
                ls_url=ls_url,
                auth=auth,
                project_identifier=project_title,
                project_label_config_at_create=PROJECT_LABEL_CONFIG_TEST,
                data_sql_schema=[
                    Column('id', String(), primary_key=True),
                    Column('text', String())
                ],
                preannotations_column='preannotations' if include_preannotations else None,
                predictions_column='predictions' if include_predictions else None,
                page_chunk_size=PAGE_CHUNK_SIZE,

                input='00_input_data',
                output='01_labeled_data',

                input_convert_func=wrapped_partial(
                    convert_to_ls_input_data,
                    include_preannotations=include_preannotations,
                    include_predictions=include_predictions
                ),
                output_convert_func=lambda df: df[['id', 'text', 'annotations']],
            ),
        ])
        steps = build_compute(ds, catalog, pipeline)
        label_studio_session = LabelStudioSession(ls_url=ls_url, auth=auth)
        wait_until_label_studio_is_up(label_studio_session)
        label_studio_session.login()

        yield ds, catalog, steps, project_title, include_preannotations, label_studio_session

        project_id = label_studio_session.get_project_id_by_title(project_title)
        if project_id is not None:
            label_studio_session.delete_project(project_id=project_id)


@parametrize_with_cases('ds, catalog, steps, project_title, include_preannotations, label_studio_session',
                        cases=CasesLabelStudio)
def test_label_studio_moderation(
    ds: DataStore, catalog: Catalog, steps: List[DatatableTransformStep],
    project_title: str, include_preannotations: bool,
    label_studio_session: LabelStudioSession
):
    # This should be ok (project will be created, but without data)
    run_steps(ds, steps)
    run_steps(ds, steps)

    # These steps should upload tasks
    batch_generate_wrapper(
        func=gen_data_df,
        ds=ds,
        output_dts=[catalog.get_datatable(ds, '00_input_data')],
    )
    run_steps(ds, steps)

    assert len(catalog.get_datatable(ds, '01_labeled_data').get_data()) == TASKS_COUNT

    # Проверяем проверку на заливку уже размеченных данных
    if include_preannotations:
        assert len(catalog.get_datatable(ds, '01_labeled_data').get_data()) == TASKS_COUNT
        df_annotation = catalog.get_datatable(ds, '01_labeled_data').get_data()
        for idx in df_annotation.index:
            assert len(df_annotation.loc[idx, 'annotations']) == 1
            assert df_annotation.loc[idx, 'annotations'][0]['result'][0]['value']['choices'][0] in (
                ["Class1_annotation", "Class2_annotation"]
            )

    # Person annotation imitation & incremental processing
    project_id = label_studio_session.get_project_id_by_title(project_title)
    assert project_id is not None

    tasks_res = label_studio_session.get_tasks(project_id=project_id, page_size=-1)
    tasks = np.array(tasks_res)
    for idxs in [[0, 3, 6, 7, 9], [1, 2, 4, 5, 8]]:
        for task in tasks[idxs]:
            label_studio_session.add_annotation_to_task(
                task_id=task['id'],
                result=[
                    {
                        "value": {
                            "choices": [np.random.choice(["Class1", "Class2"])]
                        },
                        "from_name": "label",
                        "to_name": "text",
                        "type": "choices"
                    }
                ]
            )
        run_steps(ds, steps)
        idxs_df = pd.DataFrame.from_records(
            {
                'id': [task['data']['id'] for task in tasks[idxs]]
            }
        )
        df_annotation = catalog.get_datatable(ds, '01_labeled_data').get_data(idx=data_to_index(idxs_df, ['id']))
        for idx in df_annotation.index:
            assert len(df_annotation.loc[idx, 'annotations']) == (1 + include_preannotations)
            assert df_annotation.loc[idx, 'annotations'][0]['result'][0]['value']['choices'][0] in (
                ["Class1", "Class2"]
            )


@parametrize_with_cases('ds, catalog, steps, project_title, include_preannotations, label_studio_session',
                        cases=CasesLabelStudio)
def test_label_studio_when_data_is_changed(
    ds: DataStore, catalog: Catalog, steps: List[DatatableTransformStep],
    project_title: str, include_preannotations: bool,
    label_studio_session: LabelStudioSession
):
    df1 = pd.DataFrame(
        {
            'id': [f'task_{i}' for i in range(TASKS_COUNT)],
            'text': (
                ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'] + ['a'] * (TASKS_COUNT % 10)
            ) * (TASKS_COUNT // 10)
        }
    )

    df2 = pd.DataFrame(
        {
            'id': [f'task_{i}' for i in range(TASKS_COUNT)],
            'text': (
                ['A', 'B', 'C', 'd', 'E', 'f', 'G', 'h', 'I', 'j'] + ['a'] * (TASKS_COUNT % 10)
            ) * (TASKS_COUNT // 10)
        }
    )

    def _gen():
        yield df1

    def _gen2():
        yield df2

    # Upload tasks
    batch_generate_wrapper(
        func=_gen,
        ds=ds,
        output_dts=[catalog.get_datatable(ds, '00_input_data')],
    )
    run_steps(ds, steps)

    # These steps should delete old tasks and create new tasks with same ids
    batch_generate_wrapper(
        func=_gen2,
        ds=ds,
        output_dts=[catalog.get_datatable(ds, '00_input_data')],
    )
    run_steps(ds, steps)

    project_id = label_studio_session.get_project_id_by_title(project_title)
    assert project_id is not None

    tasks = label_studio_session.get_tasks(project_id=project_id, page_size=-1)
    assert len(tasks) == TASKS_COUNT

    df = catalog.get_datatable(ds, '01_labeled_data').get_data()[['id', 'text']].sort_values(
        by='id', key=lambda x: pd.Series([int(y) for y in x.str.split('_').str[-1]])
    ).reset_index(
        drop=True
    )
    assert_df_equal(df, next(_gen2()))


@parametrize_with_cases('ds, catalog, steps, project_title, include_preannotations, label_studio_session',
                        cases=CasesLabelStudio)
def test_label_studio_when_some_data_is_deleted(
    ds: DataStore, catalog: Catalog, steps: List[DatatableTransformStep],
    project_title: str, include_preannotations: bool,
    label_studio_session: LabelStudioSession
):
    # These steps should upload tasks
    data_df = next(gen_data_df())
    data_df2 = data_df.set_index('id').drop(index=[f'task_{i}' for i in [0, 3, 5, 7, 9]]).reset_index()

    def _gen():
        yield data_df

    def _gen2():
        yield data_df2

    batch_generate_wrapper(
        func=_gen,
        ds=ds,
        output_dts=[catalog.get_datatable(ds, '00_input_data')],
    )
    run_steps(ds, steps)

    # Change 5 input elements
    batch_generate_wrapper(
        func=_gen2,
        ds=ds,
        output_dts=[catalog.get_datatable(ds, '00_input_data')],
    )

    # These steps should delete tasks with same id accordingly, as data input has changed
    run_steps(ds, steps)

    project_id = label_studio_session.get_project_id_by_title(project_title)
    assert project_id is not None

    tasks = label_studio_session.get_tasks(project_id=project_id, page_size=-1)
    assert len(tasks) == TASKS_COUNT - 5

    df = catalog.get_datatable(ds, '01_labeled_data').get_data()[['id', 'text']].sort_values(
        by='id', key=lambda x: pd.Series([int(y) for y in x.str.split('_').str[-1]])
    ).reset_index(
        drop=True
    )
    assert_df_equal(df, data_df2)


@parametrize_with_cases('ds, catalog, steps, project_title, include_preannotations, label_studio_session',
                        cases=CasesLabelStudio)
def test_label_studio_specific_updating_scenary(
    ds: DataStore, catalog: Catalog, steps: List[DatatableTransformStep],
    project_title: str, include_preannotations: bool,
    label_studio_session: LabelStudioSession
):
    df1 = pd.DataFrame(
        {
            'id': [f'task_{i}' for i in range(5)],
            'text': ['a', 'b', 'c', 'd', 'e']
        }
    )

    df2 = pd.DataFrame(
        {
            'id': [f'task_{i}' for i in range(5)],
            'text': ['A', 'B', 'C', 'd', 'e']
        }
    )

    def _gen():
        yield df1

    def _gen2():
        yield df2

    batch_generate_wrapper(
        func=_gen,
        ds=ds,
        output_dts=[catalog.get_datatable(ds, '00_input_data')],
    )
    run_steps(ds, steps)

    # Change 3 input elements
    batch_generate_wrapper(
        func=_gen2,
        ds=ds,
        output_dts=[catalog.get_datatable(ds, '00_input_data')],
    )

    # Add 5 annotations
    project_id = label_studio_session.get_project_id_by_title(project_title)
    assert project_id is not None
    tasks_res = label_studio_session.get_tasks(project_id=project_id, page_size=-1)
    tasks_ndarr = np.array(tasks_res)
    for task in tasks_ndarr:
        label_studio_session.add_annotation_to_task(
            task_id=task['id'],
            result=[
                {
                    "value": {
                        "choices": [np.random.choice(["Class1", "Class2"])]
                    },
                    "from_name": "label",
                    "to_name": "text",
                    "type": "choices"
                }
            ]
        )

    # Табличка с лейбел студией должна обновиться
    run_steps(ds, steps)

    tasks_ls = label_studio_session.get_tasks(project_id=project_id, page_size=-1)
    assert len(tasks_ls) == 5

    df_ls = catalog.get_datatable(ds, '01_labeled_data').get_data()

    assert_df_equal(
        df2,
        df_ls.drop(columns=['annotations']).sort_values(by='id').reset_index(drop=True)
    )
    if not include_preannotations:
        df_ls = df_ls.set_index('id')
        for idx in df_ls.index:
            if idx in ['task_0', 'task_1', 'task_2']:
                assert df_ls.loc[idx, 'annotations'] == []
            else:
                assert df_ls.loc[idx, 'annotations'] != []


@pytest.mark.skipif(True, reason="Full synchronization is not yet supported")
@parametrize_with_cases('ds, catalog, steps, project_title, include_preannotations, label_studio_session',
                        cases=CasesLabelStudio)
def test_label_studio_when_project_somewhat_deleted(
    ds: DataStore, catalog: Catalog, steps: List[DatatableTransformStep],
    project_title: str, include_preannotations: bool,
    label_studio_session: LabelStudioSession
):
    # These steps should upload tasks
    batch_generate_wrapper(
        func=gen_data_df,
        ds=ds,
        output_dts=[catalog.get_datatable(ds, '00_input_data')],
    )
    run_steps(ds, steps)

    # Delete the project
    project_id = label_studio_session.get_project_id_by_title(project_title)
    assert project_id is not None

    label_studio_session.delete_project(project_id)

    # These steps should upload tasks from beginning
    run_steps(ds, steps)

    new_project_id = label_studio_session.get_project_id_by_title(project_title)
    assert new_project_id is not None

    tasks = label_studio_session.get_tasks(project_id=new_project_id, page_size=-1)
    assert len(tasks) == TASKS_COUNT
