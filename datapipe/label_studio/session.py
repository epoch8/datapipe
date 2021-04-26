import logging

from dataclasses import dataclass
import time
from multiprocessing import Process
from typing import Dict
from urllib.parse import urljoin

from datapipe.datatable import gen_process_many, inc_process_many
from datapipe.metastore import MetaStore
from datapipe.step import ComputeStep
import pandas as pd

import requests

from datapipe.label_studio.run_server import LabelStudioConfig, start_label_studio_app


logger = logging.getLogger('datapipe.label_studio.service')


class LabelStudioSession:
    def __init__(
        self,
        label_studio_config: LabelStudioConfig,
    ):
        self.label_studio_config = label_studio_config
        self.session = requests.Session()
        self.session.auth = (label_studio_config.username, label_studio_config.password)
        self.ls_url = f'http://{label_studio_config.internal_host}:{label_studio_config.port}/'
        self.label_studio_process: Process = Process(
            target=start_label_studio_app,
            kwargs={
                'label_studio_config': label_studio_config
            }
        )
        self.label_studio_process.start()

        # Wait until Label Studio is up
        while True:
            try:
                self.session.head(self.ls_url)
                break
            except requests.exceptions.ConnectionError:
                logger.info(
                    f'Waiting until connect to {self.ls_url} (Label Studio) success.'
                )
                time.sleep(2.)

    def __del__(self):
        self.label_studio_process.terminate()
        self.label_studio_process.join()

    def get_project(self, project_id: str) -> Dict[str, str]:
        return self.session.get(
            urljoin(self.ls_url, f'/api/projects/{project_id}/')
        ).json()

    def create_project(self, project_setting: Dict[str, str]) -> Dict[str, str]:
        return self.session.post(
            urljoin(self.ls_url, '/api/projects/'),
            json=project_setting
        ).json()

    def get_project_id_by_title(
        self,
        title: str
    ) -> Dict[str, str]:
        projects = self.session.get(urljoin(self.ls_url, '/api/projects/')).json()
        project_ids = [project['id'] for project in projects]
        titles = [project['title'] for project in projects]
        if title in titles:
            return project_ids[titles.index(title)]

        return None

    def upload_tasks(
        self,
        input_df: pd.DataFrame,
        project_id: str
    ):
        assert 'data' in input_df.columns, "There must be column 'data' in input_df"
        for data in input_df['data']:
            assert 'unique_id' in data, "There must be 'unique_id' in input data (add it to label config)"
        new_tasks = [
            self.session.post(
                url=urljoin(self.ls_url, '/api/tasks/'),
                json={
                    'data': input_df.loc[id, 'data'],
                    'project': project_id
                }
            ).json()
            for id in input_df.index
        ]
        input_df['tasks_id'] = [task['id'] for task in new_tasks]
        input_df['annotations'] = [task['annotations'] for task in new_tasks]
        input_df = input_df[['tasks_id', 'annotations']]
        return input_df

    def get_current_tasks(
        self,
        project_id: str
    ):
        tasks = self.session.get(
            url=urljoin(self.ls_url, f'/api/projects/{project_id}/tasks/'),
        ).json()
        output_df = pd.DataFrame(
            data={
                'tasks_id': [task['id'] for task in tasks],
                'annotations': [task['annotations'] for task in tasks]
            },
            index=[task['data']['unique_id'] for task in tasks]
        )

        return output_df


@dataclass
class LabelStudioModerationStep(ComputeStep):
    label_studio_session: LabelStudioSession
    project_setting: Dict[str, str]
    chunk_size: int

    def __post_init__(self):
        self.project_id = self.label_studio_session.get_project_id_by_title(self.project_setting['title'])
        if self.project_id is None:
            project = self.label_studio_session.create_project(self.project_setting)
            self.project_id = project['id']

    def run(self, ms: MetaStore) -> None:
        # Upload Tasks from inputs to outputs
        inc_process_many(
            ms,
            self.input_dts,
            self.output_dts,
            self.label_studio_session.upload_tasks,
            self.chunk_size,
            project_id=self.project_id
        )
        # Update current annotations in outputs
        gen_process_many(
            self.output_dts,
            self.label_studio_session.get_current_tasks,
            project_id=self.project_id
        )
