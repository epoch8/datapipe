import logging

from dataclasses import dataclass
from typing import Dict, Tuple
from urllib.parse import urljoin

from datapipe.datatable import gen_process_many, inc_process_many
from datapipe.metastore import MetaStore
from datapipe.step import ComputeStep
import pandas as pd

import requests

logger = logging.getLogger('datapipe.label_studio.service')


class LabelStudioSession:
    def __init__(
        self,
        ls_url: str,
        auth: Tuple[str, str] = ('admin@epoch8.co', 'qwertyisALICE666')
    ):
        self.ls_url = ls_url
        self.session = requests.Session()
        self.session.auth = auth

    def is_auth_ok(self, raise_exception: bool):
        response = self.session.get(
            url=urljoin(self.ls_url, '/api/current-user/whoami')
        )
        if not response.ok and raise_exception:
            raise ValueError(f'Authorization failed: {response.json()}')
        return response.ok

    def sign_up(self):
        username, password = self.session.auth
        response = self.session.get(
            url=urljoin(self.ls_url, '/user/signup/')
        )
        response_signup = self.session.post(
            url=urljoin(self.ls_url, '/user/signup/'),
            data={
                'csrfmiddlewaretoken': response.cookies['csrftoken'],
                'email': username,
                'password': password
            }
        )
        if not response_signup.ok or not self.is_auth_ok(raise_exception=False):
            raise ValueError('Signup failed.')

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

    def is_service_up(self, raise_exception: bool = False) -> bool:
        try:
            self.session.head(self.ls_url)
            return True
        except requests.exceptions.ConnectionError:
            if raise_exception:
                raise
            else:
                return False

    def add_annotation_to_task(
        self,
        task_id: str,
        result: Dict
    ):
        result = self.session.post(
            url=urljoin(self.ls_url, f'/api/tasks/{task_id}/annotations/'),
            json={
                'result': result,
                'was_cancelled': False,
                'task': task_id
            }
        ).json()

        return result

    def get_all_tasks(
        self,
        project_id: str
    ):
        tasks = self.session.get(
            url=urljoin(self.ls_url, f'/api/projects/{project_id}/tasks/'),
        ).json()

        return tasks


@dataclass
class LabelStudioModerationStep(ComputeStep):
    ls_url: str
    project_setting: Dict[str, str]
    chunk_size: int

    def __post_init__(self):
        self.label_studio_session = LabelStudioSession(self.ls_url)
        if self.label_studio_session.is_service_up():
            if not self.label_studio_session.is_auth_ok(raise_exception=False):
                self.label_studio_session.sign_up()
                self.label_studio_session.is_auth_ok(raise_exception=True)

            self.project_id = self.label_studio_session.get_project_id_by_title(self.project_setting['title'])
            if self.project_id is None:
                project = self.label_studio_session.create_project(self.project_setting)
                self.project_id = project['id']
        else:
            self.project_id = None

    def run(self, ms: MetaStore) -> None:
        if self.label_studio_session.is_service_up():
            if self.project_id is None:
                self.__post_init__()

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
