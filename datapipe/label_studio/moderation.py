import time
from multiprocessing import Process
from typing import Dict
from urllib.parse import urljoin
import pandas as pd

import requests

from datapipe.label_studio.run_server import LabelStudioConfig, start_label_studio_app


class LabelStudioModeration:
    def __init__(
        self,
        label_studio_config: LabelStudioConfig,
        project_setting: Dict[str, str]
    ):
        self.label_studio_config = label_studio_config
        self.project_setting = project_setting
        self.session = requests.Session()
        self.session.auth = (label_studio_config.username, label_studio_config.password)
        self.ls_url = f'http://{label_studio_config.internal_host}:{label_studio_config.port}/'
        self.label_studio_service: Process = None

    def create_or_get_project(self) -> Dict:
        projects = self.session.get(urljoin(self.ls_url, '/api/projects/')).json()
        if not projects:
            project = self.session.post(
                urljoin(self.ls_url, '/api/projects/'),
                json=self.project_setting
            ).json()
            return project
        else:
            return projects[0]

    def run_services(self):
        self.label_studio_service = Process(
            target=start_label_studio_app,
            kwargs={
                'label_studio_config': self.label_studio_config
            }
        )
        self.label_studio_service.start()

        while True:
            try:
                self.session.head(self.ls_url)
                break
            except requests.exceptions.ConnectionError:
                time.sleep(2.)

    def terminate_services(self):
        self.label_studio_service.terminate()
        self.label_studio_service.join()

    def __del__(self):
        if self.is_running():
            self.terminate_services()

    def is_running(self):
        return self.label_studio_service is not None

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

    def get_output_df(
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
