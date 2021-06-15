from dataclasses import dataclass
from typing import Dict, List, Tuple, Union
from urllib.parse import urljoin

import pandas as pd
import requests

from datapipe.datatable import gen_process_many, inc_process_many
from datapipe.metastore import MetaStore
from datapipe.step import ComputeStep

from tqdm import tqdm


class LabelStudioSession:
    def __init__(
        self,
        ls_url: str,
        auth: Tuple[str, str]
    ):
        self.ls_url = ls_url
        self.session = requests.Session()
        self.session.auth = auth

    def is_auth_ok(self, raise_exception: bool) -> bool:
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

    def delete_project(self, project_id: str):
        return self.session.delete(
            urljoin(self.ls_url, f'/api/projects/{project_id}/')
        )

    def get_project_id_by_title(
        self,
        title: str
    ) -> Dict[str, str]:
        projects = self.session.get(urljoin(self.ls_url, '/api/projects/')).json()
        project_ids = [project['id'] for project in projects]
        titles = [project['title'] for project in projects]
        if title in titles:
            assert titles.count(title) == 1, f'There are 2 or more tasks with title="{title}"'
            return project_ids[titles.index(title)]

        return None

    def upload_tasks(
        self,
        data: Dict,
        project_id: str
    ) -> Dict:
        results = self.session.post(
            url=urljoin(self.ls_url, f'/api/projects/{project_id}/tasks/bulk/'),
            json=data
        ).json()
        return results

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
    ) -> Dict:
        result = self.session.post(
            url=urljoin(self.ls_url, f'/api/tasks/{task_id}/annotations/'),
            json={
                'result': result,
                'was_cancelled': False,
                'task': task_id
            }
        ).json()

        return result

    def get_tasks(
        self,
        project_id: str,
        page: int = 1,  # current page
        page_size: int = -1  # tasks per page, use -1 to obtain all tasks
    ) -> Tuple[Dict, int]:
        response = self.session.get(
            url=urljoin(self.ls_url, f'/api/projects/{project_id}/tasks/'),
            params={
                'page': page,
                'page_size': page_size
            }
        )

        return response.json(), response.status_code

    def get_project_summary(
        self,
        project_id: str
    ) -> Dict[str, str]:
        summary = self.session.get(urljoin(self.ls_url, f'/api/projects/{project_id}/summary/')).json()

        return summary


@dataclass
class LabelStudioModerationStep(ComputeStep):
    ls_url: str
    chunk_size: int
    auth: Tuple[str, str]
    project_title: str
    project_description: str
    project_label_config: str
    data: List[str]
    annotations: Union[str, None]
    predictions: Union[str, None]

    def __post_init__(self):
        self.label_studio_session = LabelStudioSession(
            ls_url=self.ls_url,
            auth=self.auth
        )
        if self.label_studio_session.is_service_up():

            # Authorize or sign up
            if not self.label_studio_session.is_auth_ok(raise_exception=False):
                self.label_studio_session.sign_up()
                self.label_studio_session.is_auth_ok(raise_exception=True)

            self.project_id = self.label_studio_session.get_project_id_by_title(self.project_title)
            if self.project_id is None:
                project = self.label_studio_session.create_project(
                    project_setting={
                        "title": self.project_title,
                        "description": self.project_description,
                        "label_config": self.project_label_config,
                        "expert_instruction": "",
                        "show_instruction": False,
                        "show_skip_button": False,
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
                )
                self.project_id = project['id']
        else:
            self.project_id = None

    def upload_tasks_from_df(
        self,
        input_df: pd.DataFrame
    ):
        data = [
            {
                'data': {
                    'LabelStudioModerationStep__unique_id': idx,
                    **{
                        column: input_df.loc[idx, column]
                        for column in self.data
                    }
                },
                'annotations': input_df.loc[idx, self.annotations] if self.annotations is not None else [],
                'predictions': input_df.loc[idx, self.predictions] if self.predictions is not None else [],
            }
            for idx in input_df.index
        ]
        self.label_studio_session.upload_tasks(data=data, project_id=self.project_id)
        input_df['tasks_id'] = ["Unknown" for _ in input_df.index]
        input_df['annotations'] = input_df[self.annotations] if self.annotations is not None else [
            [] for _ in input_df.index
        ]
        input_df = input_df[['tasks_id', 'annotations']]
        return input_df

    def get_current_tasks_as_df(self):
        project_summary = self.label_studio_session.get_project_summary(self.project_id)
        if 'all_data_columns' not in project_summary:
            total_tasks_count = 0
        else:
            total_tasks_count = project_summary['all_data_columns']['LabelStudioModerationStep__unique_id']

        total_pages = total_tasks_count // self.chunk_size + 1

        # created_ago - очень плохой параметр, он меняется каждый раз, когда происходит запрос
        def _cleanup_annotations(annotations):
            for ann in annotations:
                if 'created_ago' in ann:
                    del ann['created_ago']
            return annotations

        for page in tqdm(range(1, total_pages + 1), desc='Getting tasks from Label Studio Projects...'):
            tasks_page, status_code = self.label_studio_session.get_tasks(
                project_id=self.project_id,
                page=page,
                page_size=self.chunk_size
            )
            assert status_code in [200, 500]
            if status_code == 500:
                break

            output_df = pd.DataFrame(
                data={
                    'tasks_id': [str(task['id']) for task in tasks_page],
                    'annotations': [_cleanup_annotations(task['annotations']) for task in tasks_page]
                },
                index=[task['data']['LabelStudioModerationStep__unique_id'] for task in tasks_page]
            )

            yield output_df

    def run(self, ms: MetaStore) -> None:
        if self.label_studio_session.is_service_up():
            if self.project_id is None:
                self.__post_init__()

            # Upload Tasks from inputs to outputs
            inc_process_many(
                ms,
                self.input_dts,
                self.output_dts,
                self.upload_tasks_from_df,
                self.chunk_size
            )
            # Update current annotations in outputs
            gen_process_many(
                self.output_dts,
                self.get_current_tasks_as_df
            )
