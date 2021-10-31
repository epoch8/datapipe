from typing import Any, Dict, List, Tuple, Union, Optional
from urllib.parse import urljoin

import requests


class LabelStudioSession:
    def __init__(
        self,
        ls_url: str,
        auth: Union[Tuple[str, str], str]  # (username, password) or 'Token <token>'
    ):
        self.ls_url = ls_url
        assert isinstance(auth, tuple) or isinstance(auth, str)
        self.auth = auth
        self.session = requests.Session()
        if isinstance(auth, str):
            self.session.headers['Authorization'] = auth

    def login(self) -> int:
        is_auth_ok = self.is_auth_ok()
        if not is_auth_ok and isinstance(self.auth, tuple):
            username, password = self.auth
            response = self.session.get(
                url=urljoin(self.ls_url, 'user/login/')
            )
            self.session.post(
                url=urljoin(self.ls_url, 'user/login/'),
                data={
                    'csrfmiddlewaretoken': response.cookies['csrftoken'],
                    'email': username,
                    'password': password
                }
            )
            is_auth_ok = self.is_auth_ok()
            if is_auth_ok:
                self.get_current_token()

        return is_auth_ok

    def is_auth_ok(self, raise_exception: bool = False) -> bool:
        response = self.session.get(
            url=urljoin(self.ls_url, 'api/current-user/whoami')
        )
        if not response.ok and raise_exception:
            raise ValueError(f'Authorization failed: {response.json()}')
        return response.ok

    def get_current_token(self) -> str:
        if 'Authorization' not in self.session.headers:
            token = self.session.get(
                url=urljoin(self.ls_url, 'api/current-user/token')
            ).json()
            self.session.headers['Authorization'] = token['token']
        return self.session.headers['Authorization']

    def sign_up(self):
        username, password = self.auth
        response = self.session.get(
            url=urljoin(self.ls_url, 'user/signup/')
        )
        response_signup = self.session.post(
            url=urljoin(self.ls_url, 'user/signup/'),
            data={
                'csrfmiddlewaretoken': response.cookies['csrftoken'],
                'email': username,
                'password': password
            }
        )
        if not response_signup.ok or not self.is_auth_ok(raise_exception=False):
            raise ValueError('Signup failed.')

    def get_project(self, project_id: Union[int, str]) -> Dict[str, Any]:
        return self.session.get(
            urljoin(self.ls_url, f'api/projects/{project_id}/')
        ).json()

    def create_project(self, project_setting: Dict[str, Any]) -> Dict[str, Any]:
        return self.session.post(
            urljoin(self.ls_url, 'api/projects/'),
            json=project_setting
        ).json()

    def delete_project(self, project_id: str):
        return self.session.delete(
            urljoin(self.ls_url, f'api/projects/{project_id}/')
        )

    def get_project_id_by_title(
        self,
        title: str
    ) -> Optional[str]:
        projects = self.session.get(urljoin(self.ls_url, 'api/projects/')).json()
        project_ids = [project['id'] for project in projects]
        titles = [project['title'] for project in projects]
        if title in titles:
            assert titles.count(title) == 1, f'There are 2 or more tasks with title="{title}"'
            return project_ids[titles.index(title)]

        return None

    def upload_tasks(
        self,
        data: List[Dict[str, Any]],
        project_id: Union[int, str]
    ) -> Dict:
        results = self.session.post(
            url=urljoin(self.ls_url, f'api/projects/{project_id}/tasks/bulk/'),
            json=data
        ).json()
        return results

    # массовая заливка задач не дает нам идентификаторов, поэтому придется пользоваться
    # заливкой по одному.
    def upload_task(
        self,
        data: Dict[str, Any],
        project_id: Union[int, str]
    ) -> Dict:
        results = self.session.post(
            url=urljoin(self.ls_url, 'api/tasks/'),
            json={
                'data': data,
                'project': project_id,
            }
        ).json()

        return results

    def modify_task(
        self,
        task_id: Union[int, str],
        data: Dict
    ) -> Dict:
        results = self.session.patch(
            url=urljoin(self.ls_url, f'api/tasks/{task_id}/'),
            json=data
        ).json()
        return results

    def delete_task(
        self,
        task_id: Union[int, str]
    ) -> bool:
        result = self.session.delete(
            url=urljoin(self.ls_url, f'api/tasks/{task_id}/')
        )
        return result.ok

    def delete_tasks(
        self,
        project_id: Union[int, str],
        tasks_ids: List[str]
    ) -> bool:
        # If empty, API will delete all tasks
        if len(tasks_ids) == 0:
            tasks_ids = ['-1']

        result = self.session.post(
            url=urljoin(self.ls_url, f'api/dm/actions?id=delete_tasks&project={project_id}'),
            json={
                "selectedItems": {
                    "all": False,
                    "included": tasks_ids
                }
            }
        )
        return result.json()

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
        task_id: Union[int, str],
        result: Any,
    ) -> Dict:
        result = self.session.post(
            url=urljoin(self.ls_url, f'api/tasks/{task_id}/annotations/'),
            json={
                'result': result,
                'was_cancelled': False,
                'task': task_id
            }
        ).json()

        return result

    def get_tasks(
        self,
        project_id: Union[int, str],
        page: int = 1,  # current page
        page_size: int = -1,  # tasks per page, use -1 to obtain all tasks
    ) -> List[Dict[str, Any]]:
        response = self.session.get(
            url=urljoin(self.ls_url, f'api/projects/{project_id}/tasks/'),
            params={
                'page': page,
                'page_size': page_size
            }
        )

        assert response.status_code in [200, 500], f"Something wrong with GET: {response.content=}"
        if response.status_code == 500:
            return []

        return response.json()

    def get_project_summary(
        self,
        project_id: Union[int, str]
    ) -> Dict[str, Any]:
        summary = self.session.get(urljoin(self.ls_url, f'api/projects/{project_id}/summary/')).json()

        return summary

    def get_all_views(
        self
    ) -> List[Dict[str, Any]]:
        views = self.session.get(
            url=urljoin(self.ls_url, 'api/dm/views/')
        )
        return views.json()

    def create_view(
        self,
        project_id: Union[int, str],
        data: Dict[str, Any] = {}
    ) -> Dict[str, Any]:
        result = self.session.post(
            url=urljoin(self.ls_url, 'api/dm/views/'),
            json={
                'project': project_id,
                'data': data
            }
        )
        return result.json()

    def get_all_tasks_from_view(
        self,
        view_id: Union[int, str],
        page: int,
        page_size: int,
        **params
    ) -> List[Dict[str, Any]]:
        result = self.session.get(
            url=urljoin(self.ls_url, f'api/dm/views/{view_id}/tasks'),
            params={
                'page': page,
                'page_size': page_size,
                **params
            }
        ).json()
        if 'tasks' in result:
            return result['tasks']
        else:
            return []

    def delete_view(
        self,
        view_id: Union[int, str]
    ) -> Dict[str, Any]:
        result = self.session.delete(
            url=urljoin(self.ls_url, f'api/dm/views/{view_id}/')
        )
        return result.json()
