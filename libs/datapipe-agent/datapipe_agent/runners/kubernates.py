import asyncio
import os
import logging
import yaml

from kubernetes.client.rest import ApiException
from kubernetes.aio import client, config
from kubernetes.aio.client.api_client import ApiClient
from datapipe_router.types import RUN_STATUSES

from datapipe_agent.config import KubernatesConfig
from datapipe_agent.runners.base import BaseRunner
from datapipe_agent.runners.libs.watch import Watch # Delete if kubernates >= 37


logger = logging.getLogger("datapipe_agent")


FINAL_STATES = (RUN_STATUSES.FINISHED, RUN_STATUSES.FAILED,  RUN_STATUSES.UNKNOWN)
FIND_POD_DELAY = 1
CHECK_STATUS_DELAY = 1


class KubernatesRunner(BaseRunner):
    def __init__(
            self, 
            run_id: str, 
            status_queue: asyncio.Queue, 
            log_queue: asyncio.Queue,
            config: KubernatesConfig,
            labels: list[tuple[str, str]] = None,
            envs: dict[str, str] = {},
        ):
        super().__init__(run_id, status_queue, log_queue, labels=labels)

        self.config = config
        self.envs = envs
        self.labels = labels

        self.finished = False
        self.job_name = f"{config.job_prefix}-{run_id}"
        

    async def init(self):
        try:
            await config.load_incluster_config()
        except config.ConfigException:
            await config.load_kube_config()

    def get_manifest(self):
        path = os.path.join(os.getcwd(), "runners" , 'manifests', "job.yaml")
    
        with open(path, 'r') as file:
            manifest = yaml.safe_load(file)

        envs = [{"name": k, "value": v} for k, v in self.envs.items()]

        manifest["metadata"]["name"] = self.job_name
        manifest["spec"]["template"]["spec"]["containers"][0]["image"] = f"{self.config.image};{self.config.tag}"
        manifest["spec"]["template"]["spec"]["containers"][0]["command"] = self.get_command()

        if envs:
            manifest["spec"]["template"]["spec"]["containers"][0]["env"] = envs

        return manifest

    async def wait_job_status(self):
        async with ApiClient() as api:
            batchV1 = client.BatchV1Api(api)

            while True:
                try:
                    job = await batchV1.read_namespaced_job_status(name=self.job_name, namespace=self.config.namespace)
                except ApiException as e:
                    await self.set_status(RUN_STATUSES.UNKNOWN) 
                    await self.set_log(e.body) 
                    self.finished = True
                    break

                if job.status.active or job.status.terminating:
                    status = (
                        RUN_STATUSES.PENDING 
                        if job.status.ready == 0 else
                        RUN_STATUSES.RUNNING
                    )
                elif job.status.completion_time:
                    status = (
                        RUN_STATUSES.FINISHED 
                        if job.status.succeeded > 0 else
                        RUN_STATUSES.FAILED
                    )

                if self.status != status:
                    await self.set_status(status) 

                if status in FINAL_STATES:
                    self.finished = True
                    break

                await asyncio.sleep(CHECK_STATUS_DELAY)

    async def watch_job_logs(self):
        w = Watch()

        async with ApiClient() as api:
            v1 = client.CoreV1Api(api)

            while not self.finished:
                pods = await v1.list_namespaced_pod(namespace=self.config.namespace, label_selector=f"job-name={self.job_name}")
                running_pods = [item for item in pods.items if item.status.phase == "Running"]

                if not running_pods:
                    await asyncio.sleep(FIND_POD_DELAY)
                    continue

                pod_name = running_pods[0].metadata.name

                try: 
                    async for line in w.stream(
                        v1.read_namespaced_pod_log,
                        name=pod_name,
                        namespace=self.config.namespace,
                        follow=True
                    ):
                        await self.set_log(line.strip())

                except Exception as e:
                    pass

                await asyncio.sleep(FIND_POD_DELAY)
             

    async def run(self):
        logger.info(f"({self.run_id}) Pipeline run initialize")

        manifest = self.get_manifest()

        async with ApiClient() as api:
            logger.info(f"({self.run_id}) Pipeline run creating JOB")

            await self.set_log("Starting datapipe process")

            batchV1 = client.BatchV1Api(api)

            try:
                await batchV1.create_namespaced_job(namespace=self.config.namespace, body=manifest)

                status_task = asyncio.create_task(self.wait_job_status())
                log_task = asyncio.create_task(self.watch_job_logs())

                await asyncio.gather(status_task, log_task)
                await self.set_log("Datapipe process finished")
            except ApiException as e:
                await self.set_status(RUN_STATUSES.FAILED)
                await self.set_log(str(e.body))
            except Exception as e:
                await self.set_status(RUN_STATUSES.FAILED)
                await self.set_log(str(e))
            