import os

from typing import Optional, Self
from dataclasses import dataclass
from enum import Enum


class RunnerType(Enum):
    LOCAL = "local"
    KUBERNATES = "kubernates"


@dataclass
class ServicePath:
    host: str
    port: str


@dataclass
class KubernatesConfig:
    namespace: str
    job_prefix: str
    image: str 
    tag: str


@dataclass
class AgentSettings:
    name: str
    router: ServicePath
    storage: ServicePath
    runner_type: RunnerType
    kubernates: Optional[KubernatesConfig]

    @classmethod
    def from_env(
        cls, 
        name: str = None,
        router_host: str = "localhost",
        router_port: int = 10500,
        storage_host: str = "localhost",
        storage_port: int = 10600,
        runner_type: str = RunnerType.LOCAL.value,
        namespace: str = "default",
        job_prefix = "datapie-run",
        image: str = "",
        tag: str = "",
    ) -> Self:
        agent_name = os.environ.get("DATAPIPE_ROUTER_HOST", name)
        router = ServicePath(
            host=os.environ.get("DATAPIPE_ROUTER_HOST", router_host),
            port=os.environ.get("DATAPIPE_ROUTER_PORT", str(router_port)),
        )

        storage = ServicePath(
            host=os.environ.get("DATAPIPE_STORAGE_HOST", storage_host),
            port=os.environ.get("DATAPIPE_STORAGE_PORT", str(storage_port)),
        )

        runner_type = RunnerType(os.environ.get("AGENT_RUNNER_TYPE", runner_type))
        kubernates = None

        if runner_type == RunnerType.KUBERNATES:
            kubernates = KubernatesConfig(
                namespace=os.environ.get("AGENT_KUBE_NAMESPACE", namespace),
                image=os.environ.get("AGENT_KUBE_IMAGE", image),
                tag=os.environ.get("AGENT_KUBE_TAG", tag),
                job_prefix=os.environ.get("AGENT_KUBE_JOB_PREFIX", job_prefix),
            )

        return cls(
            name=agent_name,
            router=router,
            storage=storage,
            runner_type=runner_type,
            kubernates=kubernates
        )