import asyncio
import time


from datapipe_router.types import RUN_STATUSES
from dataclasses import dataclass


@dataclass
class StatusEvent:
    run_id: str
    timestamp: float
    status: str


@dataclass
class LogEvent:
    run_id: str
    timestamp: float
    sequence: int
    log: str



class BaseRunner:
    def __init__(
            self, 
            run_id: str, 
            status_queue: asyncio.Queue, 
            log_queue: asyncio.Queue,
            labels: list[tuple[str, str]] = [],
        ):
        self.run_id = run_id
        self.labels = labels
        self.status_queue = status_queue
        self.log_queue = log_queue
        self.sequence = 0

        self.status = RUN_STATUSES.CREATED

    def _check_label(self, key: str, value: str) -> bool:
        # TODO: Проверять на корректность вставляемых данных и проверку на уязвимости
        return True

    def get_command(self):
        command = "datapipe run"
        labels = [ f"{k}={v}" for k,v in self.labels if self._check_label(k, v)]
         
        if labels: 
            command = f'{command} --labels {",".join(labels)}'
        print(command)
        return command.split(" ")

    async def set_status(self, status: RUN_STATUSES):
        self.status = status

        await self.status_queue.put(
            StatusEvent(
                run_id=self.run_id,
                timestamp=time.time(),
                status=status.value,
            )
        )

    async def set_log(self, log: str):
        self.sequence += 1 

        await self.log_queue.put(
            LogEvent(
                run_id=self.run_id,
                timestamp=time.time(),
                sequence=self.sequence,
                log=log,
            )
        )

    async def init(self):
        raise NotImplementedError()

    async def run(self):
        raise NotImplementedError()