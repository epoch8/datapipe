import asyncio
import sys
import os
import logging

from datapipe_agent.runners.base import BaseRunner
from datapipe_router.types import RUN_STATUSES


logger = logging.getLogger("datapipe_agent")


class LocalRunner(BaseRunner):
    def __init__(
        self, 
        run_id: str, 
        status_queue: asyncio.Queue, 
        log_queue: asyncio.Queue,
        labels: list[tuple[str, str]] = None,
    ):
        super().__init__(run_id, status_queue, log_queue, labels=labels)

    async def read_stream(self, stream):
        while True:
            line = await stream.readline()

            if not line:
                break

            await self.set_log(line.decode().strip())

    async def init(self):
        pass

    async def run(self):
        logger.info(f"({self.run_id}) Pipeline runned")

        await self.set_status(RUN_STATUSES.PENDING)
        await self.set_log("Starting datapipe process")

        datapipe_path = os.path.join(os.path.dirname(sys.executable), "datapipe")

        proc = await asyncio.create_subprocess_exec(
            datapipe_path, "run",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )

        await self.set_status(RUN_STATUSES.RUNNING)
        await self.set_log("Started datapipe process")

        log_task = asyncio.create_task(self.read_stream(proc.stdout))

        await proc.wait()

        log_task.cancel()

        await self.set_status(RUN_STATUSES.FINISHED)
        await self.set_log("Finished datapipe process")

