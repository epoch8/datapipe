import asyncio
import sys
import os
import logging

from datapipe_agent.runners.base import StatusEvent, LogEvent, RUN_STATUSES


logger = logging.getLogger("datapipe_cloud.agent")


class LocalRunner:
    def __init__(self, run_id: str, status_queue: asyncio.Queue, log_queue: asyncio.Queue):
        self.run_id = run_id
        self.status_queue = status_queue
        self.log_queue = log_queue

        self.status = RUN_STATUSES.CREATED

    async def set_status(self, status: RUN_STATUSES):
        self.status = status

        await self.status_queue.put(
            StatusEvent(
                run_id=self.run_id,
                status=status.value
            )
        )

    async def set_log(self, log: str):
        await self.log_queue.put(
            LogEvent(
                run_id=self.run_id,
                log=log
            )
        )

    async def read_stream(self, stream):
        while True:
            line = await stream.readline()

            if not line:
                break

            await self.set_log(line.decode().strip())

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

