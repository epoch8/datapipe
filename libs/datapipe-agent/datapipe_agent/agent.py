import asyncio
import signal
import grpc
import io
import logging
import pandas as pd
import time

from datapipe.compute import DatapipeApp

import datapipe_router.pb2.agent_pb2 as agent_messages
import datapipe_router.pb2.agent_pb2_grpc as agent_messages_grpc

from datapipe_router.types import DataFilter
from datapipe_agent.runners.base import StatusEvent, LogEvent
from datapipe_agent.runners.local import LocalRunner
from datapipe_agent.libs.graph import get_pipeline_graph
from datapipe_agent.libs.table_data import get_table_data, TableDataSettings

CHANNEL_OPTIONS = [
    ('grpc.keepalive_time_ms', 30000),             # Send pings every 30 seconds if idle
    ('grpc.keepalive_timeout_ms', 10000),          # Wait 10 seconds for ping response
    ('grpc.keepalive_permit_without_calls', 1),    # Allow pings even if there are no active streams
    ('grpc.http2.max_pings_without_data', 0),      # Unlimited pings without sending data
]

RECONNECT_DELAY_SECONDS = 5


logger = logging.getLogger("datapipe_cloud.agent")


class DatapipeAgent(DatapipeApp):
    def __init__(self, app: DatapipeApp, name: str, server_host: str, server_port: int):
        self.app = app
        self.name = name
        self.server_host = server_host
        self.server_port = server_port

        self._channel = None
        self._stub = None
        self._runns = {}
        self._status_queue = asyncio.Queue()
        self._logs_queue = asyncio.Queue()

    def _get_grpc_stub(self):
        if self._channel is None:
            url = f'{self.server_host}:{self.server_port}'
            self._channel = grpc.aio.insecure_channel(url, options=CHANNEL_OPTIONS)
            self._stub = agent_messages_grpc.DatapipeServiceStub(self._channel)

        return self._stub

    async def send_data(self, request_id: str, data: agent_messages.DataEvent):
        request = data.request

        if request.filters:
            try:
                filters = DataFilter.from_bytes(request.filters).data
            except:
                logger.error(f"({request_id}) Incorrect filters data...")
                return
        else: 
            filters = {}

        if request.focus:
            try:
                focus = [
                    DataFilter.from_bytes(item).data
                    for item in request.focus
                ] 
            except:
                logger.error(f"({request_id}) Incorrect focus data...")
                return
        else: 
            focus = []

        settings = TableDataSettings(
            page=data.request.page,
            page_size=data.request.page_size,
            include_total=data.request.include_total,
            order=data.request.order,
            order_by=data.request.order_by,
            filters=filters,
            focus=focus
        )

        table_data = get_table_data(self.app.ds, self.app.catalog, request.table, settings)

        stub = self._get_grpc_stub()
        request = agent_messages.SendDataRequest(
            route_id=data.route_id,
            data=table_data.to_message()
        )

        await stub.SendData(request)

    async def send_graph(self, request_id: str, data: agent_messages.GraphEvent):
        labels = {data.label_key: data.value} if data.value else {}
        start = time.time()
        graph = get_pipeline_graph(self.app, labels)
        logger.info(f"({request_id}) Graph generated: {time.time() - start} s")
        
        stub = self._get_grpc_stub()
        request = agent_messages.SendGraphRequest(
            route_id=data.route_id,
            data=graph.to_message()
        )

        await stub.SendGraph(request)

    async def run_status_handler(self):
        while True:
            event: StatusEvent = await self._status_queue.get()

            if event:
                logger.info(f"({event.run_id}) Pipeline run status: {event.status}")

                stub = self._get_grpc_stub()
                request = agent_messages.SendRunStatusRequest(
                    run_id=event.run_id,
                    status=event.status,
                )
        
                await stub.SendRunStatus(request)

    async def run_logs_handler(self):
        while True:
            event: LogEvent = await self._logs_queue.get()

            if event:
                stub = self._get_grpc_stub()
                request = agent_messages.SendLogsRequest(
                    run_id=event.run_id,
                    log=event.log,
                )
        
                await stub.SendLogs(request)
                await asyncio.sleep(5)

    async def run_pipeline(self, request_id: str, data: agent_messages.RunEvent):
        logger.info(f"({data.run_id}) Pipiline running...")

        runner = LocalRunner(data.run_id, self._status_queue, self._logs_queue)

        self._runns[data.run_id] = runner

        await runner.run()


    async def process_command(self, command: agent_messages.ServerEventsResponse):
        active_field = command.WhichOneof("event")

        if active_field == "ping_event":
            return

        logger.info(f"Received server command [{command.request_id}]")

        if active_field == "data_event":
            asyncio.create_task(self.send_data(command.request_id, command.data_event))
        elif active_field == "graph_event":
            asyncio.create_task(self.send_graph(command.request_id, command.graph_event))
        elif active_field == "run_event":
            asyncio.create_task(self.run_pipeline(command.request_id, command.run_event))
        else:
            logger.warning("Command not found.")

    async def run_agent(self):
        logger.info(f"Datapipe agent starting...")
        self._loop = asyncio.get_running_loop()
            
        # Register OS signals for clean termination (SIGINT = Ctrl+C, SIGTERM = systemctl stop)
        for sig in (signal.SIGINT, signal.SIGTERM):
            self._loop.add_signal_handler(
                sig, 
                lambda s=sig: asyncio.create_task(self.shutdown(s))
            )

        asyncio.create_task(self.run_status_handler())
        asyncio.create_task(self.run_logs_handler())

        logger.info(f"Datapipe agent started.")

        try:
            while True:
                try:
                    logger.info(f"Initialize connection to server...")
                    stub = self._get_grpc_stub()
                    request = agent_messages.ServerEventsRequest(name=self.name)
                    
                    response_stream = stub.GetStreamServerEvents(request)

                    logger.info("Waiting server commands...")
                    async for command in response_stream:
                        await self.process_command(command)
                except grpc.aio.AioRpcError as e:
                    if e.code() == grpc.StatusCode.CANCELLED:
                        logger.error(f"RPC was cancelled: {e.details()}. Try to reconnect...")
                        await asyncio.sleep(RECONNECT_DELAY_SECONDS)

                    elif e.code() == grpc.StatusCode.UNAVAILABLE:
                        logger.error("Server unavailable. Try to reconnect...")
                        await asyncio.sleep(RECONNECT_DELAY_SECONDS)

                    else:
                        raise e

        except asyncio.CancelledError:
            logger.error(f"Datapipe agent received cancellation request.")

    async def shutdown(self, signal_name):
        """Safely cancels all running background tasks and stops the loop."""
        logger.info(f"Received exit signal {signal_name.name}...")
        
        # Grab all tasks running in the event loop except the shutdown task itself
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        
        if not tasks:
            self._loop.stop()
            return

        logger.info(f"Cancelling {len(tasks)} outstanding background tasks...")
        for task in tasks:
            task.cancel()
            
        # Wait for all tasks to acknowledge cancellation gracefully
        await asyncio.gather(*tasks, return_exceptions=False)

        logger.info("All background tasks clean. Stopping event loop.")
        self._loop.stop()


    

     