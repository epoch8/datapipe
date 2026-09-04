import asyncio
import grpc
import uuid
import logging

from enum import Enum
from typing import Tuple

import datapipe_router.pb2.agent_pb2 as agent_messages
import datapipe_router.pb2.agent_pb2_grpc as agent_messages_grpc
import datapipe_router.pb2.client_pb2 as client_messages
import datapipe_router.pb2.client_pb2_grpc as client_messages_grpc

from datapipe_router.datastore import ServerDataStore, DBConn, PipelineRun


SERVER_OPTIONS = [
    # Period in milliseconds after which a ping is sent if the channel is idle
    ('grpc.keepalive_time_ms', 15000), # 10 seconds
    
    # Time in milliseconds the sender waits for a ping response before closing the connection
    ('grpc.keepalive_timeout_ms', 10000), # 5 seconds
    
    # Allow keepalive pings even if there are no active streaming calls
    ('grpc.keepalive_permit_without_calls', 1), 
    
    # Maximum number of pings allowed without data before getting penalized (0 means infinite)
    ('grpc.http2.max_ping_strikes', 0) 
]

STREAM_PING_DELAY = 10
GED_DATA_TIMEOUT = 5

class RUN_STATUSES(Enum):
    CREATED = "Created"
    PENDING = "Pending"
    RUNNING = "Running"
    FINISHED = "Finished"
    FAILED = "Failed"
    CANCELED = "Canceled"

logger = logging.getLogger("datapipe_router.server")


class AgentConnection:
    def __init__(self, name:str):
        self.name = name
        self.queue = asyncio.Queue()

    async def command_stream(self):
        while(True):
            item = await self.queue.get()

            if item is None:
                break

            yield item 

    async def task_done(self):
        await self.queue.task_done()

    async def send_event(self, command):
        await self.queue.put(command)


class AgentsPool:
    def __init__(self):
        self.connections: dict[str, AgentConnection] = {}

    def has_connection(self, agent_id: str):
        return agent_id in self.connections

    async def get_agents(self):
        return self.connections.keys()

    async def send_event(self, agent_id, event: agent_messages.ServerEventsResponse):
        if agent_id in self.connections:
            await self.connections[agent_id].send_event(event)

    async def add_agent(self, agent_id: str) -> AgentConnection:
        self.connections[agent_id] = AgentConnection(agent_id)

        return self.connections[agent_id]

    async def remove_agent(self, agent_id: str):
        if agent_id in self.connections:
            del self.connections[agent_id]

    async def run_agent_ping_sender(self):
        while True:
            for _, conn in self.connections.items():
                await conn.send_event(
                    agent_messages.ServerEventsResponse(
                        request_id=str(uuid.uuid4()),
                        ping_event=agent_messages.PingEvent()
                    )
                )

            await asyncio.sleep(STREAM_PING_DELAY)
        

class RunCache:

    def __init__(self, run_id: str, agent_id, status: str):
        self.run_id = run_id
        self.agent_id = agent_id
        self.status = status
        self.logs = []
        self.subscribers = {}

    async def subscribe(self) -> Tuple[str, asyncio.Queue]:
        subscribe_id = str(uuid.uuid4())
        queue = asyncio.Queue()

        self.subscribers[subscribe_id] = queue

        await queue.put(self.logs)

        return subscribe_id, queue

    async def unsubscribe(self, subscribe_id):
        if subscribe_id in self.subscribers:
            await self.subscribers[subscribe_id].put(None)
            del self.subscribers[subscribe_id]

    async def unsubscribe_all(self):
        for queue in self.subscribers.values():
            await queue.put(None)

        self.subscribers = {}

    async def set_status(self, status: str):
        self.status  = status
            
    async def add_log_record(self, log: str):
        self.logs.append(log)

        for queue in self.subscribers.values():
            await queue.put([log])

    @classmethod
    def from_run(cls, run: PipelineRun):
        return cls(
            run.run_id,
            run.agent_id,
            run.status,
        )


class RunsCache:
    def __init__(self):
        self.runs: dict[str, RunCache] = {}

    async def get_runs(self):
        return self.runs.values()

    async def get_run_logs(self, run_id: str) -> list[str]:
        if run_id not in self.runs:
            return None

        return self.runs[run_id].logs
    
    async def subscribe(self, run_id) -> Tuple[str, asyncio.Queue]:
        if run_id not in self.runs:
            return None, None

        return await self.runs[run_id].subscribe()

    async def unsubscribe(self, run_id, subscribe_id):
        if run_id not in self.runs:
            await self.runs[run_id].unsubscribe(subscribe_id=subscribe_id)

    def add_run(self, run: PipelineRun):
        if run.run_id in self.runs:
            raise ValueError(f"Pipeline run {run.run_id} already added")

        self.runs[run.run_id] = RunCache.from_run(run)

    async def add_log_record(self, run_id: str, log: str):
        if run_id in self.runs:
            await self.runs[run_id].add_log_record(log)

    async def set_status(self, run_id: str, status: str):
        if run_id in self.runs:
            await self.runs[run_id].set_status(status)

            if status in (RUN_STATUSES.FINISHED, RUN_STATUSES.CANCELED, RUN_STATUSES.FAILED):
                await self.runs[run_id].unsubscribe_all()


class DataRequestRouter:

    def __init__(self):
        self.routes: dict[str, asyncio.Queue] = {}

    def create_rout(self):
        route_id = str(uuid.uuid4())
        queue = asyncio.Queue()

        self.routes[route_id] = queue

        return route_id, queue

    async def route_request(self, request: agent_messages.SendDataRequest):
        if request.route_id in self.routes:
            await self.routes[request.route_id].put(request)

    async def finish_route(self, route_id):
        if route_id in self.routes:
            del self.routes[route_id]


class ServerServicer(
    agent_messages_grpc.DatapipeServiceServicer,
    client_messages_grpc.ClientServiceServicer,
):
    def __init__(self, store: ServerDataStore):
        self.data_router =  DataRequestRouter()
        self.agents = AgentsPool()
        self.runs_cache = RunsCache() 
        self.store = store

    async def init(self):
        asyncio.create_task(self.agents.run_agent_ping_sender()) 
        
    async def GetStreamServerEvents(self, request, context):
        try:
            logger.info(f"Agent {request.name} connecting...")
            connection = await self.agents.add_agent(request.name)

            logger.info(f"Agent {request.name} connected")
            async for command in connection.command_stream():
                yield command
    
        finally:
            await self.agents.remove_agent(request.name)
            logger.info(f"Agent {request.name} disconnected")
            
    async def SendData(self, request, context):
        await self.data_router.route_request(request)

        return agent_messages.SendDataResponse()

    async def SendGraph(self, request, context):
        await self.data_router.route_request(request)

        return agent_messages.SendDataResponse()

    async def SendLogs(self, request, context):
        await self.store.add_log_record(request.run_id, request.log)
        await self.runs_cache.add_log_record(request.run_id, request.log)

        return agent_messages.SendLogsResponse(state="ok")

    async def SendRunStatus(self, request, context):
        await self.store.set_status(request.run_id, request.status)
        await self.runs_cache.set_status(request.run_id, request.status)

        return agent_messages.SendRunStatusResponse(state="ok")

    async def GetData(self, request, context):
        if self.agents.has_connection(request.agent_id):
            try:
                route_id, queue = self.data_router.create_rout()

                request_id = str(uuid.uuid4())
                event = agent_messages.ServerEventsResponse(
                    request_id=request_id,
                    data_event=agent_messages.DataEvent(
                        route_id=route_id,
                        request=request
                    )
                )

                await self.agents.send_event(request.agent_id, event)
            
                response: agent_messages.SendDataRequest = await asyncio.wait_for(
                    queue.get(), 
                    timeout=GED_DATA_TIMEOUT
                )

                return response.data
            
            except asyncio.TimeoutError:
                return client_messages.GetDataResponse(data=None)
            
            finally:
                await self.data_router.finish_route(route_id)

        return client_messages.GetDataResponse(data=None)

    async def GetGraph(self, request, context):
        if self.agents.has_connection(request.agent_id):
            try:
                route_id, queue = self.data_router.create_rout()

                request_id = str(uuid.uuid4())
                event = agent_messages.ServerEventsResponse(
                    request_id=request_id,
                    graph_event=agent_messages.GraphEvent(
                        route_id=route_id,
                        label_key=request.label_key,
                        value=request.value
                    )
                )

                await self.agents.send_event(request.agent_id, event)
            
                response: agent_messages.SendGraphRequest = await asyncio.wait_for(
                    queue.get(), 
                    timeout=GED_DATA_TIMEOUT
                )

                return response.data
            
            except asyncio.TimeoutError:
                return client_messages.GetGraphResponse()
            
            finally:
                await self.data_router.finish_route(route_id)

        return client_messages.GetGraphResponse()

    async def GetAgents(self, request, context):
        agents_info = await self.agents.get_agents()

        return client_messages.GetAgentsResponse(agents=agents_info)

    async def GetRunList(self, request, context):
        runs = await self.runs_cache.get_runs()

        return client_messages.GetRunListResponse(
            runs=[
                client_messages.RunInfo(
                    run_id=run.run_id,
                    agent_id=run.agent_id,
                    status=run.status,
                )
                for run in runs
            ]
        )

    async def GetRunLogs(self, request, context):
        logs = await self.runs_cache.get_run_logs(request.run_id)

        return client_messages.GetRunLogsResponse(
            logs=logs
        )

    async def GetRunLogsStream(self, request, context):
        subscribe_id, queue = await self.runs_cache.subscribe(request.run_id)

        if not subscribe_id:
            return

        try:
            async for logs in self.logs_stream(queue):
                if logs is None:
                    break

                yield client_messages.GetRunLogsStreamResponse(logs=logs)
        finally:
            await self.runs_cache.unsubscribe(request.run_id, subscribe_id)
    
    async def RunPipeline(self, request, context):
        run: PipelineRun = await self.store.create_run(request.agent_id)

        self.runs_cache.add_run(run)

        request_id = str(uuid.uuid4())
        event = agent_messages.ServerEventsResponse(
            request_id=request_id,
            run_event=agent_messages.RunEvent(
                run_id=run.run_id
            )
        )

        await self.agents.send_event(request.agent_id, event)

        return client_messages.RunPipelineResponse(run_id=run.run_id)

    async def logs_stream(self, queue):
        while(True):
            item = await queue.get()

            if item is None:
                break

            yield item 
    


class DatapipeServer:
    def __init__(self, port=10500, address="[::]", dbconn: DBConn = None):
        self.port = port
        self.address = address

        self.servicer = ServerServicer(
            store=ServerDataStore(dbconn)
        )

    async def run_server(self):
        logger.info(f"Starting async gRPC server on port {self.port}...")
        self.server = grpc.aio.server(options=SERVER_OPTIONS)

        agent_messages_grpc.add_DatapipeServiceServicer_to_server(self.servicer, self.server)
        client_messages_grpc.add_ClientServiceServicer_to_server(self.servicer, self.server)
                
        self.server.add_insecure_port(f'{self.address}:{self.port}')

        await self.servicer.init()
        await self.server.start()
        
        try:
            await self.server.wait_for_termination()
        except asyncio.CancelledError:
            logger.info("Server task was cancelled, shutting down gracefully...")
            # Grace period of 0 seconds or higher
            try:
                await self.server.stop(grace=5)
            except asyncio.CancelledError:
                logger.warning("Server stop cancelled,")


if __name__ == '__main__':
    asyncio.run(DatapipeServer().run_server())



    