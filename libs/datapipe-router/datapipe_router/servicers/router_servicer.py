import asyncio
import uuid
import logging
import grpc

import datapipe_router.pb2.router_agent_pb2 as agent_messages
import datapipe_router.pb2.router_agent_pb2_grpc as agent_messages_grpc
import datapipe_router.pb2.router_client_pb2 as client_messages
import datapipe_router.pb2.router_client_pb2_grpc as client_messages_grpc

from datapipe_router.datastore import ServerDataStore


STREAM_PING_DELAY = 10
GED_DATA_TIMEOUT = 10
RUN_PIPELINE_TIMEOUT = 19


logger = logging.getLogger(__name__)


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


class RouterServicer(
    agent_messages_grpc.RouterDatapipeServiceServicer,
    client_messages_grpc.RouterClientServiceServicer,
):
    def __init__(self, store: ServerDataStore):
        self.data_router =  DataRequestRouter()
        self.agents = AgentsPool()
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

    async def SendRunCreationStatus(self, request, context):
        await self.data_router.route_request(request)
        
        return agent_messages.SendRunCreationStatusResponse()
    
    async def SendPing(self, request, context):
        response = agent_messages.PingResponse(
            status="ok"
        )

        if request.name not in self.agents.connections:
            response.status = "disconnected"
            response.message = "Agent was disconnected" 
            
        return response

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

                return client_messages.GetDataResponse(data=response.data)
            
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

                return client_messages.GetGraphResponse(data=response.data)
            
            except asyncio.TimeoutError:
                return client_messages.GetGraphResponse()
            
            finally:
                await self.data_router.finish_route(route_id)

        return client_messages.GetGraphResponse()

    async def GetAgents(self, request, context):
        agents_info = await self.agents.get_agents()

        return client_messages.GetAgentsResponse(agents=agents_info)

    async def RunPipeline(self, request, context):
        if self.agents.has_connection(request.agent_id):
            try:
                route_id, queue = self.data_router.create_rout()

                request_id = str(uuid.uuid4())
                event = agent_messages.ServerEventsResponse(
                    request_id=request_id,
                    run_event=agent_messages.RunEvent(
                        route_id=route_id,
                        run_id=request.run_id,
                        labels=request.labels,
                        changelist=request.changelist
                    )
                )

                await self.agents.send_event(request.agent_id, event)
            
                response: agent_messages.SendRunCreationStatusRequest = await asyncio.wait_for(
                    queue.get(), 
                    timeout=RUN_PIPELINE_TIMEOUT
                )

                return client_messages.RunPipelineResponse(
                    run_id=request.run_id, 
                    status=response.state,
                    error=response.error
                )
            
            except asyncio.TimeoutError:
                return client_messages.RunPipelineResponse(
                    run_id=request.run_id, 
                    status="error",
                    error="Request timedout"
                )
            
            finally:
                await self.data_router.finish_route(route_id)

        return client_messages.RunPipelineResponse(
            run_id=request.run_id, 
            status="error",
            error="Agent not connected"
        )

    async def CancelPipeline(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')