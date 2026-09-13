import asyncio
import grpc
import logging

from datapipe_router.pb2.router_agent_pb2_grpc import add_RouterDatapipeServiceServicer_to_server
from datapipe_router.pb2.router_client_pb2_grpc import add_RouterClientServiceServicer_to_server

from datapipe_router.datastore import ServerDataStore, DBConn
from datapipe_router.servicers.router_servicer import RouterServicer


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


logger = logging.getLogger("datapipe_router.server")


class RouterServer:
    def __init__(self, port=10500, address="[::]", dbconn: DBConn = None):
        self.port = port
        self.address = address

        self.servicer = RouterServicer(
            store=ServerDataStore(dbconn)
        )

    async def run_server(self):
        logger.info(f"Starting async gRPC server on port {self.port}...")
        self.server = grpc.aio.server(options=SERVER_OPTIONS)

        add_RouterDatapipeServiceServicer_to_server(self.servicer, self.server)
        add_RouterClientServiceServicer_to_server(self.servicer, self.server)
                
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
    asyncio.run(RouterServer().run_server())



    