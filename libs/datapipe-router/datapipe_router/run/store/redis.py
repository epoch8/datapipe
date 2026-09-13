import redis.asyncio as redis

from datapipe_router.pb2.storage_client_pb2 import LogEvent, StatusEvent
from datapipe_router.run.store.base import BaseEventStore

KEY_EXPIRATION_TIME = 86400

#TODO добавить возможность сохранения данных на s3 

class RedisEventStore(BaseEventStore):

    def __int__(self, host: str, port: int, key_prefix:str = "run"):
        self.client = redis.Redis(host=host, port=port, decode_responses=True)
        self.key_prefix = key_prefix

    def get_logs_key(self, run_id) -> str:
        return f"{self.key_prefix}:{run_id}:logs"

    def get_statuses_key(self, run_id: str) -> str:
        return f"{self.key_prefix}:{run_id}:statuses"

    async def get_logs(self, run_id: str) -> list[LogEvent]: 
        key = self.get_logs_key(run_id)
        logs = await self.client.lrange(key, 0, -1)

        if logs is None:
            return None 

        resp = []

        for data in logs:
            event = LogEvent()
            event.ParseFromString(data)

            resp.append(event)

        return resp

    async def get_statuses(self, run_id: str) -> list[StatusEvent]: 
        key = self.get_statuses_key(run_id)
        statuses = await self.client.lrange(key, 0, -1)

        if statuses is None:
            return None 

        resp = []

        for data in statuses:
            event = StatusEvent()
            event.ParseFromString(data)

            resp.append(event)

        return resp

    async def add_status_event(self, run_id: str, event: StatusEvent):
        bstr = event.SerializeToString()
        key = self.get_logs_key(run_id)

        await self.client.lpush(key, bstr)
        await self.client.expire(key, KEY_EXPIRATION_TIME)
            
    async def add_log_event(self, run_id: str, event: LogEvent):
        bstr = event.SerializeToString()
        key = self.get_statuses_key(run_id)
        
        await self.client.lpush(key, bstr)
        await self.client.expire(key, KEY_EXPIRATION_TIME)
