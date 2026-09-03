import uuid

from typing import Any

from sqlalchemy import Column, MetaData, Table, create_engine, func, text
from sqlalchemy.pool import QueuePool, SingletonThreadPool
from sqlalchemy.event import listen

from datapipe_router.models import Base


class DBConn:
    def __init__(
        self,
        connstr: str,
        schema: str | None = None,
        create_engine_kwargs: dict[str, Any] | None = None,
    ):
        create_engine_kwargs = create_engine_kwargs or {}
        self._init(connstr, schema, create_engine_kwargs)

    def _init(
        self,
        connstr: str,
        schema: str | None,
        create_engine_kwargs: dict[str, Any],
    ) -> None:
        self.connstr = connstr
        self.schema = schema
        self.create_engine_kwargs = create_engine_kwargs

        if connstr.startswith("sqlite") or connstr.startswith("pysqlite"):
            self.supports_update_from = False

            self.con = create_engine(
                connstr,
                poolclass=SingletonThreadPool,
                **create_engine_kwargs,
            )

            # WAL mode is required for concurrent reads and writes
            # https://www.sqlite.org/wal.html
            with self.con.begin() as con:
                con.execute(text("PRAGMA journal_mode=WAL"))
        else:
            # Assume relatively new Postgres
            self.supports_update_from = True

            self.con = create_engine(
                connstr,
                poolclass=QueuePool,
                pool_pre_ping=True,
                pool_recycle=3600,
                **create_engine_kwargs,
                # pool_size=25,
            )

    #         listen(self.con, "connect", self.set_schema)

    # def set_schema(self, dbapi_connection, connection_record):
    #     print("CONNECT")
    #     with dbapi_connection.cursor() as cursor:
    #         cursor.execute("SET search_path TO :schema", {"schema": self.schema})

    def __reduce__(self) -> tuple[Any, ...]:
        return self.__class__, (
            self.connstr,
            self.schema,
            self.create_engine_kwargs,
        )

    def __getstate__(self):
        return {
            "connstr": self.connstr,
            "schema": self.schema,
            "create_engine_kwargs": self.create_engine_kwargs,
        }

    def __setstate__(self, state):
        self._init(state["connstr"], state["schema"], state["create_engine_kwargs"])






class PipelineRun:

    def __init__(self, run_id, agent_id):
        self.run_id = run_id
        self.agent_id = agent_id
        self.status = "Created"


class ServerDataStore:
    def __init__(self, dbconn: DBConn):
        self.dbconn = dbconn
        self.sqla_metadata = Base.metadata

        if self.dbconn.schema:
            self.sqla_metadata.schema = self.dbconn.schema
        
    async def add_log_record(self, run_id: str, log_record: str):
        pass

    async def set_status(self, run_id: str, status: str):
        pass

    async def create_run(self, agent_id: str) -> PipelineRun:
        run_id = str(uuid.uuid4())
        return PipelineRun(run_id, agent_id)