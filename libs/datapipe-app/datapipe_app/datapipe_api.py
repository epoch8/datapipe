import logging
import os.path
import sys
from contextlib import asynccontextmanager
from typing import Optional

from datapipe.compute import Catalog, DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import datapipe_app.api_v1alpha1 as api_v1alpha1
import datapipe_app.api_v1alpha2 as api_v1alpha2
import datapipe_app.api_v1alpha3 as api_v1alpha3
from datapipe_app.metrics import setup_prometheus_metrics
from datapipe_app.observability.db import ObservabilityStore
from datapipe_app.observability.log_buffer import get_log_buffer
from datapipe_app.observability.recorder import RunRecorder
from datapipe_app.observability.registry import ObservabilityRegistry, load_observability_plugins
from datapipe_app.observability.schema_resolution import resolve_datapipe_schema
from datapipe_app.observability.settings import get_ops_settings
from datapipe_app.observability.tables import (
    ObservabilityTableConfig,
    validate_observability_tables_against_catalog,
)

logger = logging.getLogger(__name__)

_FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "frontend/")


def _resolve_observability_url(ds: Optional[DataStore], ops=None) -> str:
    ops = ops or get_ops_settings()
    if ops.observability_db_url:
        return ops.observability_db_url
    if ds is not None:
        return ds.meta_dbconn.connstr
    raise RuntimeError(
        "OBSERVABILITY_DB_URL (DATAPIPE_APP_OBSERVABILITY_DB_URL) is required in central mode"
    )


def _make_lifespan(api: "DatapipeAPI"):
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        try:
            from datapipe_app.observability.log_buffer import get_log_buffer

            get_log_buffer(api.observability_store).flush_all()
        except Exception:
            logger.exception("Failed to flush run log buffers on shutdown")

    return lifespan


class DatapipeAPI(FastAPI, DatapipeApp):
    def __init__(
        self,
        ds: Optional[DataStore] = None,
        catalog: Optional[Catalog] = None,
        pipeline: Optional[Pipeline] = None,
        app: Optional[DatapipeApp] = None,
        observability_table_config: Optional[ObservabilityTableConfig] = None,
    ):
        self.observability_registry = ObservabilityRegistry()
        load_observability_plugins(self.observability_registry)

        ops = get_ops_settings()
        self.observability_table_config = observability_table_config
        observability_tables = observability_table_config or ObservabilityTableConfig()

        if ops.mode == "central":
            self.ds = None
            self.catalog = catalog or Catalog({})
            self.pipeline = pipeline or Pipeline([])
            self.steps = []
        elif app is not None:
            self.ds = app.ds
            self.catalog = app.catalog
            self.pipeline = app.pipeline
            self.steps = app.steps
        else:
            assert ds is not None and catalog is not None and pipeline is not None
            DatapipeApp.__init__(self, ds, catalog, pipeline)

        if self.catalog is not None and self.catalog.catalog:
            validate_observability_tables_against_catalog(observability_tables, self.catalog)

        if self.ds is not None:
            from datapipe_app.db_schema import register_observability_tables_in_metadata

            register_observability_tables_in_metadata(
                self.ds.meta_dbconn,
                tables=observability_tables,
            )

        FastAPI.__init__(self, lifespan=_make_lifespan(self))

        obs_url = _resolve_observability_url(self.ds, get_ops_settings())
        obs_schema = resolve_datapipe_schema(self.ds)
        self.observability_store = ObservabilityStore.from_url(
            obs_url,
            schema=obs_schema,
            tables=observability_tables,
        )
        self.run_recorder: Optional[RunRecorder] = None
        if ops.mode == "agent" and ops.pipeline_id:
            self.observability_store.register_pipeline(
                ops.pipeline_id,
                display_name=ops.pipeline_id.replace("_", " ").title(),
            )
            self.run_recorder = RunRecorder(
                self.observability_store,
                ops.pipeline_id,
                registry=self.observability_registry,
                ds=self.ds,
                catalog=self.catalog,
                log_buffer=get_log_buffer(self.observability_store),
            )
            from datapipe_app.observability.run_reconciler import reconcile_orphaned_runs_on_startup

            reconciled = reconcile_orphaned_runs_on_startup(
                self.observability_store,
                ops.pipeline_id,
            )
            if reconciled:
                logger.info(
                    "Marked %s orphaned run(s) as interrupted on startup: %s",
                    len(reconciled),
                    ", ".join(reconciled),
                )

        self.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        self.api = FastAPI()

        if ops.mode == "agent" and self.ds is not None:
            setup_prometheus_metrics(
                app=self,
                app_name="datapipe",
                datapipe_app=self,
            )
            self.api.mount(
                "/v1alpha1",
                api_v1alpha1.make_app(
                    self.ds, self.catalog, self.pipeline, self.steps, recorder=self.run_recorder
                ),
                name="v1alpha1",
            )
            self.api.mount(
                "/v1alpha2",
                api_v1alpha2.make_app(
                    self.ds, self.catalog, self.pipeline, self.steps, recorder=self.run_recorder
                ),
                name="v1alpha2",
            )

        self.api.mount(
            "/v1alpha3",
            api_v1alpha3.make_app(
                self.observability_store,
                self.observability_registry,
                ds=self.ds,
                catalog=self.catalog,
                pipeline=self.pipeline,
                steps=self.steps if self.ds is not None else None,
                recorder=self.run_recorder,
            ),
            name="v1alpha3",
        )

        self.mount("/api", self.api, name="api")
        self.mount(
            "/static",
            StaticFiles(directory=os.path.join(_FRONTEND_DIR, "static")),
            name="assets",
        )

        index_path = os.path.join(_FRONTEND_DIR, "index.html")

        @self.get("/")
        async def spa_index() -> FileResponse:
            return FileResponse(index_path)

        @self.get("/{spa_path:path}")
        async def spa_fallback(spa_path: str) -> FileResponse:
            if spa_path.startswith("api") or spa_path.startswith("static"):
                raise HTTPException(status_code=404)
            asset_path = os.path.join(_FRONTEND_DIR, spa_path)
            if os.path.isfile(asset_path):
                return FileResponse(asset_path)
            return FileResponse(index_path)


def setup_logging(level=logging.INFO):
    root_logger = logging.getLogger("datapipe")
    root_logger.setLevel(level)

    handler = logging.StreamHandler(stream=sys.stdout)
    root_logger.addHandler(handler)
