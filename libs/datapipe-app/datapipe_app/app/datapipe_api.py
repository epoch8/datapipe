import logging
import os.path
import sys
from contextlib import asynccontextmanager
from types import ModuleType
from typing import Optional

from datapipe.compute import Catalog, DatapipeApp, Pipeline
from datapipe.datatable import DataStore
from datapipe.executor import Executor
from datapipe.store.database import DBConn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import datapipe_app.api.v1alpha1 as api_v1alpha1
import datapipe_app.api.v1alpha2 as api_v1alpha2
import datapipe_app.api.v1alpha3 as api_v1alpha3
from datapipe_app.app.metrics import setup_prometheus_metrics
from datapipe_app.app.ops_specs import OpsSpecsMixin
from datapipe_app.ops.spec_registry import OpsSpecRegistry
from datapipe_app.pipeline.pipeline_binding import pipeline_module_for
from datapipe_app.observability.store.db import ObservabilityStore
from datapipe_app.observability.connections.dbconn import dbconn_same_target
from datapipe_app.observability.logging.log_buffer import get_log_buffer
from datapipe_app.observability.runs.recorder import RunRecorder
from datapipe_app.observability.plugins.registry import ObservabilityRegistry, load_observability_plugins
from datapipe_app.observability.run_logs import RunLogsBackend, warn_if_run_logs_backend_missing
from datapipe_app.observability.config.settings import OpsSettings, configure_active_ops, resolve_ops_settings
from datapipe_app.observability.config.tables import (
    ObservabilityTableConfig,
    ensure_observability_tables_compatible_with_pipeline,
)

from datapipe_app.app.frontend_static import resolve_frontend_dir

logger = logging.getLogger(__name__)


def _resolve_observability_dbconn(
    ds: DataStore,
    observability_dbconn: Optional[DBConn],
) -> DBConn:
    if observability_dbconn is not None:
        return observability_dbconn
    return ds.meta_dbconn


def _make_lifespan(api: "DatapipeAPI"):
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        try:
            from datapipe_app.observability.logging.log_buffer import get_log_buffer

            get_log_buffer(api.observability_store).flush_all()
        except Exception:
            logger.exception("Failed to flush run log buffers on shutdown")

    return lifespan


class DatapipeAPI(FastAPI, OpsSpecsMixin, DatapipeApp):
    catalog: Catalog
    pipeline: Pipeline
    steps: list
    ops_specs: OpsSpecRegistry
    observability_registry: ObservabilityRegistry
    observability_table_config: Optional[ObservabilityTableConfig]
    observability_dbconn: DBConn
    run_logs_backend: Optional[RunLogsBackend]
    ops_settings: OpsSettings
    executor: Executor | None

    def __init__(
        self,
        ds: Optional[DataStore] = None,
        catalog: Optional[Catalog] = None,
        pipeline: Optional[Pipeline] = None,
        app: Optional[DatapipeApp] = None,
        observability_table_config: Optional[ObservabilityTableConfig] = None,
        pipeline_id: Optional[str] = None,
        observability_dbconn: Optional[DBConn] = None,
        run_logs_backend: Optional[RunLogsBackend] = None,
        pipeline_spec: Optional[str] = None,
        executor: Executor | None = None,
    ):
        self.observability_registry = ObservabilityRegistry()
        load_observability_plugins(self.observability_registry)

        self.observability_table_config = observability_table_config
        observability_tables = observability_table_config or ObservabilityTableConfig()
        self.run_logs_backend = run_logs_backend

        if app is not None:
            if isinstance(app, DatapipeAPI):
                raise TypeError(
                    "Cannot wrap DatapipeAPI in DatapipeAPI. "
                    "Use the existing app instance directly (e.g. `datapipe --pipeline app:app api`)."
                )
            self.ds = app.ds
            self.catalog = app.catalog
            self.pipeline = app.pipeline
            self.steps = app.steps
            self.ops_specs = OpsSpecRegistry()
        else:
            assert ds is not None and catalog is not None and pipeline is not None
            DatapipeApp.__init__(self, ds, catalog, pipeline)
            self.ops_specs = OpsSpecRegistry()

        if self.ds is None:
            raise ValueError("DataStore is required")

        pipeline_module = pipeline_module_for(app)
        explicit_pipeline_id = pipeline_id
        self.ops_settings = resolve_ops_settings(
            pipeline_id=explicit_pipeline_id,
            pipeline_spec=pipeline_spec,
            pipeline_module=pipeline_module,
        )
        configure_active_ops(self.ops_settings)
        ops = self.ops_settings
        self.executor = executor

        self.observability_dbconn = _resolve_observability_dbconn(self.ds, observability_dbconn)

        ensure_observability_tables_compatible_with_pipeline(
            observability_dbconn=self.observability_dbconn,
            pipeline_dbconn=self.ds.meta_dbconn,
            config=observability_tables,
            catalog=self.catalog,
        )
        self.ops_specs.validate(self.catalog, self.ds, strict=True)

        self.observability_registry.attach_ops_specs(self.ops_specs)

        if dbconn_same_target(self.observability_dbconn, self.ds.meta_dbconn):
            from datapipe_app.app.db_schema import register_observability_tables_in_metadata

            register_observability_tables_in_metadata(
                self.ds.meta_dbconn,
                tables=observability_tables,
            )

        warn_if_run_logs_backend_missing(self.run_logs_backend)

        FastAPI.__init__(self, lifespan=_make_lifespan(self))

        self.observability_store = ObservabilityStore.from_dbconn(
            self.observability_dbconn,
            tables=observability_tables,
            run_logs_backend=self.run_logs_backend,
        )
        self.run_recorder: Optional[RunRecorder] = None
        if ops.pipeline_id:
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
                ops_specs=self.ops_specs,
                log_buffer=get_log_buffer(self.observability_store),
            )
            from datapipe_app.observability.runs.run_reconciler import reconcile_orphaned_runs_on_startup

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

        setup_prometheus_metrics(
            app=self,
            app_name="datapipe",
            datapipe_app=self,
        )
        self.api.mount(
            "/v1alpha1",
            api_v1alpha1.make_app(self.ds, self.catalog, self.pipeline, self.steps),
            name="v1alpha1",
        )
        self.api.mount(
            "/v1alpha2",
            api_v1alpha2.make_app(self.ds, self.catalog, self.pipeline, self.steps),
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
                steps=self.steps,
                recorder=self.run_recorder,
                ops_specs=self.ops_specs,
                executor_host=self,
            ),
            name="v1alpha3",
        )

        self.mount("/api", self.api, name="api")
        frontend_dir = resolve_frontend_dir()
        if frontend_dir is not None:
            self.mount(
                "/static",
                StaticFiles(directory=os.path.join(frontend_dir, "static")),
                name="assets",
            )

            index_path = os.path.join(frontend_dir, "index.html")

            @self.get("/")
            async def spa_index() -> FileResponse:
                return FileResponse(index_path)

            @self.get("/{spa_path:path}")
            async def spa_fallback(spa_path: str) -> FileResponse:
                if spa_path.startswith("api") or spa_path.startswith("static"):
                    raise HTTPException(status_code=404)
                asset_path = os.path.join(frontend_dir, spa_path)
                if os.path.isfile(asset_path):
                    return FileResponse(asset_path)
                return FileResponse(index_path)
        else:
            logger.warning(
                "datapipe-ui is not installed; Ops UI static files will not be served. "
                "Install with: pip install datapipe-app[ui]"
            )

    def refresh_ops_settings(
        self,
        *,
        pipeline_module: ModuleType | None = None,
        pipeline_spec: Optional[str] = None,
        pipeline_id: Optional[str] = None,
    ) -> None:
        self.ops_settings = resolve_ops_settings(
            pipeline_id=pipeline_id,
            pipeline_spec=pipeline_spec,
            pipeline_module=pipeline_module or pipeline_module_for(self),
        )
        configure_active_ops(self.ops_settings)


def setup_logging(level=logging.INFO):
    root_logger = logging.getLogger("datapipe")
    root_logger.setLevel(level)

    handler = logging.StreamHandler(stream=sys.stdout)
    root_logger.addHandler(handler)
