import logging
import os.path
import sys
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
from datapipe_app.frontend_static import resolve_frontend_dir
from datapipe_app.metrics import setup_prometheus_metrics

logger = logging.getLogger(__name__)


class DatapipeAPI(FastAPI, DatapipeApp):
    def __init__(
        self,
        ds: Optional[DataStore] = None,
        catalog: Optional[Catalog] = None,
        pipeline: Optional[Pipeline] = None,
        app: Optional[DatapipeApp] = None,
    ):
        if app is not None:
            self.ds = app.ds
            self.catalog = app.catalog
            self.pipeline = app.pipeline
            self.steps = app.steps
        else:
            assert ds is not None
            assert catalog is not None
            assert pipeline is not None
            DatapipeApp.__init__(self, ds, catalog, pipeline)

        FastAPI.__init__(self)

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
            api_v1alpha3.make_app(self.ds, self.catalog, self.pipeline, self.steps),
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
                "Install with: pip install 'datapipe-app[ui]' (or uv sync --extra ui)."
            )


def setup_logging(level=logging.INFO):
    root_logger = logging.getLogger("datapipe")
    root_logger.setLevel(level)

    handler = logging.StreamHandler(stream=sys.stdout)
    root_logger.addHandler(handler)
