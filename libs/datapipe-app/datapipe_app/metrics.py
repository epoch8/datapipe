from typing import Iterable

from datapipe.compute import DatapipeApp
from fastapi import FastAPI
from prometheus_client import Metric
from prometheus_client.core import REGISTRY, GaugeMetricFamily
from prometheus_client.registry import Collector
from starlette_exporter import PrometheusMiddleware, handle_metrics

from datapipe_app.settings import API_SETTINGS


class PipelineStatusCollector(Collector):
    def __init__(
        self,
        datapipe_app: DatapipeApp,
    ) -> None:
        super().__init__()

        self.datapipe_app = datapipe_app

    def describe(self) -> Iterable[Metric]:
        return []

    def collect(self) -> Iterable[Metric]:
        total_counts = GaugeMetricFamily(
            "datapipe_step_total_idx_count",
            "Total count of known idx-es in datapipe step",
            labels=["step_name"],
        )

        changed_counts = GaugeMetricFamily(
            "datapipe_step_changed_idx_count",
            "Count of changed idx-es in datapipe step",
            labels=["step_name"],
        )

        for step in self.datapipe_app.steps:
            try:
                step_status = step.get_status(self.datapipe_app.ds)

                total_counts.add_metric([step.name], step_status.total_idx_count)
                changed_counts.add_metric([step.name], step_status.changed_idx_count)
            except NotImplementedError:
                pass

        yield total_counts
        yield changed_counts


def setup_prometheus_metrics(
    app: FastAPI,
    app_name: str,
    datapipe_app: DatapipeApp,
):
    app.add_middleware(
        PrometheusMiddleware,
        app_name=app_name,
        prefix="datapipe_api",
        buckets=[0.1, 0.5, 1, 2, 3, 4, 5, 15, 30],
        skip_paths=[
            "/healthz",
            "/metrics",
            "/",
            "/favicon.ico",
            "",
        ],
        skip_methods=["OPTIONS"],
    )
    app.add_route("/metrics", handle_metrics)

    if API_SETTINGS.show_step_status:
        REGISTRY.register(PipelineStatusCollector(datapipe_app))
