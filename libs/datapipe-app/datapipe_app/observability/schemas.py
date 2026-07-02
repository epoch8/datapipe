from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class KpiItem(BaseModel):
    key: str
    label: str
    value: float | str | None = None
    delta: Optional[float] = None
    delta_pct: Optional[float] = None
    trend: list[dict[str, float | str | None]] = Field(default_factory=list)
    format: str = "float"
    higher_is_better: bool = True
    subtitle: Optional[str] = None


class AnomalyItem(BaseModel):
    severity: Literal["info", "warning", "critical"]
    metric: str
    title: str
    description: str
    run_id: Optional[str] = None
    value: Optional[float] = None
    expected: Optional[float] = None
    delta: Optional[float] = None


class MetricsRunRow(BaseModel):
    run_id: str
    run_key: Optional[str] = None
    pipeline_id: str
    model_id: str = ""
    model_version: Optional[str] = None
    task_type: Optional[str] = None
    subset: str = ""
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_s: Optional[int] = None
    status: Optional[str] = "success"
    metrics: dict[str, float | None] = Field(default_factory=dict)
    deltas: dict[str, float | None] = Field(default_factory=dict)
    delta_pct: dict[str, float | None] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class MetricsRunsResponse(BaseModel):
    rows: list[MetricsRunRow]
    total: int
    available_filters: dict[str, list[str]]


class MetricsSummaryResponse(BaseModel):
    pipeline_id: str
    primary_metric: str
    has_metrics: bool = False
    latest_run: Optional[MetricsRunRow] = None
    best_run: Optional[MetricsRunRow] = None
    previous_run: Optional[MetricsRunRow] = None
    kpis: list[KpiItem] = Field(default_factory=list)
    anomalies: list[AnomalyItem] = Field(default_factory=list)


class MetricsTimeseriesResponse(BaseModel):
    series: list[dict[str, object]]


class ClassMetricRow(BaseModel):
    label: str
    class_id: Optional[str | int] = None
    images_support: Optional[int] = None
    support: Optional[int] = None
    TP: Optional[int] = None
    FP: Optional[int] = None
    FN: Optional[int] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    iou_mean: Optional[float] = None
    mAP50: Optional[float] = None
    mAP50_95: Optional[float] = None
    pose_P: Optional[float] = None
    pose_R: Optional[float] = None
    pose_mAP50: Optional[float] = None
    pose_mAP50_95: Optional[float] = None
    delta: dict[str, float | None] = Field(default_factory=dict)
    trend: dict[str, list[dict[str, float | str | None]]] = Field(default_factory=dict)


class ClassMetricsResponse(BaseModel):
    rows: list[ClassMetricRow]
    total: int
    summary: dict[str, object]


class ClassMetricDetailResponse(BaseModel):
    label: str
    class_id: Optional[str | int] = None
    latest: ClassMetricRow
    previous: Optional[ClassMetricRow] = None
    trends: list[dict[str, object]] = Field(default_factory=list)
    error_breakdown: Optional[dict[str, int]] = None
    confusion_top: list[dict[str, object]] = Field(default_factory=list)
    gallery_url: Optional[str] = None


class TrainingRunRow(BaseModel):
    run_key: str
    run_id: Optional[str] = None
    model_id: Optional[str] = None
    task_type: Optional[str] = None
    framework: Optional[str] = None
    dataset: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_s: Optional[int] = None
    status: str = "unknown"
    attempt: Optional[int] = None
    tags: list[str] = Field(default_factory=list)
    best_metric_name: Optional[str] = None
    best_metric_value: Optional[float] = None
    params: dict[str, object] = Field(default_factory=dict)
    artifacts: dict[str, object] = Field(default_factory=dict)
    is_best: bool = False


class TrainingRunsResponse(BaseModel):
    rows: list[TrainingRunRow]
    total: int
    filters: dict[str, list[str]]


class TrainingCompareResponse(BaseModel):
    runs: list[TrainingRunRow] = Field(default_factory=list)
    available_metrics: list[dict[str, object]] = Field(default_factory=list)
    charts: list[dict[str, object]] = Field(default_factory=list)
    run_keys: list[str] = Field(default_factory=list)
    charts_legacy: list[dict[str, object]] = Field(default_factory=list)


class SqlQueryRequest(BaseModel):
    sql: str
    limit: Optional[int] = 1000
    offset: Optional[int] = 0
    datasource: Optional[str] = "datapipe_analytics"


class SqlQueryResponse(BaseModel):
    columns: list[dict[str, str]]
    rows: list[dict[str, object]]
    total: Optional[int] = None
    execution_ms: float
    truncated: bool = False


class SqlSchemaResponse(BaseModel):
    datasource: str
    tables: list[dict[str, object]]
