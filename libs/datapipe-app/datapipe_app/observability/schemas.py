from __future__ import annotations

from typing import Any, Literal, Optional

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
    dataset_id: Optional[str] = None
    train_items: Optional[int] = None
    val_items: Optional[int] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_s: Optional[int] = None
    status: Optional[str] = "success"
    metrics: dict[str, float | None] = Field(default_factory=dict)
    deltas: dict[str, float | None] = Field(default_factory=dict)
    delta_pct: dict[str, float | None] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class SplitCounts(BaseModel):
    train: Optional[int] = None
    val: Optional[int] = None
    test: Optional[int] = None


class MetricDefinition(BaseModel):
    key: str
    label: str
    short_label: str
    group: str
    task_types: list[str] = Field(default_factory=lambda: ["*"])
    format: str = "float"
    higher_is_better: bool = True
    visible_by_default: bool = True
    primary: bool = False
    description: Optional[str] = None
    decimals: Optional[int] = None


class MetricColumnGroup(BaseModel):
    key: str
    label: str
    priority: int = 0
    metrics: list[MetricDefinition] = Field(default_factory=list)


class MetricsTableSchema(BaseModel):
    task_type: str = "custom"
    primary_metric: str = "f1_score"
    groups: list[MetricColumnGroup] = Field(default_factory=list)


class MetricsModelRow(BaseModel):
    id: str
    pipeline_id: str
    model_id: str
    model_display_name: Optional[str] = None
    model_source: Optional[str] = None
    model_version: Optional[str] = None
    task_type: Optional[str] = None
    dataset_id: Optional[str] = None
    dataset_display_name: Optional[str] = None
    frozen_at: Optional[str] = None
    subset: str = ""
    split_counts: SplitCounts = Field(default_factory=SplitCounts)
    has_metrics: bool = False
    metrics_state: Optional[str] = "not_computed"
    metrics: dict[str, float | None] = Field(default_factory=dict)
    run_id: Optional[str] = None
    run_key: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_s: Optional[int] = None
    status: Optional[str] = None
    tags: list[str] = Field(default_factory=list)


class MetricsCandidateCreate(BaseModel):
    model_id: str
    model_source: str = "manual"
    artifact_uri: Optional[str] = None
    dataset_id: str
    subset: str = "val"
    task_type: Optional[str] = None


class MetricsCandidateRow(BaseModel):
    id: str
    pipeline_id: str
    model_id: str
    model_source: str = "manual"
    artifact_uri: Optional[str] = None
    dataset_id: str
    subset: str = "val"
    task_type: Optional[str] = None
    metrics_state: str = "not_computed"


class MetricsCandidatesResponse(BaseModel):
    rows: list[MetricsCandidateRow]
    total: int


class MetricsEvaluateRequest(BaseModel):
    candidate_ids: list[str] = Field(default_factory=list)
    labels: list[list[str]] = Field(default_factory=lambda: [["stage", "count-metrics"]])
    background: bool = True


class MetricsEvaluateResponse(BaseModel):
    run_id: str
    status: str = "running"


class MetricsRunsResponse(BaseModel):
    model_config = {"populate_by_name": True}

    rows: list[MetricsModelRow]
    total: int
    available_filters: dict[str, list[str]]
    table_schema: MetricsTableSchema = Field(alias="schema")


class FrozenDatasetRow(BaseModel):
    dataset_id: str
    frozen_at: Optional[str] = None
    train_count: Optional[int] = None
    val_count: Optional[int] = None
    test_count: Optional[int] = None
    models_count: Optional[int] = None


class FrozenDatasetsResponse(BaseModel):
    rows: list[FrozenDatasetRow]
    total: int


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


class EntitySourceRecord(BaseModel):
    table_name: str
    pk: dict[str, Any] | None = None
    record: dict[str, Any] = Field(default_factory=dict)


class MetricsModelDetailKpi(BaseModel):
    key: str
    label: str
    value: float | str | None = None
    format: str = "float"


class MetricsModelDetailRelated(BaseModel):
    dataset_id: Optional[str] = None
    run_key: Optional[str] = None
    run_id: Optional[str] = None


class MetricsModelDetailResponse(BaseModel):
    pipeline_id: str
    model_id: str
    title: str
    source_table: Optional[str] = None
    source_pk: dict[str, Any] | None = None
    source_record: dict[str, Any] | None = None
    source_table_url: Optional[str] = None
    model_row: Optional[MetricsModelRow] = None
    frozen_dataset: Optional[FrozenDatasetRow] = None
    frozen_dataset_source_table: Optional[str] = None
    frozen_dataset_source_pk: dict[str, Any] | None = None
    metrics_rows: list[MetricsModelRow] = Field(default_factory=list)
    kpis: list[MetricsModelDetailKpi] = Field(default_factory=list)
    related: MetricsModelDetailRelated = Field(default_factory=MetricsModelDetailRelated)


class FrozenDatasetCoverage(BaseModel):
    models_total: int = 0
    models_with_metrics: int = 0
    subsets: list[str] = Field(default_factory=list)
    best_metric_key: Optional[str] = None
    best_metric_value: Optional[float] = None
    best_model_id: Optional[str] = None


class FrozenDatasetDetailResponse(BaseModel):
    pipeline_id: str
    dataset_id: str
    title: str
    dataset: FrozenDatasetRow
    source_table: Optional[str] = None
    source_pk: dict[str, Any] | None = None
    source_record: dict[str, Any] | None = None
    source_table_url: Optional[str] = None
    models: list[MetricsModelRow] = Field(default_factory=list)
    coverage: FrozenDatasetCoverage = Field(default_factory=FrozenDatasetCoverage)
