import type { ChartSpec } from "@datapipe/ui/types/ops";

/* --- ML Observability types --- */

export type TaskType = "classification" | "detection" | "keypoints" | "segmentation" | string;

export interface MetricsRunRow {
    run_id: string;
    run_key?: string;
    pipeline_id: string;
    model_id: string;
    model_version?: string;
    task_type?: TaskType;
    subset: string;
    dataset_id?: string;
    train_items?: number;
    val_items?: number;
    started_at?: string;
    finished_at?: string;
    duration_s?: number;
    status?: string;
    metrics: Record<string, number | null>;
    deltas?: Record<string, number | null>;
    delta_pct?: Record<string, number | null>;
    tags?: string[];
}

export interface SplitCounts {
    train?: number;
    val?: number;
    test?: number;
}

export type MetricFormat = "float" | "percent" | "integer" | "duration" | "string";

export interface MetricDefinition {
    key: string;
    label: string;
    short_label: string;
    group: string;
    task_types: string[];
    format: MetricFormat;
    higher_is_better: boolean;
    visible_by_default: boolean;
    primary?: boolean;
    description?: string;
    decimals?: number;
}

export interface MetricColumnGroup {
    key: string;
    label: string;
    priority: number;
    metrics: MetricDefinition[];
}

export interface MetricsTableSchema {
    task_type: string;
    primary_metric: string;
    groups: MetricColumnGroup[];
}

export interface MetricsModelRow {
    id: string;
    pipeline_id: string;
    model_id: string;
    model_display_name?: string;
    model_source?: "training_run" | "artifact" | "manual" | "registry" | string;
    model_version?: string;
    task_type?: TaskType;
    dataset_id?: string;
    dataset_display_name?: string;
    frozen_at?: string;
    subset: string;
    split_counts?: SplitCounts;
    has_metrics: boolean;
    metrics_state?: "not_computed" | "queued" | "running" | "computed" | "failed" | string;
    metrics?: Record<string, number | null>;
    run_id?: string;
    run_key?: string;
    started_at?: string;
    finished_at?: string;
    duration_s?: number;
    status?: string;
    tags?: string[];
}

export interface MetricsCandidateCreate {
    model_id: string;
    model_source?: string;
    artifact_uri?: string;
    dataset_id: string;
    subset?: string;
    task_type?: string;
}

export interface MetricsCandidateRow {
    id: string;
    pipeline_id: string;
    model_id: string;
    model_source: string;
    artifact_uri?: string;
    dataset_id: string;
    subset: string;
    task_type?: string;
    metrics_state: string;
}

export interface MetricsCandidatesResponse {
    rows: MetricsCandidateRow[];
    total: number;
}

export interface MetricsEvaluateRequest {
    candidate_ids?: string[];
    labels?: [string, string][];
    background?: boolean;
}

export interface MetricsEvaluateResponse {
    run_id: string;
    status: string;
}

export interface FrozenDatasetRow {
    dataset_id: string;
    frozen_at?: string;
    train_count?: number;
    val_count?: number;
    test_count?: number;
    models_count?: number;
}

export interface FrozenDatasetsResponse {
    rows: FrozenDatasetRow[];
    total: number;
}

export interface MetricsModelDetailKpi {
    key: string;
    label: string;
    value: number | string | null;
    format?: MetricFormat;
}

export interface MetricsModelDetailRelated {
    dataset_id?: string | null;
    run_key?: string | null;
    run_id?: string | null;
}

export interface MetricsModelDetailResponse {
    pipeline_id: string;
    model_id: string;
    title: string;
    spec_id?: string | null;
    source_table?: string | null;
    source_pk?: Record<string, string | number | boolean | null> | null;
    source_record?: Record<string, unknown> | null;
    source_table_url?: string | null;
    model_row?: MetricsModelRow | null;
    frozen_dataset?: FrozenDatasetRow | null;
    frozen_dataset_source_table?: string | null;
    frozen_dataset_source_pk?: Record<string, unknown> | null;
    metrics_rows: MetricsModelRow[];
    kpis: MetricsModelDetailKpi[];
    related: MetricsModelDetailRelated;
}

export interface FrozenDatasetCoverage {
    models_total: number;
    models_with_metrics: number;
    subsets: string[];
    best_metric_key?: string | null;
    best_metric_value?: number | null;
    best_model_id?: string | null;
}

export interface FrozenDatasetLinkedModelRow {
    model_id: string;
    created_at?: string | null;
    run_key?: string | null;
    run_id?: string | null;
    link_table?: string | null;
    link_record?: Record<string, unknown> | null;
}

export interface FrozenDatasetDetailResponse {
    pipeline_id: string;
    dataset_id: string;
    title: string;
    spec_id?: string | null;
    dataset: FrozenDatasetRow;
    source_table?: string | null;
    source_pk?: Record<string, unknown> | null;
    source_record?: Record<string, unknown> | null;
    source_table_url?: string | null;
    linked_models: FrozenDatasetLinkedModelRow[];
    /** @deprecated API ≤ linked_models migration; normalized client-side */
    models?: MetricsModelRow[];
    coverage: FrozenDatasetCoverage;
}

export interface MetricsRunsResponse {
    rows: MetricsModelRow[];
    total: number;
    available_filters: {
        subsets: string[];
        models: string[];
        tags: string[];
        metrics: string[];
    };
    schema: MetricsTableSchema;
}

export interface KpiItem {
    key: string;
    label: string;
    value: number | string | null;
    delta?: number | null;
    delta_pct?: number | null;
    trend?: { x: string; y: number | null }[];
    format?: "float" | "percent" | "integer" | "string";
    higher_is_better?: boolean;
    subtitle?: string;
}

export interface AnomalyItem {
    severity: "info" | "warning" | "critical";
    metric: string;
    title: string;
    description: string;
    run_id?: string;
    value?: number | null;
    expected?: number | null;
    delta?: number | null;
}

export interface MetricsSummaryResponse {
    pipeline_id: string;
    primary_metric: string;
    latest_run?: MetricsRunRow;
    best_run?: MetricsRunRow;
    previous_run?: MetricsRunRow;
    kpis: KpiItem[];
    anomalies: AnomalyItem[];
    has_metrics?: boolean;
}

export interface MetricsTimeseriesResponse {
    series: {
        key: string;
        label: string;
        metric: string;
        subset?: string;
        model_id?: string;
        points: { x: string; y: number | null; run_id?: string }[];
    }[];
}

export interface ClassMetricRow {
    label: string;
    subset?: string;
    model_id?: string;
    class_id?: string | number;
    images_support?: number;
    support?: number;
    TP?: number;
    FP?: number;
    FN?: number;
    precision?: number | null;
    recall?: number | null;
    f1_score?: number | null;
    iou_mean?: number | null;
    mAP50?: number | null;
    mAP50_95?: number | null;
    pose_P?: number | null;
    pose_R?: number | null;
    pose_mAP50?: number | null;
    pose_mAP50_95?: number | null;
    delta?: Record<string, number | null>;
    trend?: Record<string, { x: string; y: number | null }[]>;
}

export interface ClassMetricsResponse {
    rows: ClassMetricRow[];
    total: number;
    summary: {
        total_classes: number;
        images_count?: number;
        macro_f1?: number;
        weighted_f1?: number;
        best_classes?: ClassMetricRow[];
        worst_classes?: ClassMetricRow[];
    };
}

export interface ClassMetricDetailResponse {
    label: string;
    class_id?: string | number;
    latest: ClassMetricRow;
    previous?: ClassMetricRow;
    trends: { metric: string; points: { x: string; y: number | null; run_id?: string }[] }[];
    error_breakdown?: {
        false_negatives?: number;
        false_positives?: number;
        localization_errors?: number;
        confusions?: number;
        duplicates?: number;
        other?: number;
    };
    confusion_top?: { actual_label: string; predicted_label: string; count: number }[];
    gallery_url?: string;
}

export interface TrainingRunRow {
    run_key: string;
    run_id?: string;
    model_id?: string;
    task_type?: string;
    framework?: string;
    dataset?: string;
    started_at?: string;
    finished_at?: string;
    duration_s?: number;
    status: string;
    attempt?: number;
    tags?: string[];
    best_metric_name?: string;
    best_metric_value?: number;
    params?: Record<string, unknown>;
    artifacts?: Record<string, unknown>;
    is_best?: boolean;
}

export interface TrainingRunsResponse {
    rows: TrainingRunRow[];
    total: number;
    filters: {
        task_types: string[];
        frameworks: string[];
        datasets: string[];
        statuses: string[];
        tags: string[];
    };
}

export interface TrainingCurveMetric {
    key: string;
    label: string;
    group: string;
    higher_is_better?: boolean;
}

export interface TrainingCurveChart {
    metric: string;
    title: string;
    y_label?: string;
    x_label: string;
    series: {
        run_key: string;
        label: string;
        color_key?: string;
        points: { x: number | string; y: number | null }[];
    }[];
}

export interface TrainingCompareResponse {
    runs: TrainingRunRow[];
    available_metrics: TrainingCurveMetric[];
    charts: TrainingCurveChart[];
    run_keys?: string[];
    charts_legacy?: ChartSpec[];
}

export interface SqlColumn {
    name: string;
    type?: string;
}

export interface SqlSchemaResponse {
    datasource: string;
    tables: {
        name: string;
        columns: { name: string; type?: string }[];
        row_count?: number;
    }[];
}

export interface UiChartSpec {
    id: string;
    title: string;
    type: "line" | "area" | "bar" | "scatter" | "heatmap";
    x: { key: string; label: string };
    y: { key?: string; label: string };
    series: {
        key: string;
        label: string;
        points: { x: string | number; y: number | null; meta?: Record<string, unknown> }[];
    }[];
}

export interface MetricsListParams {
    subset?: string;
    model_id?: string;
    task_type?: string;
    from?: string;
    to?: string;
    tags?: string[];
    search?: string;
    sort_by?: string;
    sort_dir?: "asc" | "desc";
    limit?: number;
    offset?: number;
}

export interface ClassMetricsParams {
    run_id?: string;
    run_key?: string;
    model_id?: string;
    subset?: string;
    task_type?: string;
    threshold_iou?: number;
    label_search?: string;
    sort_by?: string;
    sort_dir?: "asc" | "desc";
    limit?: number;
    offset?: number;
}

export interface TrainingRunsParams {
    task_type?: string[];
    framework?: string[];
    dataset?: string;
    model_family?: string;
    status?: string[];
    tags?: string[];
    from?: string;
    to?: string;
    search?: string;
    sort_by?: string;
    sort_dir?: "asc" | "desc";
    limit?: number;
    offset?: number;
}

/* --- Custom training experiments types (spec §22) --- */

/**
 * Backend-computed action flags for one experiment. The UI must derive all
 * available actions from these flags and MUST NOT re-compute them from
 * ``source``/``active``/``requests_count``.
 */
export interface TrainingExperimentCapabilities {
    can_edit: boolean;
    can_delete: boolean;
    can_duplicate: boolean;
    can_launch: boolean;
    can_archive: boolean;
    lock_reason?: string | null;
}

export type TrainingExperimentSource = "builtin" | "custom" | string;

export interface TrainingExperimentRow {
    id: string;
    source: TrainingExperimentSource;
    display_name?: string | null;
    description?: string | null;
    config_type: string;
    params: Record<string, unknown>;
    config_hash?: string | null;
    active: boolean;
    revision: number;
    created_at?: string | null;
    updated_at?: string | null;
    /** Codec-provided display metadata. ``summary.display`` holds the main params. */
    summary: TrainingExperimentSummaryMeta;
    requests_count: number;
    runs_count: number;
    last_used_at?: string | null;
    capabilities: TrainingExperimentCapabilities;
    /** Deprecated aliases still emitted by the backend for older clients. */
    is_active?: boolean;
    archived?: boolean;
}

export interface TrainingExperimentSummaryMeta {
    /** Human-readable key/value pairs of the most important params. */
    display?: Array<{ label: string; value: unknown }> | Record<string, unknown>;
    [key: string]: unknown;
}

export interface TrainingExperimentsSummary {
    total: number;
    builtin: number;
    custom: number;
    editable: number;
    locked: number;
    archived: number;
}

export interface TrainingExperimentsResponse {
    rows: TrainingExperimentRow[];
    total: number;
    summary: TrainingExperimentsSummary;
    filters?: {
        sources?: string[];
        states?: string[];
        [key: string]: unknown;
    };
}

export interface TrainingExperimentDetailResponse {
    experiment: TrainingExperimentRow;
    models?: TrainingExperimentModelRow[];
}

export interface TrainingExperimentModelRow {
    model_id: string;
    frozen_dataset_id: string;
    frozen_dataset_display_name?: string | null;
    created_at?: string | null;
    run_key?: string | null;
    link_table?: string | null;
}

export interface TrainingExperimentModelsResponse {
    experiment: TrainingExperimentRow;
    models: TrainingExperimentModelRow[];
    total: number;
}

export interface TrainingExperimentsListParams {
    search?: string;
    source?: string;
    state?: "active" | "archived" | string;
    include_archived?: boolean;
    order?: string;
    limit?: number;
    offset?: number;
}

/** JSON schema for the config-type params form. */
export interface TrainConfigSchemaResponse {
    config_type: string;
    /** JSON Schema. Backend may send it as ``schema`` or ``json_schema``. */
    schema: Record<string, unknown>;
}

export interface CreateTrainingExperimentPayload {
    display_name: string;
    description?: string | null;
    params: Record<string, unknown>;
}

export interface UpdateTrainingExperimentPayload {
    display_name: string;
    description?: string | null;
    params: Record<string, unknown>;
    expected_revision: number;
}

export interface DuplicateTrainingExperimentPayload {
    display_name?: string | null;
    description?: string | null;
}

export type TrainingRequestState = "queued" | "running" | "success" | "failed" | string;

export interface TrainingRequest {
    id: string;
    kind: string;
    state: TrainingRequestState;
    frozen_dataset_id?: string | null;
    train_config_id: string;
    config_name?: string | null;
    config_hash?: string | null;
    enabled?: boolean;
    force?: boolean;
    max_within_time?: string | null;
    config_source?: string | null;
    config_params_snapshot?: Record<string, unknown>;
    requested_at?: string | null;
    requested_by?: string | null;
    client_request_id?: string | null;
}

export interface TrainingRequestListRow {
    id: string;
    kind: string;
    state: TrainingRequestState;
    frozen_dataset_id?: string | null;
    train_config_id: string;
    config_name?: string | null;
    requested_at?: string | null;
    force?: boolean;
    enabled?: boolean;
    run_key?: string | null;
    model_id?: string | null;
    status?: string | null;
    started_at?: string | null;
    can_delete?: boolean;
}

export interface TrainingRequestsListResponse {
    rows: TrainingRequestListRow[];
    total: number;
}

export interface LaunchInfo {
    started: boolean;
    run_id?: string | null;
}

export interface CreateTrainingRequestPayload {
    frozen_dataset_id: string;
    train_config_id: string;
    client_request_id: string;
    launch?: boolean;
    /** Bypass max_within_time and allow training on a stale frozen dataset. */
    force?: boolean;
}

export interface CreateTrainingRequestResponse {
    request: TrainingRequest;
    launch?: LaunchInfo | null;
}

export interface LaunchResponse {
    training_request_id: string;
    started: boolean;
    run_id?: string | null;
}

export interface FreezeLaunchResponse {
    started: boolean;
    run_id?: string | null;
}

/* --- Ops image views types --- */

export interface OpsBBoxRow {
    label?: string | null;
    confidence?: number | null;
    x1?: number | null;
    y1?: number | null;
    x2?: number | null;
    y2?: number | null;
}

export interface OpsImageRecordListRow {
    record_key: string;
    pk: Record<string, unknown>;
    preview_url?: string | null;
    visualization_url?: string | null;
    detail_url?: string | null;
    subset?: string | null;
    bbox_count?: number | null;
    gt_bbox_count?: number | null;
    prediction_bbox_count?: number | null;
    metrics?: Record<string, unknown> | null;
}

export interface OpsImageRecordsResponse {
    pipeline_id: string;
    spec_id: string;
    scope: "data" | "frozen_dataset" | "model_prediction";
    parent_id?: string | null;
    primary_key_columns: string[];
    list_columns?: string[];
    rows: OpsImageRecordListRow[];
    total: number | null;
    limit: number;
    offset: number;
}

export interface OpsImageRecordsCountResponse {
    pipeline_id: string;
    spec_id: string;
    scope: "data" | "frozen_dataset" | "model_prediction";
    parent_id?: string | null;
    total: number;
}

export interface OpsImageRecordDetailResponse {
    pipeline_id: string;
    spec_id: string;
    scope: "data" | "frozen_dataset" | "model_prediction";
    parent_id?: string | null;

    record_key: string;
    pk: Record<string, unknown>;
    record: Record<string, unknown>;

    image_url?: string | null;
    subset?: string | null;

    preview_url?: string | null;
    visualization_url?: string | null;
    plain_image_url?: string | null;

    gt_visualization_url?: string | null;
    prediction_visualization_url?: string | null;
    plain_gt_image_url?: string | null;
    plain_prediction_image_url?: string | null;

    bbox_count?: number | null;
    gt_bbox_count?: number | null;
    prediction_bbox_count?: number | null;

    bbox_rows?: OpsBBoxRow[];
    gt_bbox_rows?: OpsBBoxRow[];
    prediction_bbox_rows?: OpsBBoxRow[];

    index?: number | null;
    total?: number | null;
    prev_record_key?: string | null;
    next_record_key?: string | null;
}
