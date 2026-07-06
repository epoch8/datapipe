export type Health =
    | "healthy"
    | "running"
    | "training"
    | "degraded"
    | "failed";

export interface PrimaryAction {
    type: "view_details" | "view_training" | "view_run";
    target: string;
}

export interface TrainingPreview {
    run_key?: string;
    epoch?: number;
    total_epochs?: number;
    heartbeat_age_s?: number;
    status?: string;
}

export interface OverviewCard {
    pipeline_id: string;
    display_name: string;
    task_type?: string;
    health: Health;
    last_run_at?: string;
    last_run_age?: string;
    last_run_id?: string;
    last_run_status?: string;
    next_run_at?: string;
    error_snippet?: string;
    training_preview?: TrainingPreview;
    primary_action: PrimaryAction;
    enrichments?: Enrichment[];
}

export interface OverviewResponse {
    pipelines: OverviewCard[];
    count: number;
}

export interface ChartSeries {
    key: string;
    label: string;
    data: { x: number | string; y: number }[];
}

export interface ChartSpec {
    chart_id: string;
    title: string;
    chart_type: "line";
    x: { field: string; label: string };
    y: { label: string };
    series: ChartSeries[];
}

export interface Enrichment {
    type: string;
    payload: Record<string, unknown>;
}

export type LabelSegment = {
    label_id: string;
    start_order: number;
    end_order: number;
    step_ids: string[];
};

export type LabelGraphNodeKind = "label" | "container" | "interleaved-group";

export type LabelGraphNode = {
    id: string;
    label: string;
    status: string;
    kind: LabelGraphNodeKind;
    step_ids: string[];
    step_count: number;
    parent_id?: string | null;
    children_ids?: string[];
    order_min: number;
    order_max: number;
    segments: LabelSegment[];
};

export type LabelGraphEdgeKind = "order" | "exact-order" | "secondary";

export type LabelGraphEdge = {
    id: string;
    source: string;
    target: string;
    kind: LabelGraphEdgeKind;
    visible_by_default: boolean;
    show_when_selected?: string[];
    replaces_edge_id?: string;
    source_scope?: "node" | "container" | "child";
    target_scope?: "node" | "container" | "child";
};

export type LabelContainment = {
    parent: string;
    child: string;
    kind: "semantic" | "explicit" | "heuristic";
};

export type LabelSharedRelation = {
    id: string;
    a: string;
    b: string;
    shared_step_ids: string[];
    shared_count: number;
    visible_by_default: boolean;
};

export type LabelInterleaving = {
    id: string;
    labels: string[];
    segments: LabelSegment[];
    switch_count: number;
    visible_by_default: boolean;
};

export type LabelGraphPayload = {
    label_key: string;
    nodes: LabelGraphNode[];
    edges: LabelGraphEdge[];
    containments: LabelContainment[];
    shared_relations: LabelSharedRelation[];
    interleavings: LabelInterleaving[];
};

export type StageItem = { stage: string; status: string; steps: unknown[] };
export type StageEdge = { from: string; to: string; count?: number };

export interface PipelineDetail {
    pipeline_id: string;
    display_name: string;
    task_type?: string;
    health: string;
    stages: StageItem[];
    stage_edges?: StageEdge[];
    label_graph?: LabelGraphPayload;
    recent_runs: RecentRunSummary[];
    next_run_at?: string;
    last_error?: string;
    enrichments?: Enrichment[];
    agent_mode: boolean;
}

export interface RecentRunSummary {
    run_id: string;
    status: string;
    started_at?: string;
    finished_at?: string;
    trigger?: string;
}

export interface RunListRow {
    run_id: string;
    pipeline_id: string;
    status: string;
    scope: "full_pipeline" | "stage_run" | "label_run";
    target_label?: string;
    started_at?: string;
    finished_at?: string;
    duration_s?: number;
    trigger?: string;
}

export interface RunsListResponse {
    rows: RunListRow[];
    total: number;
    filters: {
        statuses: string[];
        stages: string[];
        triggers: string[];
    };
    counts_by_status?: Record<string, number>;
}

export interface RunsListParams {
    pipeline_id?: string;
    status?: string;
    stage?: string;
    trigger?: string;
    from?: string;
    to?: string;
    search?: string;
    limit?: number;
    offset?: number;
    sort_by?: "started_at" | "duration" | "status" | "stage";
    sort_dir?: "asc" | "desc";
}

export interface StageRecentRunsResponse {
    pipeline_id: string;
    stage: string;
    recent_runs: RecentRunSummary[];
}

export interface RunLogLine {
    seq: number;
    logged_at: string;
    level: string;
    message: string;
}

export interface RunLogsResponse {
    run_id: string;
    lines: RunLogLine[];
    last_seq: number;
}

export interface RunDetail {
    run_id: string;
    pipeline_id: string;
    status: string;
    started_at?: string;
    finished_at?: string;
    error?: string;
    trigger?: string;
    run_scope?: "full_pipeline" | "stage_run" | "label_run";
    target_labels?: [string, string][];
    target_label_display?: string;
    steps: {
        step_name: string;
        status: string;
        started_at?: string;
        finished_at?: string;
        processed?: number;
        total?: number;
        error?: string;
    }[];
}

export interface Capabilities {
    ml_metrics: boolean;
    ml_training: boolean;
    mode: string;
    pipeline_id?: string;
}

export interface ResetTransformMetadataResponse {
    transform_name: string;
    status: string;
}

export interface SettingsInfo {
    pipeline_id?: string;
    mode: string;
    observability_db_connected: boolean;
    version: string;
}

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
}

export interface FrozenDatasetsResponse {
    rows: FrozenDatasetRow[];
    total: number;
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

export interface SqlQueryRequest {
    sql: string;
    limit?: number;
    offset?: number;
    datasource?: string;
}

export interface SqlQueryResponse {
    columns: SqlColumn[];
    rows: Record<string, unknown>[];
    total?: number;
    execution_ms: number;
    truncated?: boolean;
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
