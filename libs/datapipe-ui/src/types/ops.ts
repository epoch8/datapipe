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
    /** Highest seq known for this run (DB + in-memory). Used to jump to the tail. */
    max_seq?: number;
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
    pipeline_id?: string;
}

export interface ResetTransformMetadataResponse {
    transform_name: string;
    status: string;
}

export interface SettingsInfo {
    pipeline_id?: string;
    observability_db_connected: boolean;
    version: string;
}
