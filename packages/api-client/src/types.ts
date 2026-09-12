/** Shared Ops / pipeline API types (from datapipe-ui + CapabilitiesResponse). */

export interface AddonCapability {
    name: string;
    features?: Record<string, unknown>;
}

/**
 * Feature flags from `/capabilities`.
 * Hosts gate UI actions on these; server auth remains authoritative.
 */
export interface Capabilities {
    graph: boolean;
    table_data: boolean;
    table_meta: boolean;
    transform_meta: boolean;
    run_history: boolean;
    run_start: boolean;
    run_stop: boolean;
    run_logs: boolean;
    transform_run: boolean;
    transform_reset: boolean;
    addons: AddonCapability[];
    /** @deprecated Kept optional for plugin code that still reads these flags. */
    ml_metrics?: boolean;
    /** @deprecated Kept optional for plugin code that still reads these flags. */
    ml_training?: boolean;
    /** @deprecated Removed from cloud API. */
    pipeline_id?: string;
    /** @deprecated Removed from cloud API. */
    run_logs_configured?: boolean;
}

export interface SettingsInfo {
    version: string;
    /** @deprecated Removed from cloud API. */
    pipeline_id?: string;
    /** @deprecated Removed from cloud API. */
    observability_db_connected?: boolean;
    /** @deprecated Removed from cloud API. */
    run_logs_configured?: boolean;
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
    stages: StageItem[];
    stage_edges?: StageEdge[];
    label_graph?: LabelGraphPayload;
    available_label_keys?: string[];
    /** @deprecated Cloud overview has no runs store. */
    recent_runs?: RecentRunSummary[];
    /** @deprecated Removed from cloud API. */
    pipeline_id?: string;
    /** @deprecated Removed from cloud API. */
    display_name?: string;
    task_type?: string;
    health?: string;
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

export interface ResetTransformMetadataResponse {
    transform_name: string;
    status: string;
}

export interface StartRunResponse {
    run_id: string;
    status: string;
}

export interface StopRunResponse {
    run_id: string;
    status: string;
    stopped: boolean;
}

/** Graph / table catalog types (pipeline graph endpoint). */
export interface TableColumn {
    name: string;
    type: string;
}

export interface PipeTable {
    id?: string;
    name?: string;
    indexes: string[];
    size?: number | null;
    store_class: string;
    schema?: TableColumn[];
    type?: string;
}

export interface TransformNode {
    id?: string;
    name: string;
    type: "transform";
    inputs: string[];
    outputs: string[];
    labels?: string[][];
    transform_type?: string;
    indexes?: string[];
    transform_primary_keys?: string[];
    tpk?: string[];
    primary_keys?: string[];
    has_transform_meta?: boolean;
    total_idx_count?: number;
    changed_idx_count?: number;
}

export interface MetaNode {
    id?: string;
    name: string;
    type: "meta";
    graph: GraphData;
    inputs?: string[];
    outputs?: string[];
    transform_type?: string;
    transform_primary_keys?: string[];
    tpk?: string[];
    labels?: string[][];
}

export type GraphNode = MetaNode | TransformNode;

export interface GraphData {
    catalog: Record<string, PipeTable>;
    pipeline: GraphNode[];
    stages?: string[];
}

export interface FocusFilter {
    table_name: string;
    items_idx: Record<string, string | number>[];
}

export interface GetDataRequest {
    table: string;
    page: number;
    page_size: number;
    include_total?: boolean;
    focus?: FocusFilter;
    filters?: Record<string, string | number>;
    order_by?: string;
    order?: "asc" | "desc";
}

export interface GetDataResponse {
    page: number;
    page_size: number;
    total: number;
    data: Record<string, unknown>[];
}

export interface GetGraphOptions {
    stage?: string | null;
    label_key?: string | null;
}

export interface GetPipelineOptions {
    label_key?: string;
}
