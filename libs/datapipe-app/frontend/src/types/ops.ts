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

export interface PipelineDetail {
    pipeline_id: string;
    display_name: string;
    task_type?: string;
    health: string;
    stages: { stage: string; status: string; steps: unknown[] }[];
    stage_edges?: { from: string; to: string; count?: number }[];
    recent_runs: {
        run_id: string;
        status: string;
        started_at?: string;
        finished_at?: string;
    }[];
    next_run_at?: string;
    last_error?: string;
    enrichments?: Enrichment[];
    agent_mode: boolean;
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

export interface SettingsInfo {
    pipeline_id?: string;
    mode: string;
    observability_db_connected: boolean;
    version: string;
}
