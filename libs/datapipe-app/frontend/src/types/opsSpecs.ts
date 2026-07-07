export type OpsColumnKind = "text" | "number" | "datetime" | "duration" | "status" | "link" | "chip";

export type OpsColumn = {
    id: string;
    label: string;
    source: string;
    kind?: OpsColumnKind;
    fmt?: string | null;
    sortable?: boolean;
    filterable?: boolean;
    link_to?: string | null;
    width?: number | null;
};

export type OpsColumnGroup = {
    label: string;
    columns: OpsColumn[];
};

export type OpsMetricColumn = OpsColumn | OpsColumnGroup;

export type OpsSpecSummary = {
    id: string;
    title: string;
    description: string;
    icon: string;
    color: string;
    has_frozen_datasets: boolean;
    has_training: boolean;
    metric_tables_count: number;
    class_metric_tables_count: number;
    tags?: string[];
};

export type OpsSpecsResponse = {
    pipeline_id: string;
    specs: OpsSpecSummary[];
};

export type OpsTableSchema = {
    id: string;
    title: string;
    table: string;
    metric_source: string;
    primary_columns: OpsColumn[];
    metric_columns: OpsMetricColumn[];
    filters: OpsColumn[];
    default_sort: [string, "asc" | "desc"][];
    entity_links?: Record<string, string>;
};

export type OpsSpecDetail = OpsSpecSummary & {
    metrics: OpsTableSchema[];
    class_metrics: OpsTableSchema[];
};

export type OpsOverviewResponse = {
    summary: Record<string, unknown>;
    filters?: Record<string, unknown>;
    specs: Array<OpsSpecSummary & Record<string, unknown>>;
    recent_activity?: unknown[];
    lifecycle?: unknown[];
    [key: string]: unknown;
};

export type OpsRowsResponse = {
    rows: Record<string, unknown>[];
    total: number;
    table?: OpsTableSchema;
};

export type OpsRowsParams = {
    search?: string;
    sort_by?: string;
    sort_dir?: "asc" | "desc";
    limit?: number;
    offset?: number;
};
