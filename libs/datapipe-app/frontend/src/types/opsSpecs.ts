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
    default_filters?: OpsFilterRule[];
    default_sort: [string, "asc" | "desc"][];
    entity_links?: Record<string, string>;
};

export type OpsFrozenDatasetSpecPayload = {
    table: string;
    id_column: string;
    created_at_column: string;
    display_name_column?: string | null;
    label_mode?: string;
    split_columns?: Record<string, string>;
    models_count_relation_id?: string | null;
};

export type OpsModelSpecPayload = {
    table: string;
    id_column: string;
    display_name_column?: string | null;
    created_at_column?: string | null;
    artifact_uri_column?: string | null;
    is_best_table?: string | null;
    is_best_column?: string | null;
};

export type OpsTrainingSpecPayload = {
    status_table: string;
    model_id_column: string;
    status_column: string;
    started_at_column?: string | null;
    finished_at_column?: string | null;
    duration_seconds_column?: string | null;
    artifact_columns?: Record<string, string>;
    extra_columns?: OpsColumn[];
};

export type OpsRelationSpecPayload = {
    id: string;
    table: string;
    from_entity: string;
    from_column: string;
    to_entity: string;
    to_column: string;
};

export type OpsSpecDetail = OpsSpecSummary & {
    metrics: OpsTableSchema[];
    class_metrics: OpsTableSchema[];
    frozen_dataset?: OpsFrozenDatasetSpecPayload | null;
    model?: OpsModelSpecPayload | null;
    training?: OpsTrainingSpecPayload | null;
    relations?: OpsRelationSpecPayload[];
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

export type OpsFilterOperator =
    | "contains"
    | "not_contains"
    | "regex"
    | "equal"
    | "not_equal"
    | "is_empty";

export type OpsFilterMode = "or" | "and";

export type OpsFilterRule = {
    id?: string;
    column_id: string;
    operator: OpsFilterOperator;
    value?: string;
};

export type OpsRowsParams = {
    search?: string;
    sort_by?: string;
    sort_dir?: "asc" | "desc";
    limit?: number;
    offset?: number;
    filter_mode?: OpsFilterMode;
    filters?: OpsFilterRule[];
    /** @deprecated use filters */
    model?: string[] | string;
    /** @deprecated use filters */
    subset?: string[] | string;
};
