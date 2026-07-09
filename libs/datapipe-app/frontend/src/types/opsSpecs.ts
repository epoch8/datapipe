export type OpsColumnKind = "text" | "number" | "datetime" | "duration" | "status" | "link" | "chip" | "split" | "models_count";

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
    has_image: boolean;
    has_model_predictions: boolean;
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

export type OpsImageOverlayRole = "gt" | "prediction" | "record";

export type OpsImageAnnotationSpecPayload = {
    table: string;
    primary_key_columns: string[];
    bboxes_column?: string | null;
    labels_column?: string | null;
    scores_column?: string | null;
    annotation_column?: string | null;
    join_columns?: Record<string, string>;
    role?: OpsImageOverlayRole;
};

export type OpsImageDataSpecPayload = {
    kind: "image";
    image_table: string;
    image_primary_key_columns: string[];
    image_url_column: string;
    subset_table?: string | null;
    subset_join_columns?: Record<string, string>;
    subset_column?: string | null;
    ground_truth?: OpsImageAnnotationSpecPayload | null;
    visualizer?: string | null;
    preview_size?: number;
    modal_max_side?: number;
    detail_max_side?: number;
};

export type OpsImageRecordViewSpecPayload = {
    kind: "image";
    table: string;
    scope_column?: string | null;
    primary_key_columns: string[];
    image_url_column?: string | null;
    image_url_table?: string | null;
    image_url_join_columns?: Record<string, string>;
    bboxes_column?: string | null;
    labels_column?: string | null;
    scores_column?: string | null;
    width_column?: string | null;
    height_column?: string | null;
    visualizer?: string | null;
    preview_size?: number;
    modal_max_side?: number;
    detail_max_side?: number;
};

export type OpsImagePredictionViewSpecPayload = {
    kind: "image";
    table: string;
    model_id_column: string;
    image_primary_key_columns: string[];
    image_url_table: string;
    image_url_column: string;
    image_url_join_columns?: Record<string, string>;
    prediction?: OpsImageAnnotationSpecPayload | null;
    ground_truth?: OpsImageAnnotationSpecPayload | null;
    subset_table?: string | null;
    subset_join_columns?: Record<string, string>;
    subset_column?: string | null;
    metrics_on_image?: OpsTableSchema | null;
    metrics_on_image_label?: string;
    visualizer?: string | null;
    preview_size?: number;
    modal_max_side?: number;
    detail_max_side?: number;
};

export type OpsTextRecordViewSpecPayload = {
    kind: "text";
    table: string;
    scope_column?: string | null;
    primary_key_columns: string[];
};

export type OpsDataSpecPayload = {
    tables: string[];
    item_table?: string | null;
    label_table?: string | null;
    subset_table?: string | null;
    tag_table?: string | null;
    image_view?: OpsImageDataSpecPayload | null;
};

export type OpsFrozenDatasetSpecPayload = {
    table: string;
    id_column: string;
    created_at_column: string;
    display_name_column?: string | null;
    label_mode?: string;
    split_columns?: Record<string, string>;
    models_count_relation_id?: string | null;
    record_view?: OpsImageRecordViewSpecPayload | OpsTextRecordViewSpecPayload | null;
    columns?: OpsColumn[];
    default_sort?: [string, "asc" | "desc"][];
};

export type OpsModelSpecPayload = {
    table: string;
    id_column: string;
    display_name_column?: string | null;
    created_at_column?: string | null;
    artifact_uri_column?: string | null;
    is_best_table?: string | null;
    is_best_column?: string | null;
    prediction_view?: OpsImagePredictionViewSpecPayload | null;
};

export type OpsTrainingSpecPayload = {
    status_table: string;
    artifact_columns?: Record<string, string>;
    columns?: OpsColumn[];
    default_sort?: [string, "asc" | "desc"][];
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
    data?: OpsDataSpecPayload | null;
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
    columns?: OpsColumn[];
    filter_columns?: OpsColumn[];
    entity_links?: Record<string, string>;
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
