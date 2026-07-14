import React from "react";
import { Link } from "react-router-dom";
import { Button, Select, Space, Table, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import { opsApi } from "../../../api/ops";
import type { OpsImageRecordListRow, OpsImageRecordsResponse } from "../../../types/ops";
import type { OpsColumn, OpsMetricColumn, OpsRowsParams, OpsSpecDetail, OpsTableSchema, OpsImageDataSpecPayload } from "../../../types/opsSpecs";
import { EmptyState, TableSizeControl } from "../shared";
import { TableFilterBar } from "../shared/TableFilterBar";
import {
    expandChipValueRules,
    formatRule,
    dedupeFilterColumns,
    serializeFilterRules,
    type OpsFilterState,
} from "../shared/tableFilters";
import { IMAGE_RECORD_ENTITY_LINKS, imageRecordFilterColumns } from "../shared/imageRecordFilters";
import {
    defaultSortState,
    resolveSortFromTableChange,
    serverSideSorter,
    sortOrderForColumn,
    type OpsTableSortState,
} from "../shared/opsTableSort";
import type { SortOrder } from "antd/es/table/interface";
import { ImageRecordPreviewModal } from "./ImageRecordPreviewModal";

type Scope = "data" | "frozen_dataset" | "model_prediction";

const PAGE_SIZE_OPTIONS = [5, 10, 20, 50, 100];

function formatBoxCount(value: number | null | undefined): string {
    const n = value ?? 0;
    if (n === 1) return "1 box";
    return `${n} boxes`;
}

function isMetricColumnGroup(column: OpsMetricColumn): column is { label: string; columns: OpsColumn[] } {
    return "columns" in column;
}

function flattenMetricColumns(columns: OpsMetricColumn[] | undefined): OpsColumn[] {
    const flat: OpsColumn[] = [];
    for (const column of columns ?? []) {
        if (isMetricColumnGroup(column)) {
            flat.push(...column.columns);
        } else {
            flat.push(column);
        }
    }
    return flat;
}

function formatMetricValue(value: unknown): string {
    if (value === null || value === undefined || value === "") return "-";
    if (typeof value === "number") {
        return Number.isInteger(value) ? value.toLocaleString() : value.toFixed(3);
    }
    return String(value);
}

function imageRecordSortTable(filterColumns: OpsColumn[]): OpsTableSchema {
    return {
        id: "image-records",
        title: "Images",
        table: "images",
        metric_source: "",
        primary_columns: filterColumns,
        metric_columns: [],
        filters: filterColumns,
        default_sort: [["image_name", "asc"]],
    };
}

function sortColumnProps(column: OpsColumn | undefined, sortState: OpsTableSortState) {
    if (!column?.sortable) return {};
    return {
        sorter: serverSideSorter(column),
        sortDirections: ["ascend", "descend"] as SortOrder[],
        sortOrder: sortOrderForColumn(column, sortState),
    };
}

function findFilterColumn(columns: OpsColumn[], id: string): OpsColumn | undefined {
    return columns.find((column) => column.id === id);
}

function useDebouncedValue<T>(value: T, delayMs: number): T {
    const [debounced, setDebounced] = React.useState(value);
    React.useEffect(() => {
        const timer = window.setTimeout(() => setDebounced(value), delayMs);
        return () => window.clearTimeout(timer);
    }, [value, delayMs]);
    return debounced;
}

function listQueryParams(
    params: OpsRowsParams & { limit: number; offset: number; include_total?: boolean },
): OpsRowsParams & { limit: number; offset: number; include_total?: boolean } {
    return params;
}

async function fetchRecords(
    pipelineId: string,
    specId: string,
    scope: Scope,
    parentId: string | undefined,
    params: OpsRowsParams & { limit: number; offset: number },
): Promise<OpsImageRecordsResponse> {
    const query = listQueryParams({ ...params, include_total: false });
    if (scope === "data") {
        return opsApi.getImageRecords(pipelineId, specId, query);
    }
    if (scope === "frozen_dataset") {
        if (!parentId) throw new Error("parentId is required for frozen_dataset scope");
        return opsApi.getFrozenDatasetRecordRows(pipelineId, specId, parentId, query);
    }
    if (!parentId) throw new Error("parentId is required for model_prediction scope");
    return opsApi.getModelPredictionRows(pipelineId, specId, parentId, query);
}

async function fetchRecordCount(
    pipelineId: string,
    specId: string,
    scope: Scope,
    parentId: string | undefined,
    params: OpsRowsParams,
): Promise<number> {
    if (scope === "data") {
        const res = await opsApi.getImageRecordsCount(pipelineId, specId, params);
        return res.total;
    }
    if (scope === "frozen_dataset") {
        if (!parentId) throw new Error("parentId is required for frozen_dataset scope");
        const res = await opsApi.getFrozenDatasetRecordsCount(pipelineId, specId, parentId, params);
        return res.total;
    }
    if (!parentId) throw new Error("parentId is required for model_prediction scope");
    const res = await opsApi.getModelPredictionRecordsCount(pipelineId, specId, parentId, params);
    return res.total;
}

export function ImageRecordsTable({
    pipelineId,
    specId,
    scope,
    parentId,
    title,
    mode,
    imageView,
}: {
    pipelineId: string;
    specId: string;
    scope: Scope;
    parentId?: string;
    title?: string;
    mode: "image" | "frozen_dataset" | "prediction";
    imageView?: OpsImageDataSpecPayload | null;
}) {
    const filterColumns = React.useMemo(
        () => dedupeFilterColumns(imageRecordFilterColumns(mode, imageView)),
        [mode, imageView],
    );
    const sortTable = React.useMemo(() => imageRecordSortTable(filterColumns), [filterColumns]);
    const [filterState, setFilterState] = React.useState<OpsFilterState>({ mode: "or", rules: [] });
    const [sortState, setSortState] = React.useState<OpsTableSortState>(() => defaultSortState(sortTable));
    const debouncedSearch = useDebouncedValue(filterState.search ?? "", 300);
    const debouncedRules = useDebouncedValue(filterState.rules, 300);

    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [rows, setRows] = React.useState<OpsImageRecordListRow[]>([]);

    const [page, setPage] = React.useState(1);
    const [pageSize, setPageSize] = React.useState(10);
    const [recordCount, setRecordCount] = React.useState<number | null>(null);
    const [counting, setCounting] = React.useState(false);
    const [countError, setCountError] = React.useState<string | null>(null);

    const [previewRecordKey, setPreviewRecordKey] = React.useState<string | null>(null);
    const [spec, setSpec] = React.useState<OpsSpecDetail | null>(null);

    React.useEffect(() => {
        if (mode !== "prediction") {
            setSpec(null);
            return;
        }
        opsApi
            .getOpsSpec(pipelineId, specId)
            .then(setSpec)
            .catch(() => setSpec(null));
    }, [mode, pipelineId, specId]);

    const predictionMetricColumns = React.useMemo(
        () => flattenMetricColumns(spec?.model?.prediction_view?.metrics_on_image?.metric_columns),
        [spec],
    );

    React.useEffect(() => {
        setSortState(defaultSortState(sortTable));
    }, [sortTable]);

    const filterQuery = React.useMemo(
        () => ({
            ...sortState,
            search: debouncedSearch.trim() || undefined,
            filter_mode: filterState.mode,
            filters: serializeFilterRules(expandChipValueRules(debouncedRules, filterColumns), filterColumns),
        }),
        [debouncedRules, debouncedSearch, filterColumns, filterState.mode, sortState],
    );

    const load = React.useCallback(() => {
        setLoading(true);
        setError(null);
        const limit = pageSize;
        const offset = (page - 1) * pageSize;

        fetchRecords(pipelineId, specId, scope, parentId, { limit, offset, ...filterQuery })
            .then((res) => setRows(res.rows))
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, specId, scope, parentId, page, pageSize, filterQuery]);

    const countSize = React.useCallback(() => {
        setCounting(true);
        setCountError(null);
        fetchRecordCount(pipelineId, specId, scope, parentId, filterQuery)
            .then(setRecordCount)
            .catch((e) => setCountError(String(e)))
            .finally(() => setCounting(false));
    }, [pipelineId, specId, scope, parentId, filterQuery]);

    React.useEffect(() => {
        setPage(1);
        setRecordCount(null);
        setCountError(null);
    }, [pipelineId, specId, scope, parentId, debouncedSearch, debouncedRules, filterState.mode, sortState]);

    React.useEffect(() => {
        load();
    }, [load]);

    const appliedRules = serializeFilterRules(
        expandChipValueRules(filterState.rules, filterColumns),
        filterColumns,
    );

    const hasMore = recordCount != null ? page * pageSize < recordCount : rows.length >= pageSize;
    const totalPages = recordCount != null ? Math.max(1, Math.ceil(recordCount / pageSize)) : null;

    const columns: ColumnsType<OpsImageRecordListRow> = React.useMemo(() => {
        const previewCol = {
            title: "Preview",
            key: "preview",
            width: 88,
            render: (_v: unknown, row: OpsImageRecordListRow) => (
                <img
                    className="ops-record-preview-thumb"
                    src={row.preview_url ?? undefined}
                    alt={String(row.pk.image_name ?? "")}
                    onClick={() => setPreviewRecordKey(row.record_key)}
                />
            ),
        };
        const viewCol = {
            title: "View",
            key: "view",
            width: 72,
            render: (_v: unknown, row: OpsImageRecordListRow) =>
                row.detail_url ? <Link to={row.detail_url}>View</Link> : null,
        };
        const bboxesCol = {
            title: "bboxes",
            key: "bboxes",
            render: (_v: unknown, row: OpsImageRecordListRow) => (
                <span className="ops-bboxes-chip">{formatBoxCount(row.bbox_count ?? null)}</span>
            ),
        };

        if (mode === "image") {
            const showSubset = imageView?.records_show_subset ?? false;
            const showGroundTruth = imageView?.records_show_ground_truth ?? false;
            const cols: ColumnsType<OpsImageRecordListRow> = [
                previewCol,
                {
                    title: "image_name",
                    key: "image_name",
                    ...sortColumnProps(findFilterColumn(filterColumns, "image_name"), sortState),
                    render: (_v, row) => String(row.pk.image_name ?? ""),
                },
            ];
            if (showSubset) {
                cols.push({
                    title: "subset",
                    key: "subset",
                    ...sortColumnProps(findFilterColumn(filterColumns, "subset"), sortState),
                    render: (_v, row) => (row.subset ? <Tag className="ops-soft-chip">{row.subset}</Tag> : "-"),
                });
            }
            if (showGroundTruth) {
                cols.push(bboxesCol);
            }
            cols.push(viewCol);
            return cols;
        }

        if (mode === "frozen_dataset") {
            return [
                previewCol,
                {
                    title: "image_name",
                    key: "image_name",
                    ...sortColumnProps(findFilterColumn(filterColumns, "image_name"), sortState),
                    render: (_v, row) => String(row.pk.image_name ?? ""),
                },
                {
                    title: "subset_id",
                    key: "subset_id",
                    ...sortColumnProps(findFilterColumn(filterColumns, "subset_id"), sortState),
                    render: (_v, row) =>
                        row.pk.subset_id || row.subset ? (
                            <Tag className="ops-soft-chip">{String(row.pk.subset_id ?? row.subset)}</Tag>
                        ) : (
                            "-"
                        ),
                },
                bboxesCol,
                viewCol,
            ];
        }

        const metricCols = predictionMetricColumns.map((column) => ({
            title: column.label,
            key: column.id,
            width: column.width ?? undefined,
            render: (_v: unknown, row: OpsImageRecordListRow) =>
                formatMetricValue(row.metrics?.[column.source] ?? row.metrics?.[column.id]),
        }));

        return [
            previewCol,
            {
                title: "image_name",
                key: "image_name",
                ...sortColumnProps(findFilterColumn(filterColumns, "image_name"), sortState),
                render: (_v, row) => String(row.pk.image_name ?? ""),
            },
            bboxesCol,
            ...metricCols,
            viewCol,
        ];
    }, [mode, imageView, predictionMetricColumns, filterColumns, sortState]);

    return (
        <div className="ops-panel ops-polished-panel ops-spec-table-panel">
            <TableFilterBar
                columns={filterColumns}
                entityLinks={IMAGE_RECORD_ENTITY_LINKS}
                value={filterState}
                onChange={setFilterState}
                searchPlaceholder="Search images..."
            />
            <div className="ops-data-table-toolbar">
                {title ? <div className="ops-data-table-title">{title}</div> : <div />}
                <dl className="ops-image-records-size-dl">
                    <dt>Size</dt>
                    <dd>
                        <TableSizeControl size={recordCount} loading={counting} onCount={countSize} />
                    </dd>
                </dl>
            </div>
            {appliedRules.length ? (
                <div className="ops-table-filter-summary">
                    Filtered by: {appliedRules.map((rule) => formatRule(rule, filterColumns)).join(", ")}
                </div>
            ) : null}
            {countError ? <div className="ops-image-records-count-error">{countError}</div> : null}
            <EmptyState loading={loading} error={error ?? undefined} empty={!rows.length && !loading} keepChildrenWhileLoading>
                <Table
                    className="ops-records-table ops-table"
                    size="middle"
                    columns={columns}
                    dataSource={rows}
                    rowKey={(r) => r.record_key}
                    pagination={false}
                    onChange={(_pagination, _filters, sorter) => {
                        setSortState(resolveSortFromTableChange(sorter, sortTable));
                    }}
                />
            </EmptyState>
            <div className="ops-table-pagination ops-image-records-pagination">
                <Space>
                    <Button size="small" disabled={page <= 1 || loading} onClick={() => setPage((p) => Math.max(1, p - 1))}>
                        Previous
                    </Button>
                    <span>
                        Page {page}
                        {totalPages != null ? ` of ${totalPages}` : ""}
                    </span>
                    <Button size="small" disabled={!hasMore || loading} onClick={() => setPage((p) => p + 1)}>
                        Next
                    </Button>
                    <Select
                        size="small"
                        value={pageSize}
                        options={PAGE_SIZE_OPTIONS.map((value) => ({ value, label: `${value} / page` }))}
                        onChange={(next) => {
                            setPageSize(next);
                            setPage(1);
                        }}
                    />
                    {recordCount != null ? (
                        <span className="ops-muted">{recordCount.toLocaleString()} records</span>
                    ) : null}
                </Space>
            </div>
            <ImageRecordPreviewModal
                open={previewRecordKey !== null}
                onClose={() => setPreviewRecordKey(null)}
                pipelineId={pipelineId}
                specId={specId}
                scope={scope}
                parentId={parentId}
                recordKey={previewRecordKey ?? ""}
            />
        </div>
    );
}
