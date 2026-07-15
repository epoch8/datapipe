import React from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { Button, Space, Table, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { SortOrder } from "antd/es/table/interface";
import {
    ApartmentOutlined,
    BarChartOutlined,
    CheckCircleOutlined,
    ClockCircleOutlined,
    CodeOutlined,
    DatabaseOutlined,
    FolderOpenOutlined,
    LinkOutlined,
    PlayCircleOutlined,
    RightOutlined,
    SafetyCertificateOutlined,
    TableOutlined,
    TagOutlined,
    TeamOutlined,
    WarningOutlined,
} from "@ant-design/icons";
import { opsApi } from "@datapipe/ui-ml/api/client";
import { usePipelineId } from "@datapipe/ui/hooks/usePipelineId";
import type { OpsColumn, OpsFilterRule, OpsMetricColumn, OpsOverviewResponse, OpsRowsParams, OpsRowsResponse, OpsSpecDetail, OpsTableSchema } from "../../../types/opsSpecs";
import {
    EmptyState,
    PageHeader,
    TableFilterBar,
    collectFilterColumns,
    dedupeFilterColumns,
    expandChipValueRules,
    formatRule,
    mergeTableFilterState,
    parseUrlFilterState,
    serializeFilterRules,
    writeUrlFilterState,
    defaultSortState,
    resolveSortFromTableChange,
    serverSideSorter,
    sortOrderForColumn,
    type OpsFilterState,
} from "../shared";

type PageKind = "frozen-datasets" | "training" | "metrics" | "class-metrics";
type EntityKind = "frozen-dataset" | "model" | "training-run";
type Row = Record<string, unknown>;

function entityLinksFromColumns(columns: OpsColumn[] | undefined): Record<string, string> {
    const links: Record<string, string> = {};
    for (const column of columns ?? []) {
        if (column.link_to && column.source) {
            links[column.link_to] = column.source;
        }
    }
    return links;
}

function specPageColumns(
    columns: OpsColumn[],
    specId: string,
    options?: { frozenDatasetIdSource?: string },
): ColumnsType<Row> {
    const entityBySource = new Map(
        Object.entries(entityLinksFromColumns(columns)).map(([entity, source]) => [source, entity]),
    );
    return columns.map((column) => ({
        title: column.label,
        dataIndex: column.source,
        key: column.id,
        width: column.width ?? undefined,
        render: (value: unknown, row: Row) => {
            if (column.kind === "status") return <StatusTag value={value} />;
            if (column.kind === "chip") return <Tag className="ops-soft-chip">{displayValue(value)}</Tag>;
            if (column.kind === "split") {
                return <span className="ops-split-cell">{displayValue(value)}</span>;
            }
            if (column.kind === "models_count") {
                const datasetId = options?.frozenDatasetIdSource ? row[options.frozenDatasetIdSource] : undefined;
                const count = Number(value ?? 0);
                if (datasetId) {
                    return (
                        <Link
                            className="dp-entity-link"
                            to={`/frozen-datasets/${encodeURIComponent(specId)}/datasets/${encodeURIComponent(String(datasetId))}`}
                        >
                            {count} models
                        </Link>
                    );
                }
                return `${count} models`;
            }
            if (column.kind === "duration") {
                const seconds = Number(value);
                if (!Number.isFinite(seconds)) return displayValue(value);
                if (seconds < 60) return `${seconds}s`;
                const minutes = Math.floor(seconds / 60);
                const rest = seconds % 60;
                return rest ? `${minutes}m ${rest}s` : `${minutes}m`;
            }
            return renderColumn(
                column,
                specId,
                entityBySource.get(column.source),
                entityLinksFromColumns(columns),
            )(value, row);
        },
    }));
}

const pageTitles: Record<PageKind, string> = { "frozen-datasets": "Frozen Datasets", training: "Training", metrics: "Metrics", "class-metrics": "Class Metrics" };
const pageSubtitles: Record<PageKind, string> = {
    "frozen-datasets": "Snapshot registry across all specifications",
    training: "Training runs across all specifications",
    metrics: "Metric registry across all specifications",
    "class-metrics": "Per-class metric coverage across all specifications",
};
const labels: Record<string, string> = {
    total_snapshots: "Total snapshots", active_specs: "Active specifications", newest_snapshot_at: "Newest snapshot", models_trained_from_snapshots: "Models trained from snapshots",
    running_now: "Running now", queued: "Queued", completed_today: "Completed today", needs_attention: "Needs attention",
    metric_tables: "Metric tables", metric_sources: "Metric sources", models_covered: "Models covered", frozen_datasets_linked: "Frozen datasets linked",
    class_metric_tables: "Class metric tables", total_tracked_classes: "Total tracked classes", specifications_covered: "Specifications covered", filters_available: "Filters available",
};

function displayValue(value: unknown): React.ReactNode {
    if (value === null || value === undefined || value === "") return "-";
    if (typeof value === "number") return Number.isInteger(value) ? value.toLocaleString() : value.toFixed(3);
    if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}T/.test(value)) return value.slice(0, 16).replace("T", " ");
    return String(value);
}
function textValue(value: unknown) { return String(value ?? "-"); }
function isGroup(column: OpsMetricColumn): column is { label: string; columns: OpsColumn[] } { return "columns" in column; }
function statusKind(status: unknown) {
    const s = String(status ?? "").toLowerCase();
    if (s.includes("fail") || s.includes("error") || s.includes("attention")) return "error";
    if (s.includes("warn")) return "warning";
    if (s.includes("run") || s.includes("queue")) return "processing";
    if (s.includes("success") || s.includes("healthy") || s.includes("complete")) return "success";
    return "default";
}
function StatusTag({ value }: { value: unknown }) { return <Tag className={`ops-status-pill ops-status-${statusKind(value)}`}>{displayValue(value)}</Tag>; }
function IconBubble({ icon, color = "blue" }: { icon: React.ReactNode; color?: string }) { return <div className={`ops-spec-icon ops-spec-icon-${color}`}>{icon}</div>; }
function iconFor(kind: PageKind, i = 0) {
    const list = {
        "frozen-datasets": [<FolderOpenOutlined />, <TeamOutlined />, <ClockCircleOutlined />, <LinkOutlined />],
        training: [<PlayCircleOutlined />, <ClockCircleOutlined />, <CheckCircleOutlined />, <WarningOutlined />],
        metrics: [<DatabaseOutlined />, <ApartmentOutlined />, <BarChartOutlined />, <LinkOutlined />],
        "class-metrics": [<TableOutlined />, <ApartmentOutlined />, <SafetyCertificateOutlined />, <TagOutlined />],
    }[kind];
    return list[i % list.length];
}
function entityHref(
    specId: string,
    target: string,
    value: unknown,
    row?: Row,
    entityLinks?: Record<string, string>,
) {
    const id = encodeURIComponent(textValue(value));
    if (target.includes("dataset")) {
        return `/frozen-datasets/${encodeURIComponent(specId)}/datasets/${id}`;
    }
    if (target.includes("model")) {
        const path = `/metrics/${encodeURIComponent(specId)}/models/${id}`;
        const datasetCol = entityLinks?.frozen_dataset ?? entityLinks?.dataset;
        const datasetId = datasetCol && row ? row[datasetCol] : undefined;
        if (datasetId != null && datasetId !== "") {
            return `${path}?dataset_id=${encodeURIComponent(String(datasetId))}`;
        }
        return path;
    }
    if (target.includes("run")) return `/training/${encodeURIComponent(specId)}/runs/${id}`;
    return null;
}
function EntityValue({
    value,
    specId,
    column,
    fallbackKind = "",
    row,
    entityLinks,
}: {
    value: unknown;
    specId: string;
    column?: OpsColumn;
    fallbackKind?: string;
    row?: Row;
    entityLinks?: Record<string, string>;
}) {
    if (value === null || value === undefined || value === "") return <span>-</span>;
    const href = entityHref(specId, column?.link_to ?? fallbackKind, value, row, entityLinks);
    return href ? <Link className="dp-entity-link" to={href}>{textValue(value)}</Link> : <>{displayValue(value)}</>;
}
function SummaryCards({ kind, summary }: { kind: PageKind; summary: Record<string, unknown> }) {
    return <div className="ops-spec-kpi-grid">{Object.entries(summary).slice(0, 4).map(([key, value], i) => <div className="ops-spec-kpi-card" key={key}><IconBubble icon={iconFor(kind, i)} color={i === 1 ? "green" : i === 2 ? "purple" : "blue"} /><div><div className="ops-spec-kpi-label">{labels[key] ?? key.replace(/_/g, " ")}</div><div className="ops-spec-kpi-value">{displayValue(value)}</div></div><div className="ops-mini-spark"><span /><span /><span /><span /><span /></div></div>)}</div>;
}
function MiniMetric({ label, value }: { label: string; value: unknown }) { return <div className="ops-mini-metric"><div className="ops-mini-metric-label">{label}</div><div className="ops-mini-metric-value">{displayValue(value)}</div></div>; }
function SplitBar({ value }: { value: unknown }) {
    const nums = String(value ?? "0/0/0").split("/").map((x) => Number(x.trim()) || 0); const total = nums.reduce((a, b) => a + b, 0) || 1;
    return <div className="ops-split-wrap"><div className="ops-split-bar"><span style={{ width: `${(nums[0] / total) * 100}%` }} /><span style={{ width: `${(nums[1] / total) * 100}%` }} /><span style={{ width: `${(nums[2] / total) * 100}%` }} /></div><div className="ops-split-labels"><span>train</span><span>val</span><span>test</span></div></div>;
}
function OverviewCard({ kind, spec }: { kind: PageKind; spec: Row }) {
    const specId = String(spec.spec_id ?? spec.id);
    return <Link className="ops-landing-card" to={`/${kind}/${encodeURIComponent(specId)}`}><div className="ops-landing-card-head"><IconBubble icon={iconFor(kind)} color={String(spec.color ?? "blue")} /><div><div className="ops-landing-title">{String(spec.title ?? specId)}</div><div className="ops-landing-description">{String(spec.description ?? "")}</div></div><StatusTag value={spec.status ?? "healthy"} /></div><div className="ops-landing-metrics">{kind === "frozen-datasets" && <><MiniMetric label="Snapshots" value={spec.snapshots_count} /><MiniMetric label="Latest snapshot" value={(spec.latest_snapshot as Row | undefined)?.label} /><MiniMetric label="Models trained" value={spec.models_trained} /></>}{kind === "training" && <><MiniMetric label="Running" value={spec.running_count} /><MiniMetric label="Completed" value={spec.completed_count} /><MiniMetric label="Failed" value={spec.failed_count} /></>}{kind === "metrics" && <><MiniMetric label="Metric tables" value={spec.metric_tables_count} /><MiniMetric label="Sources" value={(spec.metric_sources as unknown[] | undefined)?.length ?? 0} /><MiniMetric label="Dimensions" value={(spec.dimensions_supported as unknown[] | undefined)?.length ?? 0} /></>}{kind === "class-metrics" && <><MiniMetric label="Classes" value={spec.classes_tracked} /><MiniMetric label="Tables" value={spec.class_metric_tables_count} /><MiniMetric label="Filters" value={(spec.supported_filters as unknown[] | undefined)?.length ?? 0} /></>}</div>{kind === "frozen-datasets" && <SplitBar value={spec.split} />}<div className="ops-landing-footer"><span>{kind === "metrics" ? "Open metrics" : kind === "class-metrics" ? "Open class metrics" : kind === "training" ? "Open training" : "Open datasets"}</span><RightOutlined /></div></Link>;
}
function OverviewBottom({ kind, data }: { kind: PageKind; data: OpsOverviewResponse }) {
    const activity = ((data.recent_activity as Row[] | undefined) ?? data.specs).slice(0, 5);
    return (
        <div className="ops-spec-bottom-grid">
            <div className="ops-panel ops-polished-panel">
                <div className="ops-panel-head">
                    <div className="ops-panel-title">Recent activity</div>
                    <Link to={`/${kind}`}>
                        View all activity <RightOutlined />
                    </Link>
                </div>
                <div className="ops-activity-list">
                    {activity.map((item, i) => (
                        <div
                            className="ops-activity-row"
                            key={`${i}-${String(item.title ?? item.message)}`}
                        >
                            <span className={`ops-activity-dot ops-dot-${i % 4}`} />
                            <div className="ops-activity-main">
                                <strong>{String(item.title ?? "Activity")}</strong>
                                <span>{String(item.message ?? item.description ?? "Ready")}</span>
                            </div>
                            <StatusTag value={item.status ?? "success"} />
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}

export function OpsOverviewSpecPage({ kind }: { kind: PageKind }) {
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [data, setData] = React.useState<OpsOverviewResponse | null>(null);
    const [loading, setLoading] = React.useState(true); const [error, setError] = React.useState<string | null>(null);
    const load = React.useCallback(() => { if (!pipelineId) return; setLoading(true); setError(null); opsApi.getOpsPageOverview(pipelineId, kind).then(setData).catch((e) => setError(String(e))).finally(() => setLoading(false)); }, [pipelineId, kind]);
    React.useEffect(() => { load(); }, [load]);
    return <div className="ops-page ops-spec-page"><PageHeader breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: pageTitles[kind] }]} title={pageTitles[kind]} subtitle={pageSubtitles[kind]} statusChips={[{ label: "Running", variant: "success" }, { label: "All specifications", variant: "purple" }]} onRefresh={load} primaryAction={kind === "training" ? { label: "New Training Run" } : undefined} /><EmptyState loading={pidLoading || loading} error={error} empty={!data?.specs?.length && !loading}>{data?.summary && <SummaryCards kind={kind} summary={data.summary} />}<div className="ops-landing-grid">{(data?.specs ?? []).map((spec) => <OverviewCard key={String(spec.spec_id ?? spec.id)} kind={kind} spec={spec} />)}</div>{data && <OverviewBottom kind={kind} data={data} />}</EmptyState></div>;
}
function renderColumn(column: OpsColumn, specId: string, fallbackKind?: string, entityLinks?: Record<string, string>) {
    return (value: unknown, row?: Row) => {
        if (column.kind === "status") return <StatusTag value={value} />;
        if (column.kind === "chip") return <Tag className="ops-soft-chip">{displayValue(value)}</Tag>;
        if (column.kind === "link" || column.link_to || fallbackKind) {
            return (
                <EntityValue
                    value={value}
                    specId={specId}
                    column={column}
                    fallbackKind={fallbackKind}
                    row={row}
                    entityLinks={entityLinks}
                />
            );
        }
        return displayValue(value);
    };
}
function metricColumns(table: OpsTableSchema, specId: string, sortState: { sort_by?: string; sort_dir?: "asc" | "desc" }): ColumnsType<Row> {
    const entityBySource = new Map(Object.entries(table.entity_links ?? {}).map(([entity, source]) => [source, entity]));
    const simple = (column: OpsColumn) => ({
        title: column.label,
        dataIndex: column.source,
        key: column.id,
        width: column.width ?? undefined,
        ellipsis: column.kind === "text" || column.kind === "link",
        render: renderColumn(column, specId, entityBySource.get(column.source), table.entity_links),
        sorter: serverSideSorter(column),
        sortDirections: ["ascend", "descend"] as SortOrder[],
        sortOrder: sortOrderForColumn(column, sortState),
    });
    return [...table.primary_columns.map(simple), ...table.metric_columns.map((column) => isGroup(column) ? { title: column.label, key: column.label, children: column.columns.map(simple) } : simple(column))];
}
function useDebouncedValue<T>(value: T, delayMs: number): T {
    const [debounced, setDebounced] = React.useState(value);
    React.useEffect(() => {
        const timer = window.setTimeout(() => setDebounced(value), delayMs);
        return () => window.clearTimeout(timer);
    }, [value, delayMs]);
    return debounced;
}

function SpecMetricTable({ pipelineId, specId, table, classMetrics }: { pipelineId: string; specId: string; table: OpsTableSchema; classMetrics?: boolean }) {
    const [searchParams, setSearchParams] = useSearchParams();
    const filterColumns = React.useMemo(() => collectFilterColumns(table), [table]);
    const entityLinks = table.entity_links ?? {};
    const [filterState, setFilterState] = React.useState<OpsFilterState>(() =>
        mergeTableFilterState(searchParams, table, collectFilterColumns(table)),
    );
    const [data, setData] = React.useState<OpsRowsResponse | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [page, setPage] = React.useState(1);
    const [pageSize, setPageSize] = React.useState(10);
    const [sortState, setSortState] = React.useState(() => defaultSortState(table));
    const debouncedSearch = useDebouncedValue(filterState.search ?? "", 300);
    const debouncedRules = useDebouncedValue(filterState.rules, 300);

    React.useEffect(() => {
        setSearchParams((prev) => writeUrlFilterState(prev, filterState, filterColumns), { replace: true });
    }, [filterColumns, filterState, setSearchParams]);

    const load = React.useCallback(() => {
        setLoading(true);
        setError(null);
        const call = classMetrics ? opsApi.getOpsClassMetricRows : opsApi.getOpsMetricRows;
        const params: OpsRowsParams = {
            limit: pageSize,
            offset: (page - 1) * pageSize,
            ...sortState,
            search: debouncedSearch.trim() || undefined,
            filter_mode: filterState.mode,
            filters: serializeFilterRules(expandChipValueRules(debouncedRules, filterColumns), filterColumns),
        };
        call(pipelineId, specId, table.id, params).then(setData).catch((e) => setError(String(e))).finally(() => setLoading(false));
    }, [classMetrics, debouncedRules, debouncedSearch, filterColumns, filterState.mode, page, pageSize, pipelineId, specId, sortState, table.id]);

    const tableColumns = React.useMemo(() => metricColumns(table, specId, sortState), [specId, sortState, table]);

    React.useEffect(() => { setPage(1); }, [debouncedSearch, debouncedRules, filterState.mode, sortState]);
    React.useEffect(() => { load(); }, [load]);

    const appliedRules = serializeFilterRules(expandChipValueRules(filterState.rules, filterColumns), filterColumns);

    return (
        <div className="ops-panel ops-polished-panel ops-spec-table-panel">
            <TableFilterBar
                columns={filterColumns}
                entityLinks={entityLinks}
                value={filterState}
                onChange={setFilterState}
                metricSource={table.metric_source}
                searchPlaceholder="Search by text..."
            />
            <div className="ops-spec-table-head">
                <div>
                    <div className="ops-panel-title">{table.title} - {table.metric_source}</div>
                    <div className="ops-muted">{classMetrics ? "Class metrics" : "Metric rows"}</div>
                    {data ? <div className="ops-table-filter-summary">Showing {data.rows.length} of {data.total} rows</div> : null}
                    {appliedRules.length ? (
                        <div className="ops-table-filter-summary">
                            Filtered by: {appliedRules.map((rule) => formatRule(rule, filterColumns)).join(", ")}
                        </div>
                    ) : null}
                </div>
            </div>
            <EmptyState loading={loading} error={error} empty={!data?.rows.length && !loading}>
                <Table
                    className="ops-table ops-spec-table ops-table-compact"
                    size="small"
                    columns={tableColumns}
                    dataSource={data?.rows ?? []}
                    rowKey={(row, i) => `${table.id}-${String(row.model_id ?? row.dataset_id ?? row.class_id ?? i)}`}
                    pagination={{
                        current: page,
                        pageSize,
                        total: data?.total ?? 0,
                        showSizeChanger: true,
                        onChange: (nextPage, nextSize) => { setPage(nextPage); if (nextSize) setPageSize(nextSize); },
                    }}
                    scroll={{ x: "max-content" }}
                    onChange={(_pagination, _filters, sorter) => {
                        setSortState(resolveSortFromTableChange(sorter, table));
                    }}
                />
            </EmptyState>
        </div>
    );
}

function SpecDataRowsTable({
    pipelineId,
    specId,
    kind,
    fetchRows,
    columns,
    rowColumns,
    defaultFilterColumns,
    entityLinks = {},
    title,
    onRowSelect,
    selectedKey,
    selectedSource,
}: {
    pipelineId: string;
    specId: string;
    kind: "frozen-datasets" | "training";
    fetchRows: (params: OpsRowsParams) => Promise<OpsRowsResponse>;
    columns: ColumnsType<Row>;
    rowColumns: OpsColumn[];
    defaultFilterColumns: OpsColumn[];
    entityLinks?: Record<string, string>;
    title: string;
    onRowSelect?: (row: Row) => void;
    selectedKey?: string;
    selectedSource?: string;
}) {
    const [filterColumns, setFilterColumns] = React.useState(() => dedupeFilterColumns(defaultFilterColumns));
    const filterColumnsRef = React.useRef(filterColumns);
    filterColumnsRef.current = filterColumns;
    const filterColumnsSynced = React.useRef(false);
    const [entityLinksState, setEntityLinksState] = React.useState(entityLinks);

    React.useEffect(() => {
        setEntityLinksState(entityLinks);
    }, [entityLinks]);

    React.useEffect(() => {
        setFilterColumns(dedupeFilterColumns(defaultFilterColumns));
        filterColumnsSynced.current = false;
    }, [defaultFilterColumns, specId]);

    const [filterState, setFilterState] = React.useState<OpsFilterState>({ mode: "or", rules: [] });
    const [data, setData] = React.useState<OpsRowsResponse | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [page, setPage] = React.useState(1);
    const [pageSize, setPageSize] = React.useState(10);
    const debouncedSearch = useDebouncedValue(filterState.search ?? "", 300);
    const debouncedRules = useDebouncedValue(filterState.rules, 300);

    const load = React.useCallback(() => {
        setLoading(true);
        setError(null);
        const columns = filterColumnsRef.current;
        fetchRows({
            limit: pageSize,
            offset: (page - 1) * pageSize,
            search: debouncedSearch.trim() || undefined,
            filter_mode: filterState.mode,
            filters: serializeFilterRules(expandChipValueRules(debouncedRules, columns), columns),
        })
            .then((res) => {
                setData(res);
                if (!filterColumnsSynced.current && res.filter_columns?.length) {
                    filterColumnsSynced.current = true;
                    setFilterColumns(dedupeFilterColumns(res.filter_columns));
                }
                if (res.entity_links) {
                    setEntityLinksState(res.entity_links);
                }
            })
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [debouncedRules, debouncedSearch, fetchRows, filterState.mode, page, pageSize]);

    React.useEffect(() => {
        setPage(1);
    }, [debouncedSearch, debouncedRules, filterState.mode, specId]);
    React.useEffect(() => {
        load();
    }, [load]);

    const appliedRules = serializeFilterRules(expandChipValueRules(filterState.rules, filterColumns), filterColumns);

    return (
        <div className="ops-panel ops-polished-panel ops-spec-table-panel">
            <TableFilterBar
                columns={filterColumns}
                entityLinks={entityLinksState}
                value={filterState}
                onChange={setFilterState}
                searchPlaceholder={kind === "training" ? "Search runs..." : "Search datasets..."}
            />
            <div className="ops-spec-table-head">
                <div>
                    <div className="ops-panel-title">{title}</div>
                    {data ? <div className="ops-table-filter-summary">Showing {data.rows.length} of {data.total} rows</div> : null}
                    {appliedRules.length ? (
                        <div className="ops-table-filter-summary">
                            Filtered by: {appliedRules.map((rule) => formatRule(rule, filterColumns)).join(", ")}
                        </div>
                    ) : null}
                </div>
            </div>
            <EmptyState loading={loading} error={error} empty={!data?.rows.length && !loading}>
                <Table
                    className="ops-table ops-spec-table"
                    size="middle"
                    columns={columns}
                    dataSource={data?.rows ?? []}
                    rowKey={(row, i) => String(row[rowColumns[0]?.source ?? ""] ?? i)}
                    rowClassName={onRowSelect ? (row) => (String(selectedSource ? row[selectedSource] : row[rowColumns[0]?.source ?? ""]) === selectedKey ? "ops-selected-row" : "") : undefined}
                    onRow={onRowSelect ? (row) => ({ onClick: () => onRowSelect(row) }) : undefined}
                    pagination={{
                        current: page,
                        pageSize,
                        total: data?.total ?? 0,
                        showSizeChanger: true,
                        onChange: (nextPage, nextSize) => {
                            setPage(nextPage);
                            if (nextSize) setPageSize(nextSize);
                        },
                    }}
                    scroll={{ x: "max-content" }}
                />
            </EmptyState>
        </div>
    );
}

function FrozenDatasetTable({ specId, pipelineId, spec }: { specId: string; pipelineId: string; spec: OpsSpecDetail }) {
    const columns = spec.frozen_dataset?.columns ?? [];
    const fetchRows = React.useCallback(
        (params: OpsRowsParams) => opsApi.getOpsFrozenRows(pipelineId, specId, params),
        [pipelineId, specId],
    );
    const defaultFilterColumns = React.useMemo(() => dedupeFilterColumns(columns), [columns]);
    const entityLinks = React.useMemo(() => entityLinksFromColumns(columns), [columns]);
    const tableColumns = React.useMemo(
        () => specPageColumns(columns, specId, { frozenDatasetIdSource: spec.frozen_dataset?.id_column }),
        [columns, spec.frozen_dataset?.id_column, specId],
    );
    return (
        <SpecDataRowsTable
            pipelineId={pipelineId}
            specId={specId}
            kind="frozen-datasets"
            title="Frozen datasets"
            fetchRows={fetchRows}
            columns={tableColumns}
            rowColumns={columns}
            defaultFilterColumns={defaultFilterColumns}
            entityLinks={entityLinks}
        />
    );
}
function TrainingTable({ specId, pipelineId, spec, selected, onSelect }: { specId: string; pipelineId: string; spec: OpsSpecDetail; selected?: string; onSelect: (row: Row) => void }) {
    const columns = spec.training?.columns ?? [];
    const fetchRows = React.useCallback(
        (params: OpsRowsParams) => opsApi.getOpsTrainingRows(pipelineId, specId, params),
        [pipelineId, specId],
    );
    const defaultFilterColumns = React.useMemo(() => dedupeFilterColumns(columns), [columns]);
    const entityLinks = React.useMemo(() => entityLinksFromColumns(columns), [columns]);
    const tableColumns = React.useMemo(() => specPageColumns(columns, specId), [columns, specId]);
    const selectedSource = columns.find((column) => column.link_to === "training_run")?.source ?? columns.find((column) => column.link_to === "model")?.source;
    return (
        <SpecDataRowsTable
            pipelineId={pipelineId}
            specId={specId}
            kind="training"
            title="Training runs"
            fetchRows={fetchRows}
            columns={tableColumns}
            rowColumns={columns}
            defaultFilterColumns={defaultFilterColumns}
            entityLinks={entityLinks}
            selectedKey={selected}
            selectedSource={selectedSource}
            onRowSelect={onSelect}
        />
    );
}
function TrainingRail({ row, specId, columns }: { row?: Row; specId: string; columns: OpsColumn[] }) {
    if (!row) return <div className="ops-panel ops-polished-panel ops-side-rail"><div className="ops-panel-title">Selected run</div><p className="ops-muted">Select a run from the table.</p></div>;
    const byId = new Map(columns.map((column) => [column.id, column]));
    const runColumn = columns.find((column) => column.link_to === "training_run") ?? byId.get("run_id");
    const modelColumn = columns.find((column) => column.link_to === "model");
    const frozenColumn = columns.find((column) => column.link_to === "frozen_dataset");
    const startedColumn = columns.find((column) => column.id === "started_at" || column.source.endsWith("started_at"));
    const durationColumn = columns.find((column) => column.kind === "duration");
    const statusColumn = columns.find((column) => column.kind === "status");
    return <div className="ops-panel ops-polished-panel ops-side-rail"><div className="ops-panel-title">Selected run</div><div className="ops-run-id">{displayValue(runColumn ? row[runColumn.source] : undefined)}</div><StatusTag value={statusColumn ? row[statusColumn.source] : undefined} /><dl className="ops-detail-list ops-detail-list-wide"><dt>Model</dt><dd><EntityValue value={modelColumn ? row[modelColumn.source] : undefined} specId={specId} column={modelColumn} fallbackKind="model" /></dd><dt>Frozen dataset</dt><dd><EntityValue value={frozenColumn ? row[frozenColumn.source] : undefined} specId={specId} column={frozenColumn} fallbackKind="frozen_dataset" /></dd><dt>Started</dt><dd>{displayValue(startedColumn ? row[startedColumn.source] : undefined)}</dd><dt>Duration</dt><dd>{displayValue(durationColumn ? row[durationColumn.source] : undefined)}</dd></dl><div className="ops-rail-divider" /><div className="ops-panel-title">Best metrics so far</div><table className="ops-mini-table"><tbody><tr><td>W-F1</td><td>0.782</td></tr><tr><td>mAP@0.5</td><td>0.774</td></tr><tr><td>Recall</td><td>0.756</td></tr></tbody></table></div>;
}
function DataSpecificPage({ kind, specId, spec, pipelineId, load }: { kind: PageKind; specId: string; spec: OpsSpecDetail; pipelineId: string; load: () => void }) {
    const [selected, setSelected] = React.useState<Row | undefined>();
    const isTraining = kind === "training";
    const trainingColumns = spec.training?.columns ?? [];
    const trainingSelectSource = trainingColumns.find((column) => column.link_to === "training_run")?.source
        ?? trainingColumns.find((column) => column.link_to === "model")?.source;
    const selectedKey = selected && trainingSelectSource ? String(selected[trainingSelectSource] ?? "") : "";
    return (
        <>
            <PageHeader
                breadcrumbs={[
                    { label: "Datapipe Ops", href: "/" },
                    { label: pageTitles[kind], href: `/${kind}` },
                    { label: spec.title },
                ]}
                title={`${spec.title} - ${isTraining ? "Training" : "Frozen Datasets"}`}
                subtitle={
                    isTraining
                        ? `Training runs for the ${spec.title} specification`
                        : `Snapshot registry for the ${spec.title} specification`
                }
                statusChips={[
                    { label: "Running", variant: "success" },
                    { label: spec.title, variant: "purple" },
                ]}
                onRefresh={load}
                primaryAction={isTraining ? { label: "New Training Run" } : undefined}
            />
            {isTraining ? (
                <div className="ops-detail-with-rail">
                    <div>
                        <div className="ops-spec-mini-summary">
                            <MiniMetric label="Total runs" value="-" />
                            <MiniMetric label="Completed" value="-" />
                            <MiniMetric label="Failed" value="-" />
                            <MiniMetric label="Best W-F1" value="-" />
                        </div>
                        <TrainingTable
                            specId={specId}
                            pipelineId={pipelineId}
                            spec={spec}
                            selected={selectedKey}
                            onSelect={setSelected}
                        />
                    </div>
                    <TrainingRail row={selected} specId={specId} columns={trainingColumns} />
                </div>
            ) : (
                <FrozenDatasetTable specId={specId} pipelineId={pipelineId} spec={spec} />
            )}
        </>
    );
}
export function OpsSpecificSpecPage({ kind }: { kind: PageKind }) {
    const { specId = "" } = useParams(); const { pipelineId, loading: pidLoading } = usePipelineId();
    const [spec, setSpec] = React.useState<OpsSpecDetail | null>(null); const [loading, setLoading] = React.useState(true); const [error, setError] = React.useState<string | null>(null);
    const load = React.useCallback(() => { if (!pipelineId || !specId) return; setLoading(true); setError(null); opsApi.getOpsSpec(pipelineId, specId).then(setSpec).catch((e) => setError(String(e))).finally(() => setLoading(false)); }, [pipelineId, specId]);
    React.useEffect(() => { load(); }, [load]);
    return <div className="ops-page ops-spec-page"><EmptyState loading={pidLoading || loading} error={error} empty={!spec && !loading}>{spec && pipelineId && (kind === "frozen-datasets" || kind === "training") && <DataSpecificPage kind={kind} specId={specId} spec={spec} pipelineId={pipelineId} load={load} />}{spec && pipelineId && (kind === "metrics" || kind === "class-metrics") && <><PageHeader breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: pageTitles[kind], href: `/${kind}` }, { label: spec.title }]} title={`${spec.title} - ${pageTitles[kind]}`} subtitle={kind === "metrics" ? `Metric tables for the ${spec.title} specification` : `Per-class metrics for the ${spec.title} specification`} statusChips={[{ label: "Running", variant: "success" }, { label: spec.title, variant: "purple" }]} onRefresh={load} extra={kind === "metrics" ? <Button icon={<CodeOutlined />}>View JSON Spec</Button> : undefined} />{(kind === "metrics" ? spec.metrics : spec.class_metrics).map((table) => <SpecMetricTable key={table.id} pipelineId={pipelineId} specId={specId} table={table} classMetrics={kind === "class-metrics"} />)}</>}</EmptyState></div>;
}
export function OpsEntityDetailPage({ kind }: { kind: EntityKind }) {
    const { specId = "", entityId = "" } = useParams(); const { pipelineId, loading: pidLoading } = usePipelineId();
    const [spec, setSpec] = React.useState<OpsSpecDetail | null>(null); const [rows, setRows] = React.useState<OpsRowsResponse[]>([]); const [loading, setLoading] = React.useState(true); const [error, setError] = React.useState<string | null>(null);
    const load = React.useCallback(() => { if (!pipelineId || !specId) return; setLoading(true); setError(null); opsApi.getOpsSpec(pipelineId, specId).then(async (s) => { setSpec(s); if (kind === "frozen-dataset") setRows([await opsApi.getOpsFrozenRows(pipelineId, specId, { limit: 100 })]); else if (kind === "training-run") setRows([await opsApi.getOpsTrainingRows(pipelineId, specId, { limit: 100 })]); else setRows(await Promise.all(s.metrics.map((t) => opsApi.getOpsMetricRows(pipelineId, specId, t.id, { limit: 100 })))); }).catch((e) => setError(String(e))).finally(() => setLoading(false)); }, [pipelineId, specId, kind]);
    React.useEffect(() => { load(); }, [load]);
    const found = rows.flatMap((p) => p.rows).filter((row) => Object.values(row).some((v) => String(v) === entityId)); const record = found[0];
    const title = kind === "model" ? `Model ${entityId}` : kind === "training-run" ? `Training run ${entityId}` : `Frozen dataset ${entityId}`;
    const back = kind === "model" ? `/metrics/${specId}` : kind === "training-run" ? `/training/${specId}` : `/frozen-datasets/${specId}`;
    return <div className="ops-page ops-spec-page"><PageHeader breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: spec?.title ?? specId, href: back }, { label: entityId }]} title={title} subtitle={spec?.description} onRefresh={load} /><EmptyState loading={pidLoading || loading} error={error} empty={!record && !loading}><div className="ops-entity-detail-grid"><div className="ops-panel ops-polished-panel"><div className="ops-panel-title">Source record</div><dl className="ops-source-record-dl">{Object.entries(record ?? {}).slice(0, 18).map(([k, v]) => <React.Fragment key={k}><dt>{k}</dt><dd>{displayValue(v)}</dd></React.Fragment>)}</dl></div><div className="ops-panel ops-polished-panel"><div className="ops-panel-title">Linked pages</div><div className="ops-linked-actions"><Link to={`/frozen-datasets/${specId}`}>Frozen datasets <RightOutlined /></Link><Link to={`/training/${specId}`}>Training <RightOutlined /></Link><Link to={`/metrics/${specId}`}>Metrics <RightOutlined /></Link><Link to={`/class-metrics/${specId}`}>Class metrics <RightOutlined /></Link></div></div></div>{kind === "model" && spec?.metrics.map((table, i) => <div className="ops-panel ops-polished-panel ops-spec-table-panel" key={table.id}><div className="ops-panel-title">{table.title}</div><Table className="ops-table ops-spec-table" size="small" columns={metricColumns(table, specId, defaultSortState(table))} dataSource={rows[i]?.rows.filter((row) => String(row.model_id ?? row[table.entity_links?.model ?? ""]) === entityId) ?? []} rowKey={(row, idx) => `${table.id}-${idx}`} pagination={false} scroll={{ x: "max-content" }} /></div>)}</EmptyState></div>;
}
