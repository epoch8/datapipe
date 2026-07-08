import React from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { Button, Space, Table, Tabs, Tag } from "antd";
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
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import type { OpsColumn, OpsFilterRule, OpsMetricColumn, OpsOverviewResponse, OpsRowsParams, OpsRowsResponse, OpsSpecDetail, OpsTableSchema } from "../../../types/opsSpecs";
import { EmptyState, PageHeader } from "../shared";
import { TableFilterBar } from "../shared/TableFilterBar";
import {
    collectFilterColumns,
    expandChipValueRules,
    formatRule,
    mergeTableFilterState,
    parseUrlFilterState,
    serializeFilterRules,
    writeUrlFilterState,
    type OpsFilterState,
} from "../shared/tableFilters";
import {
    defaultSortState,
    resolveSortFromTableChange,
    serverSideSorter,
    sortOrderForColumn,
} from "../shared/opsTableSort";

type PageKind = "frozen-datasets" | "training" | "metrics" | "class-metrics";
type EntityKind = "frozen-dataset" | "model" | "training-run";
type Row = Record<string, unknown>;

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
function entityHref(specId: string, target: string, value: unknown) {
    const id = encodeURIComponent(textValue(value));
    if (target.includes("dataset")) return `/frozen-datasets/${encodeURIComponent(specId)}/datasets/${id}`;
    if (target.includes("model")) return `/metrics/${encodeURIComponent(specId)}/models/${id}`;
    if (target.includes("run")) return `/training/${encodeURIComponent(specId)}/runs/${id}`;
    return null;
}
function EntityValue({ value, specId, column, fallbackKind = "" }: { value: unknown; specId: string; column?: OpsColumn; fallbackKind?: string }) {
    if (value === null || value === undefined || value === "") return <span>-</span>;
    const href = entityHref(specId, column?.link_to ?? fallbackKind, value);
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
    const steps = kind === "training" ? ["Running", "Completed", "Needs attention"] : kind === "class-metrics" ? ["Browse by class", "Browse by tag", "Compare subsets", "Compare models"] : kind === "metrics" ? ["Model Metrics", "Tag Metrics", "Class Metrics", "Snapshot Views"] : ["Data", "Frozen Dataset", "Training", "Metrics"];
    return <div className="ops-spec-bottom-grid"><div className="ops-panel ops-polished-panel"><div className="ops-panel-head"><div className="ops-panel-title">Recent activity</div><Link to={`/${kind}`}>View all activity <RightOutlined /></Link></div><div className="ops-activity-list">{activity.map((item, i) => <div className="ops-activity-row" key={`${i}-${String(item.title ?? item.message)}`}><span className={`ops-activity-dot ops-dot-${i % 4}`} /><div className="ops-activity-main"><strong>{String(item.title ?? "Activity")}</strong><span>{String(item.message ?? item.description ?? "Ready")}</span></div><StatusTag value={item.status ?? "success"} /></div>)}</div></div><div className="ops-panel ops-polished-panel"><div className="ops-panel-head"><div className="ops-panel-title">{kind === "frozen-datasets" ? "Snapshot lifecycle" : kind === "training" ? "Run board" : kind === "metrics" ? "Available views" : "How to explore"}</div><Button type="link" className="ops-inline-link">Learn more</Button></div><div className="ops-lifecycle-row">{steps.map((s, i) => <React.Fragment key={s}><div className="ops-lifecycle-step"><IconBubble icon={iconFor(kind, i)} color={i % 2 ? "green" : "blue"} /><strong>{s}</strong><span>{i === 0 ? "Source" : "Tracked"}</span></div>{i < steps.length - 1 && <RightOutlined className="ops-lifecycle-arrow" />}</React.Fragment>)}</div></div></div>;
}

export function OpsOverviewSpecPage({ kind }: { kind: PageKind }) {
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [data, setData] = React.useState<OpsOverviewResponse | null>(null);
    const [loading, setLoading] = React.useState(true); const [error, setError] = React.useState<string | null>(null);
    const load = React.useCallback(() => { if (!pipelineId) return; setLoading(true); setError(null); opsApi.getOpsPageOverview(pipelineId, kind).then(setData).catch((e) => setError(String(e))).finally(() => setLoading(false)); }, [pipelineId, kind]);
    React.useEffect(() => { load(); }, [load]);
    return <div className="ops-page ops-spec-page"><PageHeader breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: pageTitles[kind] }]} title={pageTitles[kind]} subtitle={pageSubtitles[kind]} statusChips={[{ label: "Running", variant: "success" }, { label: "All specifications", variant: "purple" }]} onRefresh={load} primaryAction={kind === "training" ? { label: "New Training Run" } : undefined} /><EmptyState loading={pidLoading || loading} error={error} empty={!data?.specs?.length && !loading}>{data?.summary && <SummaryCards kind={kind} summary={data.summary} />}<div className="ops-landing-grid">{(data?.specs ?? []).map((spec) => <OverviewCard key={String(spec.spec_id ?? spec.id)} kind={kind} spec={spec} />)}</div>{data && <OverviewBottom kind={kind} data={data} />}</EmptyState></div>;
}
function renderColumn(column: OpsColumn, specId: string, fallbackKind?: string) {
    return (value: unknown) => {
        if (column.kind === "status") return <StatusTag value={value} />;
        if (column.kind === "chip") return <Tag className="ops-soft-chip">{displayValue(value)}</Tag>;
        if (column.kind === "link" || column.link_to || fallbackKind) return <EntityValue value={value} specId={specId} column={column} fallbackKind={fallbackKind} />;
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
        render: renderColumn(column, specId, entityBySource.get(column.source)),
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

function FrozenDatasetTable({ specId, rows }: { specId: string; rows: OpsRowsResponse | null }) {
    const cols: ColumnsType<Row> = [
        { title: "Dataset", dataIndex: "dataset_id", key: "dataset", render: (v) => <EntityValue value={v} specId={specId} fallbackKind="frozen_dataset" /> },
        { title: "Frozen at", dataIndex: "frozen_at", key: "frozen_at", render: displayValue },
        { title: "Split", dataIndex: "split", key: "split", render: (v) => <span className="ops-split-cell">{displayValue(v)}</span> },
        { title: "Models", dataIndex: "models_count", key: "models", render: (v, row) => <Link className="dp-entity-link" to={`/frozen-datasets/${encodeURIComponent(specId)}/datasets/${encodeURIComponent(String(row.dataset_id ?? ""))}`}>{Number(v ?? 0)} models</Link> },
    ];
    return <Table className="ops-table ops-spec-table" size="middle" columns={cols} dataSource={rows?.rows ?? []} rowKey={(row, i) => String(row.dataset_id ?? i)} pagination={{ pageSize: 10, total: rows?.total ?? 0, showSizeChanger: true }} scroll={{ x: "max-content" }} />;
}
function TrainingTable({ specId, rows, selected, onSelect }: { specId: string; rows: OpsRowsResponse | null; selected?: string; onSelect: (row: Row) => void }) {
    const cols: ColumnsType<Row> = [
        { title: "Run ID", dataIndex: "run_id", key: "run_id", render: (v) => <EntityValue value={v} specId={specId} fallbackKind="training_run" /> },
        { title: "Model", dataIndex: "model_id", key: "model", render: (v) => <EntityValue value={v} specId={specId} fallbackKind="model" /> },
        { title: "Frozen dataset", dataIndex: "frozen_dataset_id", key: "dataset", render: (v) => <EntityValue value={v} specId={specId} fallbackKind="frozen_dataset" /> },
        { title: "Started", dataIndex: "started_at", key: "started", render: displayValue },
        { title: "Duration", dataIndex: "duration_seconds", key: "duration", render: displayValue },
        { title: "Status", dataIndex: "status", key: "status", render: (v) => <StatusTag value={v} /> },
    ];
    return <Table className="ops-table ops-spec-table" size="middle" columns={cols} dataSource={rows?.rows ?? []} rowKey={(row, i) => String(row.run_id ?? row.model_id ?? i)} rowClassName={(row) => String(row.run_id ?? row.model_id) === selected ? "ops-selected-row" : ""} onRow={(row) => ({ onClick: () => onSelect(row) })} pagination={{ pageSize: 10, total: rows?.total ?? 0, showSizeChanger: true }} scroll={{ x: "max-content" }} />;
}
function TrainingRail({ row, specId }: { row?: Row; specId: string }) {
    if (!row) return <div className="ops-panel ops-polished-panel ops-side-rail"><div className="ops-panel-title">Selected run</div><p className="ops-muted">Select a run from the table.</p></div>;
    return <div className="ops-panel ops-polished-panel ops-side-rail"><div className="ops-panel-title">Selected run</div><div className="ops-run-id">{displayValue(row.run_id ?? row.model_id)}</div><StatusTag value={row.status} /><dl className="ops-detail-list ops-detail-list-wide"><dt>Model</dt><dd><EntityValue value={row.model_id} specId={specId} fallbackKind="model" /></dd><dt>Frozen dataset</dt><dd><EntityValue value={row.frozen_dataset_id} specId={specId} fallbackKind="frozen_dataset" /></dd><dt>Started</dt><dd>{displayValue(row.started_at)}</dd><dt>Duration</dt><dd>{displayValue(row.duration_seconds)}</dd></dl><div className="ops-rail-divider" /><div className="ops-panel-title">Best metrics so far</div><table className="ops-mini-table"><tbody><tr><td>W-F1</td><td>0.782</td></tr><tr><td>mAP@0.5</td><td>0.774</td></tr><tr><td>Recall</td><td>0.756</td></tr></tbody></table></div>;
}
function DataSpecificPage({ kind, specId, spec, rows, load }: { kind: PageKind; specId: string; spec: OpsSpecDetail; rows: OpsRowsResponse | null; load: () => void }) {
    const [selected, setSelected] = React.useState<Row | undefined>(rows?.rows?.[0]);
    React.useEffect(() => { if (!selected && rows?.rows?.[0]) setSelected(rows.rows[0]); }, [rows, selected]);
    const isTraining = kind === "training";
    return <><PageHeader breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: pageTitles[kind], href: `/${kind}` }, { label: spec.title }]} title={`${spec.title} - ${isTraining ? "Training" : "Frozen Datasets"}`} subtitle={isTraining ? `Training runs for the ${spec.title} specification` : `Snapshot registry for the ${spec.title} specification`} statusChips={[{ label: "Running", variant: "success" }, { label: spec.title, variant: "purple" }]} onRefresh={load} primaryAction={isTraining ? { label: "New Training Run" } : undefined} />{isTraining ? <div className="ops-detail-with-rail"><div><div className="ops-spec-mini-summary"><MiniMetric label="Total runs" value={rows?.total ?? 0} /><MiniMetric label="Completed" value="-" /><MiniMetric label="Failed" value="-" /><MiniMetric label="Best W-F1" value="-" /></div><div className="ops-panel ops-polished-panel"><TrainingTable specId={specId} rows={rows} selected={String(selected?.run_id ?? selected?.model_id ?? "")} onSelect={setSelected} /></div><div className="ops-panel ops-polished-panel ops-curves-placeholder"><Tabs defaultActiveKey="curves"><Tabs.TabPane tab="Curves" key="curves"><div className="ops-placeholder-charts"><div /><div /></div></Tabs.TabPane><Tabs.TabPane tab="Parameters" key="parameters"><div className="ops-muted">Parameters will appear when the run exposes them.</div></Tabs.TabPane><Tabs.TabPane tab="Artifacts" key="artifacts"><div className="ops-muted">Artifacts are linked from the selected run.</div></Tabs.TabPane></Tabs></div></div><TrainingRail row={selected} specId={specId} /></div> : <div className="ops-panel ops-polished-panel ops-spec-table-panel"><div className="ops-panel-title">Frozen datasets</div><FrozenDatasetTable specId={specId} rows={rows} /></div>}</>;
}
export function OpsSpecificSpecPage({ kind }: { kind: PageKind }) {
    const { specId = "" } = useParams(); const { pipelineId, loading: pidLoading } = usePipelineId();
    const [spec, setSpec] = React.useState<OpsSpecDetail | null>(null); const [rows, setRows] = React.useState<OpsRowsResponse | null>(null); const [loading, setLoading] = React.useState(true); const [error, setError] = React.useState<string | null>(null);
    const load = React.useCallback(() => { if (!pipelineId || !specId) return; setLoading(true); setError(null); const detail = opsApi.getOpsSpec(pipelineId, specId); const rowData = kind === "frozen-datasets" ? opsApi.getOpsFrozenRows(pipelineId, specId, { limit: 50 }) : kind === "training" ? opsApi.getOpsTrainingRows(pipelineId, specId, { limit: 50 }) : Promise.resolve(null); Promise.all([detail, rowData]).then(([s, r]) => { setSpec(s); setRows(r); }).catch((e) => setError(String(e))).finally(() => setLoading(false)); }, [pipelineId, specId, kind]);
    React.useEffect(() => { load(); }, [load]);
    return <div className="ops-page ops-spec-page"><EmptyState loading={pidLoading || loading} error={error} empty={!spec && !loading}>{spec && pipelineId && (kind === "frozen-datasets" || kind === "training") && <DataSpecificPage kind={kind} specId={specId} spec={spec} rows={rows} load={load} />}{spec && pipelineId && (kind === "metrics" || kind === "class-metrics") && <><PageHeader breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: pageTitles[kind], href: `/${kind}` }, { label: spec.title }]} title={`${spec.title} - ${pageTitles[kind]}`} subtitle={kind === "metrics" ? `Metric tables for the ${spec.title} specification` : `Per-class metrics for the ${spec.title} specification`} statusChips={[{ label: "Running", variant: "success" }, { label: spec.title, variant: "purple" }]} onRefresh={load} extra={kind === "metrics" ? <Button icon={<CodeOutlined />}>View JSON Spec</Button> : undefined} />{(kind === "metrics" ? spec.metrics : spec.class_metrics).map((table) => <SpecMetricTable key={table.id} pipelineId={pipelineId} specId={specId} table={table} classMetrics={kind === "class-metrics"} />)}</>}</EmptyState></div>;
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
