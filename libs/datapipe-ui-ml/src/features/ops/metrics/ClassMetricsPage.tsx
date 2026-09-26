import React from "react";
import { Drawer, Tabs } from "antd";
import type { ColumnType, ColumnsType } from "antd/es/table";
import { opsApi } from "@datapipe/ui-ml/api/client";
import { usePipelineId } from "@datapipe/ui/hooks/usePipelineId";
import { useUrlNumber, useUrlState } from "@datapipe/ui/hooks/useUrlState";
import type { ClassMetricDetailResponse, ClassMetricRow, ClassMetricsResponse } from "../../../types/opsMl";
import {
    ChartCard,
    EmptyState,
    FilterBar,
    KpiCard,
    MetricValue,
    PageHeader,
    SortableDataTable,
    Sparkline,
    SelectedModelChips,
    TrendDelta,
    parseSortParams,
    serializeSortParams,
    type SortSpec,
} from "../shared";

function sortableColumn(
    priority: number,
    col: ColumnType<ClassMetricRow>,
): ColumnType<ClassMetricRow> {
    const field = col.dataIndex ?? col.key;
    if (!field) return col;
    return { ...col, sorter: { multiple: priority } };
}

export function ClassMetricsPage() {
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [subsetParam, setSubset] = useUrlState("subset");
    const subset = subsetParam || "val";
    const apiSubset = subset === "all" ? undefined : subset;
    const [modelIdParam, setModelIdParam] = useUrlState("model_id");
    const modelIds = React.useMemo(
        () => modelIdParam.split(",").map((s) => s.trim()).filter(Boolean),
        [modelIdParam],
    );
    const [labelSearch, setLabelSearch] = useUrlState("label_search");
    const [sortBy, setSortBy] = useUrlState("sort_by", "f1_score");
    const [sortDir, setSortDir] = useUrlState("sort_dir", "desc");
    const [page, setPage] = useUrlNumber("page", 1);
    const [selectedLabel, setSelectedLabel] = React.useState<string | undefined>();
    const [data, setData] = React.useState<ClassMetricsResponse | null>(null);
    const [detail, setDetail] = React.useState<ClassMetricDetailResponse | null>(null);
    const [modelOptions, setModelOptions] = React.useState<string[]>([]);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const pageSize = 25;

    const load = React.useCallback(() => {
        if (!pipelineId) return;
        setLoading(true);
        opsApi.getClassMetrics(pipelineId, {
            subset: apiSubset,
            model_id: modelIds.length ? modelIds.join(",") : undefined,
            label_search: labelSearch || undefined,
            sort_by: sortBy || undefined,
            sort_dir: sortDir as "asc" | "desc",
            limit: pageSize,
            offset: (page - 1) * pageSize,
        })
            .then(setData)
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, apiSubset, modelIds, labelSearch, sortBy, sortDir, page]);

    React.useEffect(() => {
        if (!pipelineId) return;
        opsApi.getMetricsRuns(pipelineId, { subset: apiSubset, limit: 1 })
            .then((runsRes) => setModelOptions(runsRes.available_filters.models))
            .catch(() => undefined);
    }, [pipelineId, apiSubset]);

    React.useLayoutEffect(() => {
        if (subsetParam) return;
        setSubset("val");
    }, [subsetParam, setSubset]);

    React.useEffect(() => { load(); }, [load]);

    React.useEffect(() => {
        if (!pipelineId || !selectedLabel) { setDetail(null); return; }
        opsApi.getClassDetail(pipelineId, selectedLabel, { subset: apiSubset }).then(setDetail).catch(() => setDetail(null));
    }, [pipelineId, selectedLabel, apiSubset]);

    const activeSorts = React.useMemo(() => parseSortParams(sortBy, sortDir), [sortBy, sortDir]);

    const handleSortChange = React.useCallback((sorts: SortSpec[]) => {
        const { sort_by, sort_dir } = serializeSortParams(sorts);
        setSortBy(sort_by || "f1_score");
        setSortDir(sort_dir || "desc");
        setPage(1);
    }, [setSortBy, setSortDir, setPage]);

    const columns: ColumnsType<ClassMetricRow> = [
        sortableColumn(1, {
            title: "label",
            dataIndex: "label",
            render: (v) => (
                <button type="button" className="ops-link-button" onClick={() => setSelectedLabel(String(v))}>
                    {v}
                </button>
            ),
        }),
        sortableColumn(2, { title: "subset_id", dataIndex: "subset", width: 72 }),
        sortableColumn(3, { title: "model_id", dataIndex: "model_id", width: 220, ellipsis: true }),
        sortableColumn(4, {
            title: "images_support",
            dataIndex: "images_support",
            render: (v) => <MetricValue value={v} format="integer" />,
        }),
        sortableColumn(5, {
            title: "support",
            dataIndex: "support",
            render: (v) => <MetricValue value={v} format="integer" />,
        }),
        sortableColumn(6, { title: "TP", dataIndex: "TP", render: (v) => <MetricValue value={v} format="integer" /> }),
        sortableColumn(7, { title: "FP", dataIndex: "FP", render: (v) => <MetricValue value={v} format="integer" /> }),
        sortableColumn(8, { title: "FN", dataIndex: "FN", render: (v) => <MetricValue value={v} format="integer" /> }),
        sortableColumn(9, { title: "precision", dataIndex: "precision", render: (v) => <MetricValue value={v} /> }),
        sortableColumn(10, { title: "recall", dataIndex: "recall", render: (v) => <MetricValue value={v} /> }),
        sortableColumn(11, {
            title: "F1",
            dataIndex: "f1_score",
            render: (v, r) => (
                <span>
                    <MetricValue value={v} />
                    {r.delta?.f1_score != null && <TrendDelta delta={r.delta.f1_score} />}
                </span>
            ),
        }),
        sortableColumn(12, { title: "IoU mean", dataIndex: "iou_mean", render: (v) => <MetricValue value={v} /> }),
        sortableColumn(13, { title: "mAP50", dataIndex: "mAP50", render: (v) => <MetricValue value={v} /> }),
        sortableColumn(14, { title: "mAP50-95", dataIndex: "mAP50_95", render: (v) => <MetricValue value={v} /> }),
        sortableColumn(15, { title: "pose P", dataIndex: "pose_P", render: (v) => <MetricValue value={v} /> }),
        sortableColumn(16, { title: "pose R", dataIndex: "pose_R", render: (v) => <MetricValue value={v} /> }),
        sortableColumn(17, { title: "pose mAP50", dataIndex: "pose_mAP50", render: (v) => <MetricValue value={v} /> }),
        sortableColumn(18, { title: "pose mAP50-95", dataIndex: "pose_mAP50_95", render: (v) => <MetricValue value={v} /> }),
    ];

    const displayPipeline = pipelineId || "image_detection_e2e";
    const summary = data?.summary;
    const modelFilterOptions = React.useMemo(() => {
        const ids = new Set([...modelOptions, ...modelIds]);
        return Array.from(ids).sort().map((m) => ({ label: m, value: m }));
    }, [modelOptions, modelIds]);

    const removeModel = React.useCallback(
        (id: string) => {
            const next = modelIds.filter((item) => item !== id);
            setModelIdParam(next.length ? next.join(",") : "");
            setPage(1);
        },
        [modelIds, setModelIdParam, setPage],
    );

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: "Class Metrics" }]}
                title="Class Metrics Explorer"
                subtitle={`Per-class metrics for ${displayPipeline}`}
                statusChips={[{ label: "Detection", variant: "purple" }]}
                onRefresh={load}
            />

            <FilterBar
                filters={[
                    { key: "subset", label: "Subset", value: subset, options: [{ label: "All", value: "all" }, { label: "train", value: "train" }, { label: "val", value: "val" }, { label: "test", value: "test" }] },
                    {
                        key: "model_id",
                        label: "Model",
                        mode: "multiple",
                        minWidth: 360,
                        dropdownMinWidth: 360,
                        value: modelIds,
                        placeholder: "All models",
                        options: modelFilterOptions,
                    },
                ]}
                onFilterChange={(key, val) => {
                    if (key === "subset") {
                        const v = Array.isArray(val) ? val[0] : val;
                        setSubset(v === "all" || !v ? "all" : v);
                    }
                    if (key === "model_id") {
                        const ids = Array.isArray(val) ? val : val ? [val] : [];
                        setModelIdParam(ids.length ? ids.join(",") : "");
                    }
                    setPage(1);
                }}
                search={labelSearch}
                onSearchChange={setLabelSearch}
                searchPlaceholder="Search labels…"
            />

            <SelectedModelChips modelIds={modelIds} onRemove={removeModel} />

            <EmptyState loading={pidLoading || loading} error={error} empty={!data?.rows.length && !loading}>
                {summary && (
                    <div className="ops-kpi-row">
                        <KpiCard label="Total classes" value={summary.total_classes} format="integer" />
                        <KpiCard label="Macro F1" value={summary.macro_f1} />
                        <KpiCard label="Weighted F1" value={summary.weighted_f1} />
                        <KpiCard label="Best class" value={summary.best_classes?.[0]?.label ?? "—"} format="string" subtitle={`F1 ${summary.best_classes?.[0]?.f1_score?.toFixed(3)}`} />
                        <KpiCard label="Worst class" value={summary.worst_classes?.[0]?.label ?? "—"} format="string" />
                    </div>
                )}

                <Tabs defaultActiveKey="explorer">
                    <Tabs.TabPane tab="Per-class explorer" key="explorer">
                        <SortableDataTable
                            title="Metrics by class"
                            columns={columns}
                            dataSource={data?.rows ?? []}
                            rowKey={(r) => `${r.label}|${r.subset ?? ""}|${r.model_id ?? ""}`}
                            total={data?.total ?? 0}
                            page={page}
                            pageSize={pageSize}
                            onPageChange={(p) => setPage(p)}
                            activeSorts={activeSorts}
                            multiSort
                            onSortChange={handleSortChange}
                            scroll={{ x: 1600 }}
                        />

                        <div className="ops-chart-grid" style={{ marginTop: 16 }}>
                            <ChartCard
                                spec={{
                                    id: "worst-f1",
                                    title: "Worst classes by F1",
                                    type: "bar",
                                    xLabel: "Class",
                                    yLabel: "F1",
                                    series: [{
                                        key: "f1",
                                        label: "F1",
                                        color: "#EF4444",
                                        points: (summary?.worst_classes ?? []).map((c) => ({ x: c.label, y: c.f1_score ?? null })),
                                    }],
                                }}
                            />
                            <ChartCard
                                spec={{
                                    id: "pr-class",
                                    title: "Precision vs Recall by class",
                                    type: "scatter",
                                    xLabel: "Precision",
                                    yLabel: "Recall",
                                    series: [{
                                        key: "classes",
                                        label: "Classes",
                                        points: (data?.rows ?? []).map((c) => ({ x: c.precision ?? 0, y: c.recall ?? null })),
                                    }],
                                }}
                            />
                        </div>
                    </Tabs.TabPane>
                </Tabs>
            </EmptyState>

            <Drawer
                className="ops-class-drawer"
                title={selectedLabel}
                visible={!!selectedLabel}
                onClose={() => setSelectedLabel(undefined)}
                width={360}
            >
                {detail && (
                    <>
                        <p>Class ID: {detail.class_id}</p>
                        <p>Instances: <MetricValue value={detail.latest.support} format="integer" /></p>
                        <p>F1: <MetricValue value={detail.latest.f1_score} /></p>
                        {detail.trends[0] && <Sparkline data={detail.trends[0].points.map((p) => ({ x: p.x, y: p.y }))} height={48} />}
                        {detail.error_breakdown && (
                            <div style={{ marginTop: 16 }}>
                                <strong>Error breakdown</strong>
                                <div>FN: {detail.error_breakdown.false_negatives}</div>
                                <div>FP: {detail.error_breakdown.false_positives}</div>
                                <div>Localization: {detail.error_breakdown.localization_errors}</div>
                            </div>
                        )}
                        {detail.gallery_url && <span className="ops-model-chip">FiftyOne gallery</span>}
                    </>
                )}
            </Drawer>
        </div>
    );
}
