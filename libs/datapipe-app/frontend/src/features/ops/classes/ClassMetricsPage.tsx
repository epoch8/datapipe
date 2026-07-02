import React from "react";
import { Drawer, Tabs, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import { useUrlState } from "../../../hooks/useUrlState";
import type { ClassMetricDetailResponse, ClassMetricRow, ClassMetricsResponse } from "../../../types/ops";
import {
    ChartCard,
    EmptyState,
    FilterBar,
    KpiCard,
    MetricValue,
    PageHeader,
    SortableDataTable,
    Sparkline,
    TrendDelta,
} from "../shared";

export function ClassMetricsPage() {
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [subset, setSubset] = useUrlState("subset");
    const [modelId, setModelId] = useUrlState("model_id");
    const [labelSearch, setLabelSearch] = useUrlState("label_search");
    const [selectedLabel, setSelectedLabel] = React.useState<string | undefined>();
    const [data, setData] = React.useState<ClassMetricsResponse | null>(null);
    const [detail, setDetail] = React.useState<ClassMetricDetailResponse | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [page, setPage] = React.useState(1);
    const pageSize = 25;

    const load = React.useCallback(() => {
        if (!pipelineId) return;
        setLoading(true);
        opsApi
            .getClassMetrics(pipelineId, { subset: subset || undefined, model_id: modelId || undefined, label_search: labelSearch || undefined, limit: pageSize, offset: (page - 1) * pageSize })
            .then(setData)
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, subset, modelId, labelSearch, page]);

    React.useEffect(() => { load(); }, [load]);

    React.useEffect(() => {
        if (!pipelineId || !selectedLabel) { setDetail(null); return; }
        opsApi.getClassDetail(pipelineId, selectedLabel, { subset: subset || undefined }).then(setDetail).catch(() => setDetail(null));
    }, [pipelineId, selectedLabel, subset]);

    const columns: ColumnsType<ClassMetricRow> = [
        { title: "label", dataIndex: "label", sorter: true, render: (v) => <a onClick={() => setSelectedLabel(String(v))}>{v}</a> },
        { title: "images_support", dataIndex: "images_support", render: (v) => <MetricValue value={v} format="integer" /> },
        { title: "support", dataIndex: "support", sorter: true, render: (v) => <MetricValue value={v} format="integer" /> },
        { title: "TP", dataIndex: "TP", render: (v) => <MetricValue value={v} format="integer" /> },
        { title: "FP", dataIndex: "FP", render: (v) => <MetricValue value={v} format="integer" /> },
        { title: "FN", dataIndex: "FN", render: (v) => <MetricValue value={v} format="integer" /> },
        { title: "precision", dataIndex: "precision", sorter: true, render: (v) => <MetricValue value={v} /> },
        { title: "recall", dataIndex: "recall", sorter: true, render: (v) => <MetricValue value={v} /> },
        { title: "F1", dataIndex: "f1_score", sorter: true, render: (v, r) => (
            <span>
                <MetricValue value={v} />
                {r.delta?.f1_score != null && <TrendDelta delta={r.delta.f1_score} />}
            </span>
        )},
        { title: "IoU mean", dataIndex: "iou_mean", render: (v) => <MetricValue value={v} /> },
        { title: "mAP50", dataIndex: "mAP50", render: (v) => <MetricValue value={v} /> },
        { title: "trend", key: "trend", render: (_, r) => (
            <Sparkline data={Array.from({ length: 8 }, (_, i) => ({ x: i, y: (r.f1_score ?? 0.7) + (i - 4) * 0.01 }))} />
        )},
    ];

    const displayPipeline = pipelineId || "image_detection_e2e";
    const summary = data?.summary;

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: "Classes" }]}
                title="Class Metrics Explorer"
                subtitle={`Per-class metrics for ${displayPipeline}`}
                statusChips={[{ label: "Detection", variant: "purple" }]}
                onRefresh={load}
            />

            <FilterBar
                filters={[
                    { key: "subset", label: "Subset", value: subset, options: [{ label: "All", value: "" }, { label: "train", value: "train" }, { label: "val", value: "val" }, { label: "test", value: "test" }] },
                    { key: "model_id", label: "Model", value: modelId, options: [{ label: "All", value: "" }, { label: "yolo11x.pt", value: "yolo11x.pt" }] },
                ]}
                onFilterChange={(key, val) => {
                    const v = Array.isArray(val) ? val[0] : val;
                    if (key === "subset") setSubset(v ?? "");
                    if (key === "model_id") setModelId(v ?? "");
                }}
                search={labelSearch}
                onSearchChange={setLabelSearch}
                searchPlaceholder="Search labels…"
            />

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
                            rowKey="label"
                            total={data?.total ?? 0}
                            page={page}
                            pageSize={pageSize}
                            onPageChange={(p) => setPage(p)}
                            onSortChange={() => undefined}
                            scroll={{ x: 1400 }}
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
                    <Tabs.TabPane tab="SQL / Custom query" key="sql">
                        <p style={{ color: "var(--dp-gray-500)" }}>Use <a href="/sql-studio">SQL Studio</a> for custom class queries.</p>
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
                        {detail.gallery_url && <Tag color="blue">FiftyOne gallery</Tag>}
                    </>
                )}
            </Drawer>
        </div>
    );
}
