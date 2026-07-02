import React from "react";
import { Checkbox, Slider, Switch, Tabs, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import { useUrlState } from "../../../hooks/useUrlState";
import type { TrainingCompareResponse, TrainingRunRow } from "../../../types/ops";
import {
    ChartCard,
    EmptyState,
    FilterBar,
    MetricValue,
    PageHeader,
    RUN_COLORS,
    SortableDataTable,
    smoothPoints,
} from "../shared";

export function TrainingRunsPage() {
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [search, setSearch] = useUrlState("search");
    const [selectedRunKeys, setSelectedRunKeys] = React.useState<string[]>([]);
    const [runs, setRuns] = React.useState<TrainingRunRow[]>([]);
    const [total, setTotal] = React.useState(0);
    const [compare, setCompare] = React.useState<TrainingCompareResponse | null>(null);
    const [selectedMetrics, setSelectedMetrics] = React.useState<string[]>([]);
    const [smoothing, setSmoothing] = React.useState(0.6);
    const [logScale, setLogScale] = React.useState(false);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [page, setPage] = React.useState(1);
    const pageSize = 10;
    const [activeTab, setActiveTab] = React.useState("curves");

    const load = React.useCallback(() => {
        if (!pipelineId) return;
        setLoading(true);
        opsApi
            .getTrainingRuns(pipelineId, { search: search || undefined, limit: pageSize, offset: (page - 1) * pageSize })
            .then((res) => { setRuns(res.rows); setTotal(res.total); })
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, search, page]);

    React.useEffect(() => { load(); }, [load]);

    React.useEffect(() => {
        const params = new URLSearchParams(window.location.search);
        const keys = params.get("run_keys");
        if (keys) setSelectedRunKeys(keys.split(",").filter(Boolean).slice(0, 4));
    }, []);

    React.useEffect(() => {
        if (selectedRunKeys.length < 1) { setCompare(null); return; }
        opsApi.compareTraining(selectedRunKeys.slice(0, 4)).then((res) => {
            setCompare(res);
            if (!selectedMetrics.length && res.available_metrics.length) {
                setSelectedMetrics(res.available_metrics.slice(0, 6).map((m) => m.key));
            }
        }).catch(() => setCompare(null));
    }, [selectedRunKeys, selectedMetrics.length]);

    const bestRun = runs.find((r) => r.is_best) ?? runs[0];

    const columns: ColumnsType<TrainingRunRow> = [
        {
            title: "Run ID",
            dataIndex: "run_key",
            render: (v, _, i) => (
                <span style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <span style={{ width: 4, height: 20, background: RUN_COLORS[i % RUN_COLORS.length], borderRadius: 2 }} />
                    {v}
                </span>
            ),
        },
        { title: "Model", dataIndex: "model_id" },
        { title: "Task", dataIndex: "task_type", render: (v) => <Tag color="purple">{v}</Tag> },
        { title: "Framework", dataIndex: "framework" },
        { title: "Started", dataIndex: "started_at", render: (v) => v?.slice(0, 16)?.replace("T", " ") },
        { title: "Duration", dataIndex: "duration_s", render: (v) => (v ? `${Math.floor(v / 60)}m` : "—") },
        { title: "Best metric", render: (_, r) => `${r.best_metric_name}: ${r.best_metric_value?.toFixed(3) ?? "—"}` },
        { title: "Status", dataIndex: "status", render: (v) => <Tag color={v === "success" ? "green" : "red"}>{v}</Tag> },
        { title: "Tags", dataIndex: "tags", render: (tags: string[]) => tags?.map((t) => <Tag key={t}>{t}</Tag>) },
    ];

    const displayPipeline = pipelineId || "image_detection_e2e";
    const charts = compare?.charts.filter((c) => selectedMetrics.includes(c.metric)) ?? [];

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: "Training / Runs" }]}
                title="Training Runs & Curves"
                statusChips={[{ label: "Running", variant: "success" }, { label: "Detection", variant: "purple" }]}
                onRefresh={load}
                primaryAction={{ label: "New Training Run", href: `/pipelines/${displayPipeline}` }}
            />

            <FilterBar
                filters={[
                    { key: "task", label: "Task type", options: [{ label: "Detection", value: "detection" }, { label: "Keypoints", value: "keypoints" }] },
                    { key: "framework", label: "Framework", options: [{ label: "YOLOv8", value: "YOLOv8" }, { label: "YOLOv5", value: "YOLOv5" }] },
                ]}
                onFilterChange={() => undefined}
                search={search}
                onSearchChange={setSearch}
                searchPlaceholder="Search runs…"
            />

            <EmptyState loading={pidLoading || loading} error={error}>
                <div className="ops-training-layout">
                    <SortableDataTable
                        title={`Runs (${total})`}
                        columns={columns}
                        dataSource={runs}
                        rowKey="run_key"
                        total={total}
                        page={page}
                        pageSize={pageSize}
                        onPageChange={(p) => setPage(p)}
                        onSortChange={() => undefined}
                        rowSelection={{
                            selectedRowKeys: selectedRunKeys,
                            onChange: (keys) => setSelectedRunKeys((keys as string[]).slice(0, 4)),
                        }}
                    />

                    {bestRun && (
                        <div className="ops-panel">
                            <div className="ops-panel-title">Best run</div>
                            <div style={{ fontSize: 13 }}>
                                <div><strong>{bestRun.run_key}</strong> <Tag color="green">{bestRun.status}</Tag></div>
                                <div>Model: {bestRun.model_id}</div>
                                <div>Task: {bestRun.task_type} · {bestRun.framework}</div>
                                <div style={{ marginTop: 8 }}>
                                    <MetricValue value={bestRun.best_metric_value} /> <span style={{ fontSize: 12, color: "var(--dp-gray-500)" }}>{bestRun.best_metric_name}</span>
                                </div>
                                {bestRun.artifacts && (
                                    <div style={{ marginTop: 12 }}>
                                        <strong>Artifacts</strong>
                                        {Object.entries(bestRun.artifacts).map(([k, v]) => (
                                            <div key={k} style={{ fontSize: 12 }}>{k}: {String(v)}</div>
                                        ))}
                                    </div>
                                )}
                            </div>
                        </div>
                    )}
                </div>

                <Tabs activeKey={activeTab} onChange={setActiveTab}>
                    <Tabs.TabPane tab="Curves" key="curves">
                        {selectedRunKeys.length > 0 && (
                            <div style={{ marginBottom: 12, display: "flex", gap: 8, flexWrap: "wrap" }}>
                                {selectedRunKeys.map((k, i) => (
                                    <Tag key={k} color={RUN_COLORS[i]}>{k}</Tag>
                                ))}
                            </div>
                        )}
                        <div style={{ display: "flex", gap: 16 }}>
                            <div style={{ width: 200 }}>
                                <strong>Metrics</strong>
                                <Checkbox.Group
                                    style={{ display: "flex", flexDirection: "column", gap: 4, marginTop: 8 }}
                                    value={selectedMetrics}
                                    onChange={(v) => setSelectedMetrics(v as string[])}
                                    options={compare?.available_metrics.map((m) => ({ label: m.label, value: m.key })) ?? []}
                                />
                            </div>
                            <div style={{ flex: 1 }}>
                                <div style={{ display: "flex", gap: 16, alignItems: "center", marginBottom: 12 }}>
                                    <span>Smoothing: {smoothing.toFixed(1)}</span>
                                    <Slider min={0} max={0.95} step={0.05} value={smoothing} onChange={setSmoothing} style={{ width: 120 }} />
                                    <Switch checked={logScale} onChange={setLogScale} checkedChildren="Log" unCheckedChildren="Linear" />
                                </div>
                                <div className="ops-curves-grid">
                                    {charts.map((chart) => (
                                        <ChartCard
                                            key={chart.metric}
                                            spec={{
                                                id: chart.metric,
                                                title: chart.title,
                                                type: "line",
                                                xLabel: chart.x_label,
                                                yLabel: chart.y_label ?? chart.title,
                                                series: chart.series.map((s, i) => ({
                                                    key: s.run_key,
                                                    label: s.label,
                                                    color: RUN_COLORS[i % RUN_COLORS.length],
                                                    points: smoothPoints(s.points.map((p) => ({ x: p.x, y: p.y })), smoothing),
                                                })),
                                            }}
                                            height={180}
                                            logScale={logScale}
                                        />
                                    ))}
                                </div>
                            </div>
                        </div>
                    </Tabs.TabPane>
                    <Tabs.TabPane tab="Parameters" key="parameters">
                        {runs.filter((r) => selectedRunKeys.includes(r.run_key)).map((r) => (
                            <pre key={r.run_key} style={{ fontSize: 12 }}>{JSON.stringify(r.params, null, 2)}</pre>
                        ))}
                    </Tabs.TabPane>
                    <Tabs.TabPane tab="Artifacts" key="artifacts">
                        {runs.filter((r) => selectedRunKeys.includes(r.run_key)).map((r) => (
                            <div key={r.run_key}>
                                <strong>{r.run_key}</strong>
                                <pre style={{ fontSize: 12 }}>{JSON.stringify(r.artifacts, null, 2)}</pre>
                            </div>
                        ))}
                    </Tabs.TabPane>
                </Tabs>
            </EmptyState>
        </div>
    );
}
