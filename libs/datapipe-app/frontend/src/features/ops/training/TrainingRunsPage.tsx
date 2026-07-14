import React from "react";
import { Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import { useUrlState } from "../../../hooks/useUrlState";
import type { TrainingRunRow } from "../../../types/ops";
import {
    EmptyState,
    FilterBar,
    MetricValue,
    PageHeader,
    SortableDataTable,
} from "../shared";

export function TrainingRunsPage() {
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [search, setSearch] = useUrlState("search");
    const [runs, setRuns] = React.useState<TrainingRunRow[]>([]);
    const [total, setTotal] = React.useState(0);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [page, setPage] = React.useState(1);
    const pageSize = 10;

    const load = React.useCallback(() => {
        if (!pipelineId) return;
        setLoading(true);
        opsApi
            .getTrainingRuns(pipelineId, { search: search || undefined, limit: pageSize, offset: (page - 1) * pageSize })
            .then((res) => {
                setRuns(res.rows);
                setTotal(res.total);
            })
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, search, page]);

    React.useEffect(() => {
        load();
    }, [load]);

    const bestRun = runs.find((r) => r.is_best) ?? runs[0];
    const displayPipeline = pipelineId || "image_detection_e2e";

    const columns: ColumnsType<TrainingRunRow> = [
        { title: "Run ID", dataIndex: "run_key" },
        { title: "Model", dataIndex: "model_id" },
        { title: "Task", dataIndex: "task_type", render: (v) => <Tag color="purple">{v}</Tag> },
        { title: "Framework", dataIndex: "framework" },
        { title: "Started", dataIndex: "started_at", render: (v) => v?.slice(0, 16)?.replace("T", " ") },
        { title: "Duration", dataIndex: "duration_s", render: (v) => (v ? `${Math.floor(v / 60)}m` : "—") },
        { title: "Best metric", render: (_, r) => `${r.best_metric_name}: ${r.best_metric_value?.toFixed(3) ?? "—"}` },
        { title: "Status", dataIndex: "status", render: (v) => <Tag color={v === "success" ? "green" : "red"}>{v}</Tag> },
        { title: "Tags", dataIndex: "tags", render: (tags: string[]) => tags?.map((t) => <Tag key={t}>{t}</Tag>) },
    ];

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: "Training / Runs" }]}
                title="Training Runs"
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
                    />

                    {bestRun && (
                        <div className="ops-panel">
                            <div className="ops-panel-title">Best run</div>
                            <div style={{ fontSize: 13 }}>
                                <div><strong>{bestRun.run_key}</strong> <Tag color="green">{bestRun.status}</Tag></div>
                                <div>Model: {bestRun.model_id}</div>
                                <div>Task: {bestRun.task_type} · {bestRun.framework}</div>
                                <div style={{ marginTop: 8 }}>
                                    <MetricValue value={bestRun.best_metric_value} />{" "}
                                    <span style={{ fontSize: 12, color: "var(--dp-gray-500)" }}>{bestRun.best_metric_name}</span>
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
            </EmptyState>
        </div>
    );
}
