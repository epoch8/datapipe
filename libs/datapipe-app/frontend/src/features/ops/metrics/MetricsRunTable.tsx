import React from "react";
import { Button, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { MetricsRunRow } from "../../../types/ops";
import { MetricValue, SortableDataTable, TrendDelta, type SortSpec } from "../shared";

type Props = {
    rows: MetricsRunRow[];
    total: number;
    page: number;
    pageSize: number;
    loading?: boolean;
    selectedRunIds: string[];
    onPageChange: (page: number, pageSize: number) => void;
    onSortChange: (sorts: SortSpec[]) => void;
    onSelectionChange: (ids: string[]) => void;
    onCompare?: () => void;
};

export function MetricsRunTable({
    rows,
    total,
    page,
    pageSize,
    loading,
    selectedRunIds,
    onPageChange,
    onSortChange,
    onSelectionChange,
    onCompare,
}: Props) {
    const columns: ColumnsType<MetricsRunRow> = [
        { title: "run_id", dataIndex: "run_id", sorter: true, width: 120 },
        { title: "started_at", dataIndex: "started_at", sorter: true, render: (v) => v?.slice(0, 16)?.replace("T", " ") ?? "—" },
        {
            title: "model_id",
            dataIndex: "model_id",
            render: (v, r) => (
                <span>
                    {v} {r.model_version && <Tag>{r.model_version}</Tag>}
                </span>
            ),
        },
        { title: "subset", dataIndex: "subset", sorter: true },
        { title: "mAP50", key: "mAP50", sorter: true, render: (_, r) => <MetricValue value={r.metrics.mAP50} /> },
        { title: "mAP50-95", key: "mAP50_95", sorter: true, render: (_, r) => <MetricValue value={r.metrics.mAP50_95} /> },
        { title: "precision", key: "precision", render: (_, r) => <MetricValue value={r.metrics.precision} /> },
        {
            title: "recall",
            key: "recall",
            render: (_, r) => (
                <span>
                    <MetricValue value={r.metrics.recall} />
                    {r.delta_pct?.recall != null && (
                        <TrendDelta deltaPct={r.delta_pct.recall} />
                    )}
                </span>
            ),
        },
        { title: "F1", key: "f1", render: (_, r) => <MetricValue value={r.metrics.f1_score} /> },
        { title: "IoU", key: "iou", render: (_, r) => <MetricValue value={r.metrics.iou_mean} /> },
        { title: "support", key: "support", render: (_, r) => <MetricValue value={r.metrics.support} format="integer" /> },
        { title: "duration", dataIndex: "duration_s", render: (v) => (v ? `${Math.floor(v / 60)}m ${v % 60}s` : "—") },
        {
            title: "status",
            dataIndex: "status",
            render: (v) => (
                <Tag color={v === "success" ? "green" : v === "failed" ? "red" : "blue"}>{v ?? "—"}</Tag>
            ),
        },
    ];

    return (
        <SortableDataTable
            title="Metrics by run"
            extra={
                onCompare && (
                    <Button type="primary" disabled={selectedRunIds.length < 2} onClick={onCompare}>
                        Compare runs ({selectedRunIds.length})
                    </Button>
                )
            }
            columns={columns}
            dataSource={rows}
            rowKey="run_id"
            loading={loading}
            total={total}
            page={page}
            pageSize={pageSize}
            onPageChange={onPageChange}
            onSortChange={onSortChange}
            rowSelection={{
                selectedRowKeys: selectedRunIds,
                onChange: (keys) => onSelectionChange(keys as string[]),
            }}
            scroll={{ x: 1200 }}
        />
    );
}
