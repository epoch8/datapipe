import React from "react";
import { Button, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { MetricsRunRow } from "../../../types/opsMl";
import { MetricValue, SortableDataTable, readMetricNumber, type SortSpec } from "../shared";

type Props = {
    rows: MetricsRunRow[];
    total: number;
    page: number;
    pageSize: number;
    loading?: boolean;
    activeSorts?: SortSpec[];
    selectedRunIds: string[];
    onPageChange: (page: number, pageSize: number) => void;
    onSortChange: (sorts: SortSpec[]) => void;
    onSelectionChange: (ids: string[]) => void;
    onCompare?: () => void;
};

// Human labels for known metric keys; unknown keys fall back to their raw name.
const METRIC_LABELS: Record<string, string> = {
    mAP50: "mAP50",
    mAP50_95: "mAP50-95",
    precision: "precision",
    recall: "recall",
    f1_score: "F1",
    accuracy: "accuracy",
    iou_mean: "IoU",
    weighted_precision: "w-precision",
    weighted_recall: "w-recall",
    weighted_f1_score: "w-F1",
    macro_precision: "m-precision",
    macro_recall: "m-recall",
    macro_f1_score: "m-F1",
    weighted_without_pseudo_classes_precision: "w-precision (np)",
    weighted_without_pseudo_classes_recall: "w-recall (np)",
    weighted_without_pseudo_classes_f1_score: "w-F1 (np)",
    macro_without_pseudo_classes_precision: "m-precision (np)",
    macro_without_pseudo_classes_recall: "m-recall (np)",
    macro_without_pseudo_classes_f1_score: "m-F1 (np)",
    pose_P: "pose-P",
    pose_R: "pose-R",
    pose_mAP50: "pose-mAP50",
    pose_mAP50_95: "pose-mAP50-95",
};

// Preferred left-to-right ordering; any present key not listed here is appended
// afterwards in alphabetical order so new metrics still show up automatically.
const METRIC_ORDER = [
    "mAP50",
    "mAP50_95",
    "weighted_f1_score",
    "macro_f1_score",
    "weighted_precision",
    "weighted_recall",
    "macro_precision",
    "macro_recall",
    "accuracy",
    "precision",
    "recall",
    "f1_score",
    "iou_mean",
    "weighted_without_pseudo_classes_f1_score",
    "macro_without_pseudo_classes_f1_score",
    "pose_mAP50",
    "pose_mAP50_95",
    "pose_P",
    "pose_R",
];

// Count-like fields are rendered separately (support) or hidden, never as metrics.
const COUNT_KEYS = new Set([
    "images_support",
    "support",
    "TP",
    "FP",
    "FN",
    "TP_extra_bbox",
    "FP_extra_bbox",
    "FN_extra_bbox",
]);

function presentMetricKeys(rows: MetricsRunRow[]): string[] {
    const present = new Set<string>();
    for (const row of rows) {
        for (const key of Object.keys(row.metrics ?? {})) {
            if (readMetricNumber(row.metrics, key) != null && !COUNT_KEYS.has(key)) present.add(key);
        }
    }
    const ordered = METRIC_ORDER.filter((k) => present.has(k));
    const extra = Array.from(present).filter((k) => !METRIC_ORDER.includes(k)).sort();
    return [...ordered, ...extra];
}

export function MetricsRunTable({
    rows,
    total,
    page,
    pageSize,
    loading,
    activeSorts,
    selectedRunIds,
    onPageChange,
    onSortChange,
    onSelectionChange,
    onCompare,
}: Props) {
    const metricKeys = React.useMemo(() => presentMetricKeys(rows), [rows]);

    const metricColumns: ColumnsType<MetricsRunRow> = metricKeys.map((key) => ({
        title: METRIC_LABELS[key] ?? key,
        key,
        sorter: true,
        render: (_: unknown, r: MetricsRunRow) => (
            <MetricValue value={readMetricNumber(r.metrics, key) ?? undefined} />
        ),
    }));

    const columns: ColumnsType<MetricsRunRow> = [
        { title: "run_id", dataIndex: "run_id", sorter: true, width: 120 },
        { title: "started_at", dataIndex: "started_at", sorter: true, render: (v) => v?.slice(0, 16)?.replace("T", " ") ?? "—" },
        {
            title: "model_id",
            dataIndex: "model_id",
            render: (v, r) => (
                <span>
                    {v || "—"} {r.model_version && <Tag>{r.model_version}</Tag>}
                </span>
            ),
        },
        { title: "dataset_id", dataIndex: "dataset_id", render: (v) => v || "—" },
        {
            title: "train items",
            dataIndex: "train_items",
            render: (v) => <MetricValue value={v} format="integer" />,
        },
        {
            title: "val items",
            dataIndex: "val_items",
            render: (v) => <MetricValue value={v} format="integer" />,
        },
        { title: "subset", dataIndex: "subset", sorter: true },
        ...metricColumns,
        { title: "support", key: "support", sorter: true, render: (_, r) => <MetricValue value={readMetricNumber(r.metrics, "support") ?? undefined} format="integer" /> },
        { title: "duration", key: "duration_s", sorter: true, render: (_, r) => (r.duration_s ? `${Math.floor(r.duration_s / 60)}m ${r.duration_s % 60}s` : "—") },
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
            activeSorts={activeSorts}
            rowSelection={{
                selectedRowKeys: selectedRunIds,
                onChange: (keys) => onSelectionChange(keys as string[]),
            }}
            scroll={{ x: 1200 }}
        />
    );
}
