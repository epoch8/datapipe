import React from "react";
import type { ColumnsType } from "antd/es/table";
import type { MetricFormat, MetricsModelRow, MetricsTableSchema } from "../../../types/ops";
import { MetricValue, readMetricNumber } from "../shared";
import { buildAllMetricsSchema, type MetricsViewMode } from "./metricsSchema";

type LeafSpec = {
    key: string;
    title: string;
    format?: MetricFormat;
};

type GroupSpec = {
    title: string;
    key: string;
    metrics: LeafSpec[];
};

function metricFormat(format: MetricFormat): "float" | "percent" | "integer" | "string" {
    if (format === "duration") return "float";
    return format;
}

function metricCell(row: MetricsModelRow, key: string, format: MetricFormat = "float"): React.ReactNode {
    const value = readMetricNumber(row.metrics, key);
    if (value == null) {
        return null;
    }
    return <MetricValue value={value} format={metricFormat(format)} />;
}

function leafColumn(spec: LeafSpec): ColumnsType<MetricsModelRow>[number] {
    return {
        title: spec.title,
        key: spec.key,
        width: spec.format === "integer" ? 80 : 100,
        sorter: true,
        align: "right",
        render: (_: unknown, row: MetricsModelRow) => metricCell(row, spec.key, spec.format ?? "float"),
    };
}

function groupColumn(spec: GroupSpec): ColumnsType<MetricsModelRow>[number] {
    if (spec.metrics.length === 1) {
        return leafColumn({ ...spec.metrics[0], title: spec.title });
    }
    return {
        title: spec.title,
        key: spec.key,
        children: spec.metrics.map((m) => leafColumn(m)),
    };
}

const CLASSIFICATION_DETAILED: GroupSpec[] = [
    {
        title: "F1",
        key: "grp_f1",
        metrics: [
            { key: "weighted_f1_score", title: "W-F1" },
            { key: "macro_f1_score", title: "M-F1" },
        ],
    },
    {
        title: "Precision",
        key: "grp_precision",
        metrics: [
            { key: "weighted_without_pseudo_classes_precision", title: "W-Precision" },
            { key: "macro_without_pseudo_classes_precision", title: "M-Precision" },
        ],
    },
    {
        title: "Recall",
        key: "grp_recall",
        metrics: [
            { key: "weighted_without_pseudo_classes_recall", title: "W-Recall" },
            { key: "macro_without_pseudo_classes_recall", title: "M-Recall" },
        ],
    },
    {
        title: "Accuracy",
        key: "accuracy",
        metrics: [{ key: "accuracy", title: "Accuracy" }],
    },
];

const DETECTION_DETAILED: GroupSpec[] = [
    {
        title: "mAP",
        key: "grp_map",
        metrics: [
            { key: "mAP50", title: "mAP50" },
            { key: "mAP50_95", title: "mAP50-95" },
        ],
    },
    {
        title: "F1",
        key: "grp_f1",
        metrics: [
            { key: "weighted_f1_score", title: "W-F1" },
            { key: "macro_f1_score", title: "M-F1" },
        ],
    },
    {
        title: "Precision",
        key: "grp_precision",
        metrics: [
            { key: "weighted_precision", title: "W-Precision" },
            { key: "macro_precision", title: "M-Precision" },
        ],
    },
    {
        title: "Recall",
        key: "grp_recall",
        metrics: [
            { key: "weighted_recall", title: "W-Recall" },
            { key: "macro_recall", title: "M-Recall" },
        ],
    },
];

function layoutForTask(taskType: string): GroupSpec[] {
    const tt = taskType.toLowerCase();
    if (tt === "detection") return DETECTION_DETAILED;
    return CLASSIFICATION_DETAILED;
}

export function buildMetricColumns(
    schema: MetricsTableSchema,
    viewMode: MetricsViewMode,
    rows: MetricsModelRow[],
): ColumnsType<MetricsModelRow> {
    if (viewMode === "all") {
        const allSchema = buildAllMetricsSchema(schema, rows);
        const cols: ColumnsType<MetricsModelRow> = [];
        for (const group of allSchema.groups) {
            if (group.metrics.length === 1) {
                const m = group.metrics[0];
                cols.push(leafColumn({ key: m.key, title: m.label, format: m.format }));
                continue;
            }
            cols.push(
                groupColumn({
                    title: group.label,
                    key: group.key,
                    metrics: group.metrics.map((m) => ({
                        key: m.key,
                        title: m.short_label,
                        format: m.format,
                    })),
                }),
            );
        }
        return cols;
    }

    const cols = layoutForTask(schema.task_type).map((g) => groupColumn(g));
    cols.push(leafColumn({ key: "support", title: "Support", format: "integer" }));
    return cols;
}
