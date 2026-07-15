import React from "react";
import { Tooltip } from "antd";
import type { MetricFormat } from "../../../types/opsMl";

type Props = {
    value?: number | null;
    label?: string;
    format?: MetricFormat;
    higherIsBetter?: boolean;
    isBest?: boolean;
    delta?: number | null;
};

function formatValue(value: number, format: MetricFormat): string {
    if (format === "integer") return String(Math.round(value));
    if (format === "percent") return `${(value * 100).toFixed(1)}%`;
    return value.toFixed(3);
}

export function MetricValueCell({ value, label, format = "float", isBest, delta }: Props) {
    if (value == null) {
        return null;
    }
    const tip = label ? `${label}: ${formatValue(value, format)}` : undefined;
    return (
        <Tooltip title={tip}>
            <span className={`ops-metric-cell${isBest ? " ops-metric-cell-best" : ""}`}>
                {label && <span className="ops-metric-cell-label">{label}</span>}
                <span className="ops-metric-cell-value">{formatValue(value, format)}</span>
                {delta != null && (
                    <span className={`ops-metric-cell-delta${delta >= 0 ? " up" : " down"}`}>
                        {delta >= 0 ? "+" : ""}
                        {delta.toFixed(3)}
                    </span>
                )}
            </span>
        </Tooltip>
    );
}

type StackedProps = {
    title?: string;
    hideTitle?: boolean;
    items: { label: string; value?: number | null; format?: MetricFormat }[];
};

export function StackedMetricCell({ title, hideTitle, items }: StackedProps) {
    const visible = items.filter((i) => i.value != null);
    if (!visible.length) return null;
    return (
        <div className="ops-metric-stacked">
            {!hideTitle && title && <div className="ops-metric-stacked-title">{title}</div>}
            {visible.map((item) => (
                <div key={item.label} className="ops-metric-stacked-row">
                    <span>{item.label}</span>
                    <strong>{formatValue(item.value as number, item.format ?? "float")}</strong>
                </div>
            ))}
        </div>
    );
}
