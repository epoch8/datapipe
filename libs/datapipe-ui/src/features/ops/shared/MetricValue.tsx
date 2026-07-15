import React from "react";
import { formatMetric } from "./metricsFormat";

type Props = {
    value: number | string | null | undefined;
    format?: "float" | "percent" | "integer" | "string";
    className?: string;
};

export function MetricValue({ value, format = "float", className }: Props) {
    return <span className={className ?? "ops-metric-value"}>{formatMetric(value, format)}</span>;
}
