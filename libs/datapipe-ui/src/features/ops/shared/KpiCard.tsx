import React from "react";
import { MetricValue } from "./MetricValue";
import { Sparkline } from "./Sparkline";
import { TrendDelta } from "./TrendDelta";

type Props = {
    label: string;
    value: number | string | null | undefined;
    format?: "float" | "percent" | "integer" | "string";
    delta?: number | null;
    deltaPct?: number | null;
    higherIsBetter?: boolean;
    trend?: { x: string; y: number | null }[];
    icon?: React.ReactNode;
    subtitle?: string;
};

export function KpiCard({
    label,
    value,
    format = "float",
    delta,
    deltaPct,
    higherIsBetter = true,
    trend,
    icon,
    subtitle,
}: Props) {
    return (
        <div className="ops-kpi-card">
            <div className="ops-kpi-header">
                <span className="ops-kpi-label">{label}</span>
                {icon}
            </div>
            <div className="ops-kpi-body">
                <div className="ops-kpi-value-row">
                    {format === "string" ? (
                        <div className="ops-kpi-string-value" title={value != null ? String(value) : undefined}>
                            {value ?? "—"}
                        </div>
                    ) : (
                        <MetricValue value={value} format={format} />
                    )}
                    {(delta != null || deltaPct != null) && (
                        <TrendDelta delta={delta} deltaPct={deltaPct} higherIsBetter={higherIsBetter} />
                    )}
                </div>
                {subtitle && <div className="ops-kpi-subtitle">{subtitle}</div>}
                {trend && trend.length > 0 && <Sparkline data={trend} />}
            </div>
        </div>
    );
}
