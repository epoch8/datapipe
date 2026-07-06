import React from "react";
import type { MetricFormat } from "../../../types/ops";
import { MetricValue } from "../shared";

type Kpi = {
    key: string;
    label: string;
    value: number | string | null;
    format?: MetricFormat;
};

export function MetricKpiStrip({ items }: { items: Kpi[] }) {
    if (!items.length) return null;
    return (
        <div className="ops-metric-kpi-strip">
            {items.map((item) => (
                <div key={item.key} className="ops-metric-kpi-item">
                    <div className="ops-metric-kpi-label">{item.label}</div>
                    <div className="ops-metric-kpi-value">
                        {typeof item.value === "number" ? (
                            <MetricValue
                                value={item.value}
                                format={item.format === "duration" ? "float" : (item.format ?? "float")}
                            />
                        ) : (
                            item.value ?? ""
                        )}
                    </div>
                </div>
            ))}
        </div>
    );
}
