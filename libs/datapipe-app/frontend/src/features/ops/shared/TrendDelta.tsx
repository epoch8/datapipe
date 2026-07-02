import React from "react";
import { ArrowDownOutlined, ArrowUpOutlined } from "@ant-design/icons";
import { formatDelta, formatDeltaPct } from "./metricsFormat";

type Props = {
    delta?: number | null;
    deltaPct?: number | null;
    higherIsBetter?: boolean;
    showAbsolute?: boolean;
};

export function TrendDelta({
    delta,
    deltaPct,
    higherIsBetter = true,
    showAbsolute = true,
}: Props) {
    const abs = formatDelta(delta, higherIsBetter);
    const pct = formatDeltaPct(deltaPct, higherIsBetter);
    const positive = deltaPct != null ? pct.positive : abs.positive;
    const cls = positive ? "ops-delta-positive" : "ops-delta-negative";

    if (delta == null && deltaPct == null) {
        return <span className="ops-delta-neutral">—</span>;
    }

    return (
        <span className={`ops-trend-delta ${cls}`}>
            {positive ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
            {showAbsolute && delta != null && <span>{abs.text}</span>}
            {deltaPct != null && <span className="ops-delta-pct">{pct.text}</span>}
        </span>
    );
}
