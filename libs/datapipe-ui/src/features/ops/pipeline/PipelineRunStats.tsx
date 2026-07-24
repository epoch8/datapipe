import React from "react";
import type { RecentRunSummary, RunListRow } from "../../../types/ops";
import { KpiCard } from "../shared/KpiCard";

type Props = {
    runs: RecentRunSummary[] | RunListRow[];
    countsByStatus?: Record<string, number>;
    total?: number;
};

function failedLast7d(runs: Props["runs"]): number {
    const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
    return runs.filter((run) => {
        if (run.status !== "failed" || !run.started_at) return false;
        return new Date(run.started_at).getTime() >= cutoff;
    }).length;
}

export function PipelineRunStats({ runs, countsByStatus, total }: Props) {
    const completed = countsByStatus?.completed ?? runs.filter((r) => r.status === "completed").length;
    const running = countsByStatus?.running ?? runs.filter((r) => r.status === "running").length;
    const failed7d = failedLast7d(runs);
    const runTotal = total ?? runs.length;
    const successRate = runTotal > 0 ? completed / runTotal : null;

    return (
        <div className="ops-kpi-row">
            <KpiCard label="Total runs" value={runTotal} format="integer" />
            <KpiCard
                label="Success rate"
                value={successRate}
                format="percent"
                subtitle={completed ? `${completed} completed` : undefined}
            />
            <KpiCard label="Running now" value={running} format="integer" />
            <KpiCard label="Failed last 7d" value={failed7d} format="integer" higherIsBetter={false} />
        </div>
    );
}
