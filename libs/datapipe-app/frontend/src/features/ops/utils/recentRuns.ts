import type { RecentRunSummary } from "../../../types/ops";

export function prependRecentRun(
    runs: RecentRunSummary[],
    run: { run_id: string; status: string },
): RecentRunSummary[] {
    const entry: RecentRunSummary = {
        run_id: run.run_id,
        status: run.status,
        started_at: new Date().toISOString(),
    };
    return [entry, ...runs.filter((item) => item.run_id !== run.run_id)];
}
