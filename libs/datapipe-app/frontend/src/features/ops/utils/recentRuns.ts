import type { RecentRunSummary } from "../../../types/ops";

/** Human-readable label scope for a pipeline run trigger. */
export function formatRunTriggerLabel(trigger?: string | null): string | null {
    if (!trigger) return null;
    if (trigger === "api:pipeline") return "all stages";
    if (trigger.startsWith("api:stage:")) {
        return trigger.slice("api:stage:".length) || null;
    }
    return trigger;
}

export function prependRecentRun(
    runs: RecentRunSummary[],
    run: { run_id: string; status: string; trigger?: string },
): RecentRunSummary[] {
    const entry: RecentRunSummary = {
        run_id: run.run_id,
        status: run.status,
        started_at: new Date().toISOString(),
        trigger: run.trigger,
    };
    return [entry, ...runs.filter((item) => item.run_id !== run.run_id)];
}
