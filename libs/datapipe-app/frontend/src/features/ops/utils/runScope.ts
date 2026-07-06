import type { RunDetail } from "../../../types/ops";

export type RunScopeKind = "full_pipeline" | "stage_run" | "label_run";

export function resolveRunScopeDisplay(run: Pick<
    RunDetail,
    "run_scope" | "target_label_display" | "target_labels" | "trigger"
>): { scopeLabel: string; targetLabel: string; highlightLabel: string | null } {
    if (run.run_scope === "full_pipeline" || (!run.target_labels?.length && run.trigger === "api:pipeline")) {
        return { scopeLabel: "full pipeline", targetLabel: "all labels", highlightLabel: null };
    }
    if (run.target_label_display) {
        return {
            scopeLabel: "stage run",
            targetLabel: run.target_label_display,
            highlightLabel: run.target_label_display,
        };
    }
    if (run.trigger?.startsWith("api:stage:")) {
        const stage = run.trigger.slice("api:stage:".length);
        return { scopeLabel: "stage run", targetLabel: stage, highlightLabel: stage };
    }
    const stage = run.target_labels?.find(([key]) => key === "stage")?.[1];
    if (stage) {
        return { scopeLabel: "stage run", targetLabel: stage, highlightLabel: stage };
    }
    return { scopeLabel: "full pipeline", targetLabel: "all labels", highlightLabel: null };
}

export function formatRunStageColumn(run: Pick<
    RunDetail,
    "run_scope" | "target_label_display" | "target_labels" | "trigger"
>): string {
    const { targetLabel } = resolveRunScopeDisplay(run);
    return targetLabel;
}
