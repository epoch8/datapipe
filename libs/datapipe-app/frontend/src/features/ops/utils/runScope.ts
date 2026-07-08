import type { RunDetail } from "../../../types/ops";

export type RunScopeKind = "full_pipeline" | "stage_run" | "label_run";

const STAGE_TRIGGER_PREFIXES = ["api:stage:", "cli:stage:"] as const;

function stageFromTrigger(trigger?: string): string | null {
    if (!trigger) return null;
    for (const prefix of STAGE_TRIGGER_PREFIXES) {
        if (trigger.startsWith(prefix)) return trigger.slice(prefix.length);
    }
    return null;
}

export function resolveRunScopeDisplay(run: Pick<
    RunDetail,
    "run_scope" | "target_label_display" | "target_labels" | "trigger"
>): { scopeLabel: string; targetLabel: string; highlightLabel: string | null } {
    if (run.run_scope === "full_pipeline" || (!run.target_labels?.length && (run.trigger === "api:pipeline" || run.trigger === "cli:pipeline"))) {
        return { scopeLabel: "full pipeline", targetLabel: "all labels", highlightLabel: null };
    }
    if (run.target_label_display) {
        return {
            scopeLabel: "stage run",
            targetLabel: run.target_label_display,
            highlightLabel: run.target_label_display,
        };
    }
    const triggerStage = stageFromTrigger(run.trigger);
    if (triggerStage) {
        return { scopeLabel: "stage run", targetLabel: triggerStage, highlightLabel: triggerStage };
    }
    const labelStage = run.target_labels?.find(([key]) => key === "stage")?.[1];
    if (labelStage) {
        return { scopeLabel: "stage run", targetLabel: labelStage, highlightLabel: labelStage };
    }
    return { scopeLabel: "full pipeline", targetLabel: "all labels", highlightLabel: null };
}

export function formatRunListStage(row: {
    scope?: string;
    target_label?: string;
    trigger?: string;
}): string {
    if (row.target_label) return row.target_label;
    const stage = stageFromTrigger(row.trigger);
    if (stage) return stage;
    if (row.trigger === "api:pipeline" || row.trigger === "cli:pipeline" || row.scope === "full_pipeline") {
        return "all labels";
    }
    return row.trigger ?? "—";
}

export function formatRunStageColumn(run: Pick<
    RunDetail,
    "run_scope" | "target_label_display" | "target_labels" | "trigger"
>): string {
    const { targetLabel } = resolveRunScopeDisplay(run);
    return targetLabel;
}
