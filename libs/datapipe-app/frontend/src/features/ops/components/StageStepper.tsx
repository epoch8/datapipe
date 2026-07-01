import React from "react";
import { StageFlowDiagram, type StageItem } from "./StageFlowDiagram";

export type { StageItem };

/** First open stage drives icons; later stages stay visually "wait" until then. */
export function stepperCurrentIndex(stages: Pick<StageItem, "status">[]): number {
    const firstOpen = stages.findIndex((s) => s.status === "pending" || s.status === "running");
    if (firstOpen >= 0) return firstOpen;
    const firstFailed = stages.findIndex((s) => s.status === "failed");
    if (firstFailed >= 0) return firstFailed;
    return stages.length;
}

export function StageStepper({
    stages,
    edges,
    onStageSelect,
    onStageRun,
}: {
    stages: StageItem[];
    edges?: { from: string; to: string; count?: number }[];
    onStageSelect?: (stage: string) => void;
    onStageRun?: (stage: string) => void;
}) {
    return (
        <div style={{ marginBottom: 24, overflowX: "auto" }}>
            <StageFlowDiagram
                stages={stages}
                edges={edges}
                onStageSelect={onStageSelect}
                onStageRun={onStageRun}
            />
        </div>
    );
}
