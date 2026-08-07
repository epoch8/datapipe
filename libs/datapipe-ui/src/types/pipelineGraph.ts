export type GraphRunStep = {
    step_name: string;
    status: string;
};

/** How the pipeline DAG is packed on screen. */
export type GraphFlowLayout = "snake" | "horizontal" | "vertical";

export type PipelineGraphProps = {
    stageFilter?: string | null;
    labelKey?: string | null;
    labelFilter?: string | null;
    labelOrder?: string[];
    /** Nested label value → top-level column key (for columns layout). */
    labelColumnMap?: Record<string, string>;
    layoutMode?: "dag" | "columns";
    runSteps?: GraphRunStep[] | null;
    height?: number | string;
    /** @deprecated Prefer `flowLayout`. Still used when flowLayout is omitted. */
    rankDir?: "TB" | "LR";
    /** snake = wrapped zigzag (default), horizontal = long LR ribbon, vertical = TB. */
    flowLayout?: GraphFlowLayout;
    refreshIntervalMs?: number;
    pipelineId?: string | null;
    graphRefreshToken?: number;
};

export function resolveFlowLayout(
    flowLayout?: GraphFlowLayout | null,
    rankDir?: "TB" | "LR" | null,
): GraphFlowLayout {
    if (flowLayout === "snake" || flowLayout === "horizontal" || flowLayout === "vertical") {
        return flowLayout;
    }
    return rankDir === "TB" ? "vertical" : "snake";
}

export function flowLayoutRankDir(flow: GraphFlowLayout): "TB" | "LR" {
    return flow === "vertical" ? "TB" : "LR";
}

export function flowLayoutWrapRows(flow: GraphFlowLayout): boolean {
    return flow === "snake";
}
