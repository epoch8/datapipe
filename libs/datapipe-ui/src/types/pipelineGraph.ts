export type GraphRunStep = {
    step_name: string;
    status: string;
};

/** How the pipeline DAG is packed on screen. */
export type GraphFlowLayout =
    | "snake"
    | "horizontal"
    | "vertical"
    | "horizontal_compact"
    | "vertical_compact";

const FLOW_LAYOUTS: ReadonlySet<string> = new Set([
    "snake",
    "horizontal",
    "vertical",
    "horizontal_compact",
    "vertical_compact",
]);

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
    /**
     * snake = chronology zigzag;
     * horizontal/vertical = chronology flat LR/TB;
     * *_compact = dependency-depth LR/TB (lane prototype).
     */
    flowLayout?: GraphFlowLayout;
    refreshIntervalMs?: number;
    pipelineId?: string | null;
    graphRefreshToken?: number;
};

export function resolveFlowLayout(
    flowLayout?: GraphFlowLayout | null,
    rankDir?: "TB" | "LR" | null,
): GraphFlowLayout {
    if (flowLayout && FLOW_LAYOUTS.has(flowLayout)) {
        return flowLayout;
    }
    return rankDir === "TB" ? "vertical" : "snake";
}

export function flowLayoutRankDir(flow: GraphFlowLayout): "TB" | "LR" {
    return flow === "vertical" || flow === "vertical_compact" ? "TB" : "LR";
}

export function flowLayoutWrapRows(flow: GraphFlowLayout): boolean {
    return flow === "snake";
}

/** Chronology for snake / H / V; dependency depth for compact modes. */
export function flowLayoutPreferChronology(flow: GraphFlowLayout): boolean {
    return flow !== "horizontal_compact" && flow !== "vertical_compact";
}
