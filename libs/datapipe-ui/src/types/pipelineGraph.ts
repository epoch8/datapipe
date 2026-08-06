export type GraphRunStep = {
    step_name: string;
    status: string;
};

export type PipelineGraphProps = {
    stageFilter?: string | null;
    labelKey?: string | null;
    labelFilter?: string | null;
    labelOrder?: string[];
    layoutMode?: "dag" | "columns";
    runSteps?: GraphRunStep[] | null;
    height?: number | string;
    rankDir?: "TB" | "LR";
    refreshIntervalMs?: number;
    pipelineId?: string | null;
    graphRefreshToken?: number;
};
