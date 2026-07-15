export type GraphRunStep = {
    step_name: string;
    status: string;
};

export type PipelineGraphProps = {
    stageFilter?: string | null;
    runSteps?: GraphRunStep[] | null;
    height?: number | string;
    rankDir?: "TB" | "LR";
    refreshIntervalMs?: number;
    pipelineId?: string | null;
    graphRefreshToken?: number;
};
