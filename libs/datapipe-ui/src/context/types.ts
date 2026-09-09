/** Local type bridge until full migration onto @datapipe/api-client exports. */
export type {
    Capabilities,
    PipelineDetail,
    RecentRunSummary,
    ResetTransformMetadataResponse,
    RunDetail,
    RunLogsResponse,
    RunsListParams,
    RunsListResponse,
    SettingsInfo,
    StageRecentRunsResponse,
    AddonCapability,
} from "../types/ops";

export type PipelineApiClient = {
    getCapabilities: () => Promise<import("../types/ops").Capabilities>;
    getSettings: () => Promise<import("../types/ops").SettingsInfo>;
    getPipeline: (opts?: { label_key?: string }) => Promise<import("../types/ops").PipelineDetail>;
    getGraph?: (opts?: { stage?: string | null; label_key?: string | null }) => Promise<unknown>;
    resetTransformMetadata: (
        transformName: string,
    ) => Promise<import("../types/ops").ResetTransformMetadataResponse>;
    getRuns: (
        params?: import("../types/ops").RunsListParams,
    ) => Promise<import("../types/ops").RunsListResponse>;
    getRun: (id: string) => Promise<import("../types/ops").RunDetail>;
    getRunLogs: (
        runId: string,
        after?: number,
        limit?: number,
    ) => Promise<import("../types/ops").RunLogsResponse>;
    startRun: (
        labels?: [string, string][],
        background?: boolean,
    ) => Promise<{ run_id: string; status: string } & Partial<import("../types/ops").RecentRunSummary>>;
    stopRun: (
        runId: string,
    ) => Promise<{ run_id: string; status: string; stopped: boolean }>;
    getStageRecentRuns: (
        pipelineId: string,
        stage: string,
        limit?: number,
    ) => Promise<import("../types/ops").StageRecentRunsResponse>;
    resolveStageRecentRuns: (
        pipelineId: string,
        stage: string,
        limit?: number,
    ) => Promise<import("../types/ops").StageRecentRunsResponse>;
    createWsUrl?: (path: string) => string;
};

export type UiTheme = "light" | "dark";
