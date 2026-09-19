import {
    ApiError,
    apiErrorFromResponse,
    apiFetch,
} from "./http";
import type {
    Capabilities,
    GetDataRequest,
    GetDataResponse,
    GetGraphOptions,
    GetPipelineOptions,
    GraphData,
    PipelineDetail,
    RecentRunSummary,
    ResetTransformMetadataResponse,
    RunDetail,
    RunLogsResponse,
    RunsListParams,
    RunsListResponse,
    SettingsInfo,
    StageRecentRunsResponse,
    StartRunResponse,
    StopRunResponse,
} from "./types";

export interface PipelineApiClient {
    getCapabilities(): Promise<Capabilities>;
    getSettings(): Promise<SettingsInfo>;
    getPipeline(opts?: GetPipelineOptions): Promise<PipelineDetail>;
    getGraph(opts?: GetGraphOptions): Promise<GraphData>;
    getTableData(req: GetDataRequest): Promise<GetDataResponse>;
    getTransformData(req: GetDataRequest): Promise<GetDataResponse>;
    getTableSize(tableName: string): Promise<number>;
    getTransformMetaSize(transformName: string): Promise<number>;
    resetTransformMetadata(transformName: string): Promise<ResetTransformMetadataResponse>;
    getRuns(params?: RunsListParams): Promise<RunsListResponse>;
    getRun(id: string): Promise<RunDetail>;
    getRunLogs(runId: string, after?: number, limit?: number): Promise<RunLogsResponse>;
    startRun(
        labels?: [string, string][],
        background?: boolean,
    ): Promise<StartRunResponse & Partial<RecentRunSummary>>;
    stopRun(runId: string): Promise<StopRunResponse>;
    getStageRecentRuns(
        pipelineId: string,
        stage: string,
        limit?: number,
    ): Promise<StageRecentRunsResponse>;
    resolveStageRecentRuns(
        pipelineId: string,
        stage: string,
        limit?: number,
    ): Promise<StageRecentRunsResponse>;
    /** Build a WebSocket URL for a path under the API base (e.g. `/ws/transform/x/run-status`). */
    createWsUrl(path: string): string;
}

export interface LocalPipelineApiClientOptions {
    /** HTTP API prefix. Default: `/api/v1alpha3`. */
    apiBase?: string;
    /**
     * Optional WebSocket URL factory. Receives a path like `/ws/transform/foo/run-status`
     * (leading slash). Default derives `ws(s)://host{apiBase}{path}` from `window.location`.
     */
    createWsUrl?: (path: string) => string;
}

const DEFAULT_API_BASE = "/api/v1alpha3";

function normalizeBase(apiBase: string): string {
    return apiBase.replace(/\/$/, "") || DEFAULT_API_BASE;
}

/** Fill missing capability flags for older servers that only return `addons`. */
function normalizeCapabilities(
    raw: Partial<Capabilities> & { addons?: Capabilities["addons"] },
): Capabilities {
    return {
        graph: raw.graph ?? true,
        table_data: raw.table_data ?? true,
        table_meta: raw.table_meta ?? true,
        transform_meta: raw.transform_meta ?? true,
        run_history: raw.run_history ?? Boolean(raw.run_logs_configured),
        run_start: raw.run_start ?? false,
        run_stop: raw.run_stop ?? false,
        run_logs: raw.run_logs ?? Boolean(raw.run_logs_configured),
        transform_run: raw.transform_run ?? true,
        transform_reset: raw.transform_reset ?? true,
        addons: raw.addons ?? [],
        run_logs_configured: raw.run_logs_configured,
        ml_metrics: raw.ml_metrics,
        ml_training: raw.ml_training,
        pipeline_id: raw.pipeline_id,
    };
}

function toQuery(params: Record<string, string | number | string[] | undefined | unknown>): string {
    const q = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
        if (v === undefined || v === "" || v === null) return;
        if (Array.isArray(v)) v.forEach((item) => q.append(k, String(item)));
        else q.set(k, String(v));
    });
    const s = q.toString();
    return s ? `?${s}` : "";
}

function defaultCreateWsUrl(apiBase: string, path: string): string {
    const normalizedPath = path.startsWith("/") ? path : `/${path}`;
    if (typeof window === "undefined" || !window.location) {
        return `ws://localhost${apiBase}${normalizedPath}`;
    }
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${window.location.host}${apiBase}${normalizedPath}`;
}

/**
 * HTTP client against a local (or same-origin) Ops API under `apiBase`.
 */
export function createLocalPipelineApiClient(
    options: LocalPipelineApiClientOptions = {},
): PipelineApiClient {
    const apiBase = normalizeBase(options.apiBase ?? DEFAULT_API_BASE);
    const createWsUrl =
        options.createWsUrl ?? ((path: string) => defaultCreateWsUrl(apiBase, path));

    async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
        const url = `${apiBase}${path.startsWith("/") ? path : `/${path}`}`;
        const res = await apiFetch(url, init);
        if (!res.ok) {
            throw await apiErrorFromResponse(res, url);
        }
        if (res.status === 204 || res.headers?.get?.("content-length") === "0") {
            return undefined as unknown as T;
        }
        return res.json() as Promise<T>;
    }

    async function fallbackStageRecentRuns(
        pipelineId: string,
        stage: string,
    ): Promise<StageRecentRunsResponse> {
        const detail = await fetchJson<PipelineDetail>("/pipeline");
        const stageInfo = detail.stages.find((s) => s.stage === stage);
        if (!stageInfo) {
            return { pipeline_id: pipelineId, stage, recent_runs: [] };
        }
        const stageStepNames = new Set(
            (stageInfo.steps as { name?: string }[])
                .map((step) => step.name)
                .filter((name): name is string => Boolean(name)),
        );
        if (!stageStepNames.size) {
            return { pipeline_id: pipelineId, stage, recent_runs: [] };
        }
        const matching: RecentRunSummary[] = [];
        for (const run of detail.recent_runs ?? []) {
            const runDetail = await fetchJson<RunDetail>(`/runs/${encodeURIComponent(run.run_id)}`);
            if (runDetail.steps.some((step) => stageStepNames.has(step.step_name))) {
                matching.push(run);
            }
        }
        return { pipeline_id: pipelineId, stage, recent_runs: matching };
    }

    return {
        getCapabilities: async () =>
            normalizeCapabilities(await fetchJson<Partial<Capabilities>>("/capabilities")),
        getSettings: () => fetchJson<SettingsInfo>("/settings"),
        getPipeline: (opts?: GetPipelineOptions) => {
            const params = new URLSearchParams();
            if (opts?.label_key) params.set("label_key", opts.label_key);
            const query = params.toString();
            return fetchJson<PipelineDetail>(query ? `/pipeline?${query}` : "/pipeline");
        },
        getGraph: (opts?: GetGraphOptions) => {
            const params = new URLSearchParams();
            if (opts?.stage) params.set("stage", opts.stage);
            if (opts?.label_key && opts.label_key !== "stage") {
                params.set("label_key", opts.label_key);
            }
            const query = params.toString();
            return fetchJson<GraphData>(query ? `/graph?${query}` : "/graph");
        },
        getTableData: (req: GetDataRequest) =>
            fetchJson<GetDataResponse>("/get-table-data", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(req),
            }),
        getTransformData: (req: GetDataRequest) =>
            fetchJson<GetDataResponse>("/get-transform-data", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(req),
            }),
        getTableSize: async (tableName: string) => {
            const data = await fetchJson<{ size: number }>(
                `/tables/${encodeURIComponent(tableName)}/size`,
            );
            return data.size;
        },
        getTransformMetaSize: async (transformName: string) => {
            const data = await fetchJson<{ size: number }>(
                `/transforms/${encodeURIComponent(transformName)}/meta-size`,
            );
            return data.size;
        },
        resetTransformMetadata: (transformName: string) =>
            fetchJson<ResetTransformMetadataResponse>(
                `/transforms/${encodeURIComponent(transformName)}/reset-metadata`,
                { method: "POST" },
            ),
        getRuns: (params: RunsListParams = {}) =>
            fetchJson<RunsListResponse>(
                `/runs${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
            ),
        getRun: (id: string) => fetchJson<RunDetail>(`/runs/${encodeURIComponent(id)}`),
        getRunLogs: (runId: string, after = 0, limit = 200) =>
            fetchJson<RunLogsResponse>(
                `/runs/${encodeURIComponent(runId)}/logs?after=${after}&limit=${limit}`,
            ),
        startRun: (labels?: [string, string][], background = true) =>
            fetchJson<StartRunResponse & Partial<RecentRunSummary>>("/runs", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ labels: labels || [], background }),
            }),
        stopRun: (runId: string) =>
            fetchJson<StopRunResponse>(`/runs/${encodeURIComponent(runId)}/stop`, {
                method: "POST",
            }),
        getStageRecentRuns: (pipelineId: string, stage: string, limit = 10) =>
            fetchJson<StageRecentRunsResponse>(
                `/pipelines/${encodeURIComponent(pipelineId)}/stages/${encodeURIComponent(stage)}/recent-runs?limit=${limit}`,
            ),
        resolveStageRecentRuns: async (pipelineId: string, stage: string, limit = 10) => {
            try {
                return await fetchJson<StageRecentRunsResponse>(
                    `/pipelines/${encodeURIComponent(pipelineId)}/stages/${encodeURIComponent(stage)}/recent-runs?limit=${limit}`,
                );
            } catch (e) {
                if (e instanceof ApiError && e.status === 404) {
                    return fallbackStageRecentRuns(pipelineId, stage);
                }
                const msg = String(e);
                if (!msg.includes("404") && !msg.includes("Not Found")) throw e;
                return fallbackStageRecentRuns(pipelineId, stage);
            }
        },
        createWsUrl,
    };
}
