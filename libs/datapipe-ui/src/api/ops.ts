import type {
    PipelineDetail,
    RecentRunSummary,
    ResetTransformMetadataResponse,
    RunDetail,
    RunLogsResponse,
    RunsListParams,
    RunsListResponse,
    SettingsInfo,
    StageRecentRunsResponse,
    Capabilities,
} from "../types/ops";
import { ApiError, apiFetch } from "./http";
import type { PipelineApiClient } from "../context/types";

type ErrorEnvelope = {
    error?: { code?: unknown; message?: unknown; details?: unknown };
    detail?: unknown;
    message?: unknown;
};

async function apiErrorFromResponse(res: Response, url: string): Promise<ApiError> {
    let body: ErrorEnvelope | null = null;
    let rawText = "";
    try {
        rawText = await res.text();
        body = rawText ? (JSON.parse(rawText) as ErrorEnvelope) : null;
    } catch {
        body = null;
    }

    const envelope = body?.error;
    if (envelope && typeof envelope === "object") {
        const code = typeof envelope.code === "string" ? envelope.code : null;
        const message =
            typeof envelope.message === "string" && envelope.message
                ? envelope.message
                : `API error (${res.status})`;
        return new ApiError("http", message, {
            status: res.status,
            url,
            code,
            details: envelope.details,
        });
    }

    let detail: string | null = null;
    if (body) {
        if (typeof body.detail === "string") detail = body.detail;
        else if (Array.isArray(body.detail)) {
            detail = body.detail
                .map((item) => (typeof item === "string" ? item : JSON.stringify(item)))
                .join("; ");
        } else if (typeof body.message === "string") detail = body.message;
    }
    if (detail === null) detail = rawText || res.statusText || `HTTP ${res.status}`;

    return new ApiError("http", `API error (${res.status}): ${detail}`, {
        status: res.status,
        url,
        code: null,
        details: body ?? undefined,
    });
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

function normalizeCapabilities(raw: Partial<Capabilities>): Capabilities {
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

export type CreateOpsApiOptions = {
    apiBase?: string;
    createWsUrl?: (path: string) => string;
};

export function createOpsApi(options: CreateOpsApiOptions = {}): PipelineApiClient {
    const apiBase = (options.apiBase ?? "/api/v1alpha3").replace(/\/$/, "");
    const createWsUrl =
        options.createWsUrl ??
        ((path: string) => {
            if (typeof window === "undefined") return path;
            const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
            const abs = path.startsWith("/") ? path : `${apiBase}/${path}`;
            return `${protocol}//${window.location.host}${abs}`;
        });

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
            try {
                const runDetail = await fetchJson<RunDetail>(`/runs/${run.run_id}`);
                if (runDetail.steps.some((step) => stageStepNames.has(step.step_name))) {
                    matching.push(run);
                }
            } catch {
                // ignore
            }
        }
        return { pipeline_id: pipelineId, stage, recent_runs: matching };
    }

    const api: PipelineApiClient = {
        createWsUrl,
        getCapabilities: async () =>
            normalizeCapabilities(await fetchJson<Partial<Capabilities>>("/capabilities")),
        getSettings: () => fetchJson<SettingsInfo>("/settings"),
        getPipeline: (opts?: { label_key?: string }) =>
            fetchJson<PipelineDetail>(`/pipeline${toQuery({ label_key: opts?.label_key })}`),
        getGraph: (opts) => {
            const params = new URLSearchParams();
            if (opts?.stage) params.set("stage", opts.stage);
            if (opts?.label_key && opts.label_key !== "stage") {
                params.set("label_key", opts.label_key);
            }
            const query = params.toString();
            return fetchJson(query ? `/graph?${query}` : "/graph");
        },
        resetTransformMetadata: (transformName: string) =>
            fetchJson<ResetTransformMetadataResponse>(
                `/transforms/${encodeURIComponent(transformName)}/reset-metadata`,
                { method: "POST" },
            ),
        getStageRecentRuns: (pipelineId: string, stage: string, limit = 10) =>
            fetchJson<StageRecentRunsResponse>(
                `/pipelines/${encodeURIComponent(pipelineId)}/stages/${encodeURIComponent(stage)}/recent-runs?limit=${limit}`,
            ),
        resolveStageRecentRuns: async (pipelineId, stage, limit = 10) => {
            try {
                return await api.getStageRecentRuns(pipelineId, stage, limit);
            } catch (e) {
                const status = e instanceof ApiError ? e.status : undefined;
                const msg = String(e);
                if (status !== 404 && !msg.includes("404") && !msg.includes("Not Found")) {
                    throw e;
                }
                return fallbackStageRecentRuns(pipelineId, stage);
            }
        },
        getRun: (id: string) => fetchJson<RunDetail>(`/runs/${encodeURIComponent(id)}`),
        getRuns: (params: RunsListParams = {}) =>
            fetchJson<RunsListResponse>(
                `/runs${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
            ),
        getRunLogs: (runId: string, after = 0, limit = 200) =>
            fetchJson<RunLogsResponse>(
                `/runs/${encodeURIComponent(runId)}/logs?after=${after}&limit=${limit}`,
            ),
        startRun: (labels?: [string, string][], background = true) =>
            fetchJson<{ run_id: string; status: string } & Partial<RecentRunSummary>>("/runs", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ labels: labels || [], background }),
            }),
        stopRun: (runId: string) =>
            fetchJson<{ run_id: string; status: string; stopped: boolean }>(
                `/runs/${encodeURIComponent(runId)}/stop`,
                { method: "POST" },
            ),
    };

    return api;
}

/** Default local client; hosts may replace via {@link setActiveOpsApi}. */
export let coreOpsApi: PipelineApiClient = createOpsApi();

export function setActiveOpsApi(api: PipelineApiClient): void {
    coreOpsApi = api;
}

export { ApiError, toQuery };

export function getRefreshIntervalMs(): number {
    const stored = localStorage.getItem("datapipe_ops_refresh_s");
    const seconds = stored ? parseInt(stored, 10) : 30;
    return (Number.isFinite(seconds) ? seconds : 30) * 1000;
}

export function exportCsv(columns: string[], rows: Record<string, unknown>[], filename = "export.csv") {
    const header = columns.join(",");
    const body = rows.map((r) => columns.map((c) => JSON.stringify(r[c] ?? "")).join(",")).join("\n");
    const blob = new Blob([`${header}\n${body}`], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
}
