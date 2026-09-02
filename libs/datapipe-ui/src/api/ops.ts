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

const API_BASE = "/api/v1alpha3";

type ErrorEnvelope = {
    error?: { code?: unknown; message?: unknown; details?: unknown };
    detail?: unknown;
    message?: unknown;
};

/**
 * Parse a non-2xx response body into an {@link ApiError}. Prefers the normative
 * envelope `{error:{code,message,details}}` used by ops endpoints and
 * falls back to FastAPI's `{detail}` / `{message}` shapes.
 *
 * Callers must branch on `ApiError.code` (never `message.includes(...)`).
 */
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

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
    const url = `${API_BASE}${path}`;
    const res = await apiFetch(url, init);
    if (!res.ok) {
        throw await apiErrorFromResponse(res, url);
    }
    if (res.status === 204 || res.headers?.get?.("content-length") === "0") {
        return undefined as unknown as T;
    }
    return res.json() as Promise<T>;
}

async function notAvailable<T = never>(feature: string): Promise<T> {
    throw new ApiError("http", `${feature} is not available on cloud Ops API v1alpha3`, {
        status: 501,
        url: `${API_BASE}/${feature}`,
        code: "not_available",
    });
}

export const coreOpsApi = {
    getCapabilities: () => fetchJson<Capabilities>("/capabilities"),
    getSettings: () => fetchJson<SettingsInfo>("/settings"),
    getPipeline: (opts?: { label_key?: string }): Promise<PipelineDetail> => {
        const params = new URLSearchParams();
        if (opts?.label_key) params.set("label_key", opts.label_key);
        const query = params.toString();
        return fetchJson<PipelineDetail>(query ? `/pipeline?${query}` : "/pipeline");
    },
    resetTransformMetadata: (transformName: string) =>
        fetchJson<ResetTransformMetadataResponse>(
            `/transforms/${encodeURIComponent(transformName)}/reset-metadata`,
            { method: "POST" },
        ),
    // Observability endpoints exist in full Ops UI builds only; stubs keep unused pages compiling.
    getStageRecentRuns: (
        _pipelineId: string,
        _stage: string,
        _limit = 10,
    ): Promise<StageRecentRunsResponse> => notAvailable("stage-recent-runs"),
    resolveStageRecentRuns: (
        _pipelineId: string,
        _stage: string,
        _limit = 10,
    ): Promise<StageRecentRunsResponse> => notAvailable("stage-recent-runs"),
    getRun: (_id: string): Promise<RunDetail> => notAvailable("runs"),
    getRuns: (_params: RunsListParams = {}): Promise<RunsListResponse> => notAvailable("runs"),
    getRunLogs: (_runId: string, _after = 0, _limit = 200): Promise<RunLogsResponse> =>
        notAvailable("run-logs"),
    startRun: (
        _labels?: [string, string][],
        _background = true,
    ): Promise<{ run_id: string; status: string } & Partial<RecentRunSummary>> =>
        notAvailable("runs"),
    stopRun: (_runId: string): Promise<{ run_id: string; status: string; stopped: boolean }> =>
        notAvailable("runs"),
};

export { fetchJson, ApiError };

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
