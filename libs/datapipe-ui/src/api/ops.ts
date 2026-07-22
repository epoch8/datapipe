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
import { ApiError, apiFetch, readApiErrorBody } from "./http";

const API_BASE = "/api/v1alpha3";

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
    const url = `${API_BASE}${path}`;
    const res = await apiFetch(url, init);
    if (!res.ok) {
        const detail = await readApiErrorBody(res);
        throw new ApiError("http", `API error (${res.status}): ${detail}`, { status: res.status, url });
    }
    return res.json() as Promise<T>;
}

function toQuery(params: Record<string, string | number | string[] | undefined | unknown>): string {
    const q = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
        if (v === undefined || v === "" || v === null) return;
        if (k === "filters") {
            const rules = Array.isArray(v) ? v : [];
            if (rules.length) q.set("filters", JSON.stringify(rules));
            return;
        }
        if (Array.isArray(v)) v.forEach((item) => q.append(k, String(item)));
        else q.set(k, String(v));
    });
    const s = q.toString();
    return s ? `?${s}` : "";
}

async function fallbackStageRecentRuns(
    pipelineId: string,
    stage: string,
): Promise<StageRecentRunsResponse> {
    const detail = await fetchJson<PipelineDetail>(`/pipelines/${encodeURIComponent(pipelineId)}`);
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
    for (const run of detail.recent_runs) {
        const runDetail = await fetchJson<RunDetail>(`/runs/${run.run_id}`);
        if (runDetail.steps.some((step) => stageStepNames.has(step.step_name))) {
            matching.push(run);
        }
    }
    return { pipeline_id: pipelineId, stage, recent_runs: matching };
}

export const coreOpsApi = {
    getCapabilities: () => fetchJson<Capabilities>("/capabilities"),
    getSettings: () => fetchJson<SettingsInfo>("/settings"),
    getPipeline: (id: string) => fetchJson<PipelineDetail>(`/pipelines/${id}`),
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
            const msg = String(e);
            if (!msg.includes("404") && !msg.includes("Not Found")) throw e;
            return fallbackStageRecentRuns(pipelineId, stage);
        }
    },
    getRun: (id: string) => fetchJson<RunDetail>(`/runs/${id}`),
    getRuns: (params: RunsListParams = {}) =>
        fetchJson<RunsListResponse>(
            `/runs${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),
    getRunLogs: (runId: string, after = 0, limit = 200) =>
        fetchJson<RunLogsResponse>(`/runs/${runId}/logs?after=${after}&limit=${limit}`),
    startRun: (labels?: [string, string][], background = true) =>
        fetchJson<{ run_id: string; status: string }>("/runs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ labels: labels || [], background }),
        }),
    resetTransformMetadata: (pipelineId: string, transformName: string) =>
        fetchJson<ResetTransformMetadataResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/transforms/${encodeURIComponent(transformName)}/reset-metadata`,
            { method: "POST" },
        ),
};

export { fetchJson, toQuery };

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
