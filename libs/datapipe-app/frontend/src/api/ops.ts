import type {
    Capabilities,
    ChartSpec,
    OverviewResponse,
    PipelineDetail,
    RunDetail,
    RunLogsResponse,
    SettingsInfo,
} from "../types/ops";

const API_BASE = "/api/v1alpha3";

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
    const res = await fetch(`${API_BASE}${path}`, init);
    if (!res.ok) {
        const text = await res.text();
        throw new Error(text || res.statusText);
    }
    return res.json() as Promise<T>;
}

export const opsApi = {
    getOverview: () => fetchJson<OverviewResponse>("/overview"),
    getCapabilities: () => fetchJson<Capabilities>("/capabilities"),
    getSettings: () => fetchJson<SettingsInfo>("/settings"),
    getPipeline: (id: string) => fetchJson<PipelineDetail>(`/pipelines/${id}`),
    getRun: (id: string) => fetchJson<RunDetail>(`/runs/${id}`),
    getRunLogs: (runId: string, after = 0, limit = 500) =>
        fetchJson<RunLogsResponse>(`/runs/${runId}/logs?after=${after}&limit=${limit}`),
    getMetricsCharts: (pipelineId: string, modelId?: string) => {
        const params = new URLSearchParams({ pipeline_id: pipelineId });
        if (modelId) params.set("model_id", modelId);
        return fetchJson<{ charts: ChartSpec[] }>(`/metrics/charts?${params}`);
    },
    getMetricsSummary: (pipelineId: string) =>
        fetchJson<Record<string, unknown>>(`/metrics/summary?pipeline_id=${pipelineId}`),
    getTrainingCurves: (runKey: string, limitEpochs?: number) => {
        const params = limitEpochs ? `?limit_epochs=${limitEpochs}` : "";
        return fetchJson<{ charts: ChartSpec[] }>(`/training/${encodeURIComponent(runKey)}/curves${params}`);
    },
    getTrainingRun: (runKey: string) =>
        fetchJson<Record<string, unknown>>(`/training/${encodeURIComponent(runKey)}`),
    getTrainingRuns: (pipelineId: string) =>
        fetchJson<{ runs: Record<string, unknown>[] }>(`/pipelines/${pipelineId}/training/runs`),
    startRun: (labels?: [string, string][], background = false) =>
        fetchJson<{ run_id: string; status: string }>("/runs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ labels: labels || [], background }),
        }),
};

export function getRefreshIntervalMs(): number {
    const stored = localStorage.getItem("datapipe_ops_refresh_s");
    const seconds = stored ? parseInt(stored, 10) : 30;
    return (Number.isFinite(seconds) ? seconds : 30) * 1000;
}
