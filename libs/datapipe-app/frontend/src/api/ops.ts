import type {
    Capabilities,
    ChartSpec,
    OverviewResponse,
    PipelineDetail,
    RecentRunSummary,
    RunDetail,
    RunLogsResponse,
    SettingsInfo,
    StageRecentRunsResponse,
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

export const opsApi = {
    getOverview: () => fetchJson<OverviewResponse>("/overview"),
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
            if (!msg.includes("404") && !msg.includes("Not Found")) {
                throw e;
            }
            return fallbackStageRecentRuns(pipelineId, stage);
        }
    },
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
