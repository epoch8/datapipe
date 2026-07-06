import type {
    Capabilities,
    ChartSpec,
    ClassMetricDetailResponse,
    ClassMetricsParams,
    ClassMetricsResponse,
    MetricsListParams,
    MetricsRunsResponse,
    MetricsSummaryResponse,
    MetricsTimeseriesResponse,
    OverviewResponse,
    PipelineDetail,
    RecentRunSummary,
    ResetTransformMetadataResponse,
    RunDetail,
    RunLogsResponse,
    SettingsInfo,
    SqlQueryRequest,
    SqlQueryResponse,
    SqlSchemaResponse,
    StageRecentRunsResponse,
    TrainingCompareResponse,
    TrainingRunsParams,
    TrainingRunsResponse,
} from "../types/ops";
import { ApiError, apiFetch, readApiErrorBody } from "./http";
import { opsMock } from "./opsMock";

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

async function fetchWithMock<T>(path: string, init: RequestInit | undefined, mockFn: () => T): Promise<T> {
    try {
        return await fetchJson<T>(path, init);
    } catch (e) {
        const msg = String(e);
        if (msg.includes("404") || msg.includes("Not Found") || msg.includes("422")) {
            return mockFn();
        }
        throw e;
    }
}

function toQuery(params: Record<string, string | number | string[] | undefined>): string {
    const q = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
        if (v === undefined || v === "") return;
        if (Array.isArray(v)) v.forEach((item) => q.append(k, item));
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
            if (!msg.includes("404") && !msg.includes("Not Found")) throw e;
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
    getMetricsSummaryLegacy: (pipelineId: string) =>
        fetchJson<Record<string, unknown>>(`/metrics/summary?pipeline_id=${pipelineId}`),

    getMetricsRuns: (pipelineId: string, params: MetricsListParams = {}) =>
        fetchWithMock<MetricsRunsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/runs${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
            undefined,
            () => opsMock.getMetricsRuns(pipelineId, params),
        ),

    getMetricsSummary: (pipelineId: string, params: { subset?: string; model_id?: string; primary_metric?: string } = {}) =>
        fetchWithMock<MetricsSummaryResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/summary${toQuery(params)}`,
            undefined,
            () => opsMock.getMetricsSummary(pipelineId),
        ),

    getMetricsTimeseries: (
        pipelineId: string,
        params: { metrics: string[]; subset?: string[]; group_by?: string; model_id?: string; from?: string; to?: string },
    ) =>
        fetchWithMock<MetricsTimeseriesResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/timeseries${toQuery({ ...params, metrics: params.metrics.join(",") })}`,
            undefined,
            () => opsMock.getMetricsTimeseries(),
        ),

    getClassMetrics: (pipelineId: string, params: ClassMetricsParams = {}) =>
        fetchWithMock<ClassMetricsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/classes${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
            undefined,
            () => opsMock.getClassMetrics(),
        ),

    getClassDetail: (pipelineId: string, label: string, params: { run_id?: string; subset?: string } = {}) =>
        fetchWithMock<ClassMetricDetailResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/classes/${encodeURIComponent(label)}${toQuery(params)}`,
            undefined,
            () => opsMock.getClassDetail(label),
        ),

    getTrainingCurves: (runKey: string, limitEpochs?: number) => {
        const params = limitEpochs ? `?limit_epochs=${limitEpochs}` : "";
        return fetchJson<{ charts: ChartSpec[] }>(`/training/${encodeURIComponent(runKey)}/curves${params}`);
    },
    getTrainingRun: (runKey: string) =>
        fetchJson<Record<string, unknown>>(`/training/${encodeURIComponent(runKey)}`),

    getTrainingRuns: (pipelineId: string, params: TrainingRunsParams = {}) =>
        fetchWithMock<TrainingRunsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/training/runs${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
            undefined,
            () => opsMock.getTrainingRuns(),
        ),

    compareTraining: (runKeys: string[], params: { metrics?: string[]; smoothing?: number; pipeline_id?: string } = {}) =>
        fetchWithMock<TrainingCompareResponse>(
            `/training/compare${toQuery({ run_keys: runKeys.join(","), ...params, metrics: params.metrics?.join(",") })}`,
            undefined,
            () => opsMock.compareTraining(runKeys),
        ),

    runSqlQuery: (req: SqlQueryRequest) =>
        fetchWithMock<SqlQueryResponse>(
            "/sql/query",
            { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(req) },
            () => opsMock.runSqlQuery(),
        ),

    getSqlSchema: () =>
        fetchWithMock<SqlSchemaResponse>("/sql/schema", undefined, () => opsMock.getSqlSchema()),

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
