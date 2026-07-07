import type {
    Capabilities,
    ChartSpec,
    ClassMetricDetailResponse,
    ClassMetricsParams,
    ClassMetricsResponse,
    MetricsCandidateCreate,
    MetricsEvaluateRequest,
    MetricsEvaluateResponse,
    MetricsListParams,
    MetricsRunsResponse,
    MetricsSummaryResponse,
    MetricsTableSchema,
    MetricsTimeseriesResponse,
    FrozenDatasetDetailResponse,
    FrozenDatasetLinkedModelRow,
    FrozenDatasetsResponse,
    MetricsModelRow,
    MetricsModelDetailResponse,
    OverviewResponse,
    PipelineDetail,
    RecentRunSummary,
    ResetTransformMetadataResponse,
    RunDetail,
    RunLogsResponse,
    RunsListParams,
    RunsListResponse,
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
import type { OpsOverviewResponse, OpsRowsParams, OpsRowsResponse, OpsSpecDetail, OpsSpecsResponse } from "../types/opsSpecs";

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

type FrozenDatasetDetailPayload = FrozenDatasetDetailResponse & {
    models?: MetricsModelRow[];
};

function normalizeFrozenDatasetDetail(payload: FrozenDatasetDetailPayload): FrozenDatasetDetailResponse {
    if (payload.linked_models?.length) {
        return payload;
    }
    const legacyModels = payload.models;
    if (!legacyModels?.length) {
        return { ...payload, linked_models: payload.linked_models ?? [] };
    }
    const byModel = new Map<string, FrozenDatasetLinkedModelRow>();
    for (const row of legacyModels) {
        if (!row.model_id || byModel.has(row.model_id)) continue;
        byModel.set(row.model_id, {
            model_id: row.model_id,
            created_at: row.started_at ?? null,
            run_key: row.run_key ?? null,
            run_id: row.run_id ?? null,
        });
    }
    return { ...payload, linked_models: Array.from(byModel.values()) };
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
    getOpsSpecs: (pipelineId: string) =>
        fetchJson<OpsSpecsResponse>(`/pipelines/${encodeURIComponent(pipelineId)}/ops-specs`),
    getOpsSpec: (pipelineId: string, specId: string) =>
        fetchJson<OpsSpecDetail>(`/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}`),
    getOpsPageOverview: (pipelineId: string, page: "frozen-datasets" | "training" | "metrics" | "class-metrics") =>
        fetchJson<OpsOverviewResponse>(`/pipelines/${encodeURIComponent(pipelineId)}/ops-pages/${page}/overview`),
    getOpsFrozenRows: (pipelineId: string, specId: string, params: OpsRowsParams = {}) =>
        fetchJson<OpsRowsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/frozen-datasets${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),
    getOpsTrainingRows: (pipelineId: string, specId: string, params: OpsRowsParams = {}) =>
        fetchJson<OpsRowsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/training/runs${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),
    getOpsMetricRows: (pipelineId: string, specId: string, tableId: string, params: OpsRowsParams = {}) =>
        fetchJson<OpsRowsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/metrics/${encodeURIComponent(tableId)}/rows${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),
    getOpsClassMetricRows: (pipelineId: string, specId: string, tableId: string, params: OpsRowsParams = {}) =>
        fetchJson<OpsRowsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/class-metrics/${encodeURIComponent(tableId)}/rows${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),
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
    getRuns: (params: RunsListParams = {}) =>
        fetchJson<RunsListResponse>(
            `/runs${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),
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
        fetchJson<MetricsRunsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/runs${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),

    getFrozenDatasets: (pipelineId: string) =>
        fetchJson<FrozenDatasetsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/frozen-datasets`,
        ),

    getModelDetail: (
        pipelineId: string,
        modelId: string,
        params?: { dataset_id?: string; subset?: string },
    ) =>
        fetchJson<MetricsModelDetailResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/models/${encodeURIComponent(modelId)}${toQuery(params ?? {})}`,
        ),

    getFrozenDatasetDetail: (
        pipelineId: string,
        datasetId: string,
        params?: { subset?: string },
    ) =>
        fetchJson<FrozenDatasetDetailPayload>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/frozen-datasets/${encodeURIComponent(datasetId)}${toQuery(params ?? {})}`,
        ).then(normalizeFrozenDatasetDetail),

    getMetricsSchema: (pipelineId: string, taskType?: string) =>
        fetchJson<MetricsTableSchema>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/schema${toQuery({ task_type: taskType })}`,
        ),

    addMetricsCandidate: (pipelineId: string, body: MetricsCandidateCreate) =>
        fetchJson<{ id: string }>(`/pipelines/${encodeURIComponent(pipelineId)}/metrics/candidates`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        }),

    deleteMetricsCandidate: (pipelineId: string, candidateId: string) =>
        fetchJson<{ status: string }>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/candidates/${encodeURIComponent(candidateId)}`,
            { method: "DELETE" },
        ),

    evaluateMetrics: (pipelineId: string, body: MetricsEvaluateRequest = {}) =>
        fetchJson<MetricsEvaluateResponse>(`/pipelines/${encodeURIComponent(pipelineId)}/metrics/evaluate`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                candidate_ids: body.candidate_ids ?? [],
                labels: body.labels ?? [["stage", "count-metrics"]],
                background: body.background ?? true,
            }),
        }),

    getMetricsSummary: (pipelineId: string, params: { subset?: string; model_id?: string; primary_metric?: string } = {}) =>
        fetchJson<MetricsSummaryResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/summary${toQuery(params)}`,
        ),

    getMetricsTimeseries: (
        pipelineId: string,
        params: { metrics: string[]; subset?: string[]; group_by?: string; model_id?: string; from?: string; to?: string },
    ) =>
        fetchJson<MetricsTimeseriesResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/timeseries${toQuery({ ...params, metrics: params.metrics.join(",") })}`,
        ),

    getClassMetrics: (pipelineId: string, params: ClassMetricsParams = {}) =>
        fetchJson<ClassMetricsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/classes${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),

    getClassDetail: (pipelineId: string, label: string, params: { run_id?: string; subset?: string } = {}) =>
        fetchJson<ClassMetricDetailResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/classes/${encodeURIComponent(label)}${toQuery(params)}`,
        ),

    getTrainingCurves: (runKey: string, limitEpochs?: number) => {
        const params = limitEpochs ? `?limit_epochs=${limitEpochs}` : "";
        return fetchJson<{ charts: ChartSpec[] }>(`/training/${encodeURIComponent(runKey)}/curves${params}`);
    },
    getTrainingRun: (runKey: string) =>
        fetchJson<Record<string, unknown>>(`/training/${encodeURIComponent(runKey)}`),

    getTrainingRuns: (pipelineId: string, params: TrainingRunsParams = {}) =>
        fetchJson<TrainingRunsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/training/runs${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),

    compareTraining: (runKeys: string[], params: { metrics?: string[]; smoothing?: number; pipeline_id?: string } = {}) =>
        fetchJson<TrainingCompareResponse>(
            `/training/compare${toQuery({ run_keys: runKeys.join(","), ...params, metrics: params.metrics?.join(",") })}`,
        ),

    runSqlQuery: (req: SqlQueryRequest) =>
        fetchJson<SqlQueryResponse>("/sql/query", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(req),
        }),

    getSqlSchema: () => fetchJson<SqlSchemaResponse>("/sql/schema"),

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
