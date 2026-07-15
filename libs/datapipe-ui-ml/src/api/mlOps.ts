import type {
    ClassMetricDetailResponse,
    ClassMetricsParams,
    ClassMetricsResponse,
    FrozenDatasetDetailResponse,
    FrozenDatasetLinkedModelRow,
    FrozenDatasetsResponse,
    MetricsCandidateCreate,
    MetricsEvaluateRequest,
    MetricsEvaluateResponse,
    MetricsListParams,
    MetricsModelDetailResponse,
    MetricsModelRow,
    MetricsRunsResponse,
    MetricsSummaryResponse,
    MetricsTableSchema,
    MetricsTimeseriesResponse,
    OpsImageRecordDetailResponse,
    OpsImageRecordsCountResponse,
    OpsImageRecordsResponse,
    SqlSchemaResponse,
    TrainingCompareResponse,
    TrainingRunsParams,
    TrainingRunsResponse,
} from "../types/opsMl";
import type { OpsOverviewResponse, OpsRowsParams, OpsRowsResponse, OpsSpecDetail, OpsSpecsResponse } from "../types/opsSpecs";
import { fetchJson, toQuery } from "@datapipe/ui/api/ops";

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

export const mlOpsApi = {
    getOpsSpecs: (pipelineId: string) =>
        fetchJson<OpsSpecsResponse>(`/pipelines/${encodeURIComponent(pipelineId)}/ops-specs`),
    getOpsSpec: (pipelineId: string, specId: string) =>
        fetchJson<OpsSpecDetail>(`/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}`),
    getImageRecords: (
        pipelineId: string,
        specId: string,
        params: OpsRowsParams & { include_total?: boolean } = {},
    ) =>
        fetchJson<OpsImageRecordsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/image/records${toQuery(params as Record<string, string | number | string[] | boolean | undefined>)}`,
        ),
    getImageRecordsCount: (pipelineId: string, specId: string, params: OpsRowsParams = {}) =>
        fetchJson<OpsImageRecordsCountResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/image/records/count${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),
    getImageRecordDetail: (pipelineId: string, specId: string, recordKey: string) =>
        fetchJson<OpsImageRecordDetailResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/image/records/${encodeURIComponent(recordKey)}`,
        ),
    getFrozenDatasetRecordRows: (
        pipelineId: string,
        specId: string,
        datasetId: string,
        params: OpsRowsParams & { include_total?: boolean } = {},
    ) =>
        fetchJson<OpsImageRecordsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/frozen-datasets/${encodeURIComponent(datasetId)}/records${toQuery(params as Record<string, string | number | string[] | boolean | undefined>)}`,
        ),
    getFrozenDatasetRecordsCount: (
        pipelineId: string,
        specId: string,
        datasetId: string,
        params: OpsRowsParams = {},
    ) =>
        fetchJson<OpsImageRecordsCountResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/frozen-datasets/${encodeURIComponent(datasetId)}/records/count${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),
    getFrozenDatasetRecordDetail: (pipelineId: string, specId: string, datasetId: string, recordKey: string) =>
        fetchJson<OpsImageRecordDetailResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/frozen-datasets/${encodeURIComponent(datasetId)}/records/${encodeURIComponent(recordKey)}`,
        ),
    getModelPredictionRows: (
        pipelineId: string,
        specId: string,
        modelId: string,
        params: OpsRowsParams & { include_total?: boolean } = {},
    ) =>
        fetchJson<OpsImageRecordsResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/models/${encodeURIComponent(modelId)}/predictions${toQuery(params as Record<string, string | number | string[] | boolean | undefined>)}`,
        ),
    getModelPredictionRecordsCount: (
        pipelineId: string,
        specId: string,
        modelId: string,
        params: OpsRowsParams = {},
    ) =>
        fetchJson<OpsImageRecordsCountResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/models/${encodeURIComponent(modelId)}/predictions/count${toQuery(params as Record<string, string | number | string[] | undefined>)}`,
        ),
    getModelPredictionDetail: (pipelineId: string, specId: string, modelId: string, recordKey: string) =>
        fetchJson<OpsImageRecordDetailResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/models/${encodeURIComponent(modelId)}/predictions/${encodeURIComponent(recordKey)}`,
        ),
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
    getSqlSchema: () => fetchJson<SqlSchemaResponse>("/sql/schema"),
};
