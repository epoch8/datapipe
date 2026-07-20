import type {
    ClassMetricDetailResponse,
    ClassMetricsParams,
    ClassMetricsResponse,
    CreateTrainingExperimentPayload,
    CreateTrainingRequestPayload,
    CreateTrainingRequestResponse,
    DuplicateTrainingExperimentPayload,
    FreezeLaunchResponse,
    FrozenDatasetDetailResponse,
    FrozenDatasetLinkedModelRow,
    FrozenDatasetsResponse,
    LaunchResponse,
    MetricsListParams,
    MetricsModelDetailResponse,
    MetricsModelRow,
    MetricsRunsResponse,
    MetricsSummaryResponse,
    OpsImageRecordDetailResponse,
    OpsImageRecordsCountResponse,
    OpsImageRecordsResponse,
    TrainConfigSchemaResponse,
    TrainingExperimentDetailResponse,
    TrainingExperimentModelsResponse,
    TrainingExperimentRow,
    TrainingExperimentsListParams,
    TrainingExperimentsResponse,
    UpdateTrainingExperimentPayload,
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

function trainingBase(pipelineId: string, specId: string): string {
    return `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/training`;
}

type ExperimentDetailPayload = TrainingExperimentDetailResponse | TrainingExperimentRow;

function normalizeExperimentDetail(payload: ExperimentDetailPayload): TrainingExperimentRow {
    if (payload && typeof payload === "object" && "experiment" in payload) {
        return (payload as TrainingExperimentDetailResponse).experiment;
    }
    return payload as TrainingExperimentRow;
}

type ConfigSchemaPayload = {
    config_type: string;
    schema?: Record<string, unknown>;
    json_schema?: Record<string, unknown>;
};

function normalizeConfigSchema(payload: ConfigSchemaPayload): TrainConfigSchemaResponse {
    return {
        config_type: payload.config_type,
        schema: payload.schema ?? payload.json_schema ?? {},
    };
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
    launchFrozenDataset: (pipelineId: string, specId: string) =>
        fetchJson<FreezeLaunchResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/ops-specs/${encodeURIComponent(specId)}/frozen-datasets/launch`,
            { method: "POST" },
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
    resolveMetricsEntity: (
        pipelineId: string,
        params: { model_id?: string; dataset_id?: string },
    ) =>
        fetchJson<{
            kind: "model" | "dataset";
            spec_id: string;
            model_id?: string | null;
            dataset_id?: string | null;
        }>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/resolve-entity${toQuery(params)}`,
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
    getMetricsSummary: (pipelineId: string, params: { subset?: string; model_id?: string; primary_metric?: string } = {}) =>
        fetchJson<MetricsSummaryResponse>(
            `/pipelines/${encodeURIComponent(pipelineId)}/metrics/summary${toQuery(params)}`,
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

    /* --- Custom training experiments (spec §23) --- */

    getTrainingExperiments: (
        pipelineId: string,
        specId: string,
        params: TrainingExperimentsListParams = {},
    ) =>
        fetchJson<TrainingExperimentsResponse>(
            `${trainingBase(pipelineId, specId)}/experiments${toQuery(params as Record<string, string | number | boolean | string[] | undefined>)}`,
        ),
    getTrainingExperiment: (pipelineId: string, specId: string, id: string) =>
        fetchJson<ExperimentDetailPayload>(
            `${trainingBase(pipelineId, specId)}/experiments/${encodeURIComponent(id)}`,
        ).then(normalizeExperimentDetail),
    getTrainingExperimentDetail: (pipelineId: string, specId: string, id: string) =>
        fetchJson<TrainingExperimentDetailResponse>(
            `${trainingBase(pipelineId, specId)}/experiments/${encodeURIComponent(id)}`,
        ),
    getTrainingExperimentModels: (pipelineId: string, specId: string, id: string) =>
        fetchJson<TrainingExperimentModelsResponse>(
            `${trainingBase(pipelineId, specId)}/experiments/${encodeURIComponent(id)}/models`,
        ),
    createTrainingExperiment: (
        pipelineId: string,
        specId: string,
        payload: CreateTrainingExperimentPayload,
    ) =>
        fetchJson<ExperimentDetailPayload>(`${trainingBase(pipelineId, specId)}/experiments`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        }).then(normalizeExperimentDetail),
    updateTrainingExperiment: (
        pipelineId: string,
        specId: string,
        id: string,
        payload: UpdateTrainingExperimentPayload,
    ) =>
        fetchJson<ExperimentDetailPayload>(
            `${trainingBase(pipelineId, specId)}/experiments/${encodeURIComponent(id)}`,
            {
                method: "PATCH",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            },
        ).then(normalizeExperimentDetail),
    deleteTrainingExperiment: (pipelineId: string, specId: string, id: string) =>
        fetchJson<void>(
            `${trainingBase(pipelineId, specId)}/experiments/${encodeURIComponent(id)}`,
            { method: "DELETE" },
        ),
    duplicateTrainingExperiment: (
        pipelineId: string,
        specId: string,
        id: string,
        payload: DuplicateTrainingExperimentPayload = {},
    ) =>
        fetchJson<ExperimentDetailPayload>(
            `${trainingBase(pipelineId, specId)}/experiments/${encodeURIComponent(id)}/duplicate`,
            {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            },
        ).then(normalizeExperimentDetail),
    archiveTrainingExperiment: (pipelineId: string, specId: string, id: string) =>
        fetchJson<ExperimentDetailPayload>(
            `${trainingBase(pipelineId, specId)}/experiments/${encodeURIComponent(id)}/archive`,
            { method: "POST" },
        ).then(normalizeExperimentDetail),
    unarchiveTrainingExperiment: (pipelineId: string, specId: string, id: string) =>
        fetchJson<ExperimentDetailPayload>(
            `${trainingBase(pipelineId, specId)}/experiments/${encodeURIComponent(id)}/unarchive`,
            { method: "POST" },
        ).then(normalizeExperimentDetail),
    getTrainConfigSchema: (pipelineId: string, specId: string, configType?: string) =>
        fetchJson<ConfigSchemaPayload>(
            `${trainingBase(pipelineId, specId)}/config-schema${toQuery({ config_type: configType })}`,
        ).then(normalizeConfigSchema),
    createTrainingRequest: (
        pipelineId: string,
        specId: string,
        payload: CreateTrainingRequestPayload,
    ) =>
        fetchJson<CreateTrainingRequestResponse>(`${trainingBase(pipelineId, specId)}/requests`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        }),
    launchTrainingRequest: (pipelineId: string, specId: string, requestId: string) =>
        fetchJson<LaunchResponse>(
            `${trainingBase(pipelineId, specId)}/requests/${encodeURIComponent(requestId)}/launch`,
            { method: "POST" },
        ),
};
