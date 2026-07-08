type ModelUrlParams = { dataset_id?: string; subset?: string; specId?: string };
type DatasetUrlParams = { subset?: string; specId?: string };

export function buildModelUrl(modelId: string, pipelineId?: string, params?: ModelUrlParams): string {
    const path = params?.specId
        ? `/metrics/${encodeURIComponent(params.specId)}/models/${encodeURIComponent(modelId)}`
        : pipelineId
          ? `/pipelines/${encodeURIComponent(pipelineId)}/metrics/models/${encodeURIComponent(modelId)}`
          : `/metrics/models/${encodeURIComponent(modelId)}`;
    const q = new URLSearchParams();
    if (params?.dataset_id) q.set("dataset_id", params.dataset_id);
    if (params?.subset) q.set("subset", params.subset);
    const qs = q.toString();
    return qs ? `${path}?${qs}` : path;
}

export function buildDatasetUrl(datasetId: string, pipelineId?: string, params?: DatasetUrlParams): string {
    const path = params?.specId
        ? `/frozen-datasets/${encodeURIComponent(params.specId)}/datasets/${encodeURIComponent(datasetId)}`
        : pipelineId
          ? `/pipelines/${encodeURIComponent(pipelineId)}/metrics/datasets/${encodeURIComponent(datasetId)}`
          : `/metrics/datasets/${encodeURIComponent(datasetId)}`;
    const q = new URLSearchParams();
    if (params?.subset) q.set("subset", params.subset);
    const qs = q.toString();
    return qs ? `${path}?${qs}` : path;
}

export function buildMetricsUrl(pipelineId?: string): string {
    return pipelineId ? `/pipelines/${encodeURIComponent(pipelineId)}/metrics` : "/metrics";
}

export function buildTableRowUrl(opts: {
    pipelineId: string;
    tableName: string;
    focusColumn: string;
    focusValue: string;
}): string {
    const q = new URLSearchParams({
        focus_col: opts.focusColumn,
        focus_value: opts.focusValue,
    });
    return `/pipelines/${encodeURIComponent(opts.pipelineId)}/tables/${encodeURIComponent(opts.tableName)}?${q}`;
}

export function truncateMiddle(id: string, max = 28): string {
    if (!id || id.length <= max) return id || "";
    const head = Math.ceil((max - 1) / 2);
    const tail = Math.floor((max - 1) / 2);
    return `${id.slice(0, head)}…${id.slice(-tail)}`;
}
