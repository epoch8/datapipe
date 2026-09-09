import { ApiError, apiFetch, readApiErrorBody } from "./http";
import { coreOpsApi } from "./ops";

async function fetchJsonOrThrow(url: string): Promise<unknown> {
    const response = await apiFetch(url);
    if (!response.ok) {
        const detail = await readApiErrorBody(response);
        throw new ApiError("http", `API error (${response.status}): ${detail}`, {
            status: response.status,
            url,
        });
    }
    return response.json();
}

/**
 * Fetch pipeline graph. Prefer {@link coreOpsApi.getGraph} when available;
 * falls back to relative `/api/v1alpha3/graph` for legacy callers.
 */
export async function fetchGraph(
    stage?: string | null,
    labelKey?: string | null,
): Promise<unknown> {
    if (coreOpsApi.getGraph) {
        return coreOpsApi.getGraph({ stage, label_key: labelKey });
    }
    const params = new URLSearchParams();
    if (stage) params.set("stage", stage);
    if (labelKey && labelKey !== "stage") params.set("label_key", labelKey);
    const query = params.toString();
    const url = query ? `/api/v1alpha3/graph?${query}` : "/api/v1alpha3/graph";
    return fetchJsonOrThrow(url);
}

export async function fetchTableSize(tableName: string): Promise<number> {
    const url = `/api/v1alpha3/tables/${encodeURIComponent(tableName)}/size`;
    const data = (await fetchJsonOrThrow(url)) as { size: number };
    return data.size;
}

export async function fetchTransformMetaSize(transformName: string): Promise<number> {
    const url = `/api/v1alpha3/transforms/${encodeURIComponent(transformName)}/meta-size`;
    const data = (await fetchJsonOrThrow(url)) as { size: number };
    return data.size;
}

export function getDefaultTablePageSize(): number {
    const stored = localStorage.getItem("datapipe_table_page_size");
    const parsed = stored ? parseInt(stored, 10) : 5;
    return Number.isFinite(parsed) && parsed > 0 ? parsed : 5;
}

export function setDefaultTablePageSize(size: number): void {
    localStorage.setItem("datapipe_table_page_size", String(size));
}
