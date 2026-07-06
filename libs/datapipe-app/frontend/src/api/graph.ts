import { ApiError, apiFetch, readApiErrorBody } from "./http";

const GRAPH_BASE =
    (process.env["REACT_APP_GET_GRAPH_URL"] as string) || "/api/v1alpha2/graph";
const API_BASE = GRAPH_BASE.replace(/\/graph$/, "");

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

export async function fetchGraph(stage?: string | null): Promise<unknown> {
    const url = stage ? `${GRAPH_BASE}?stage=${encodeURIComponent(stage)}` : GRAPH_BASE;
    return fetchJsonOrThrow(url);
}

export async function fetchTableSize(tableName: string): Promise<number> {
    const url = `${API_BASE}/tables/${encodeURIComponent(tableName)}/size`;
    const data = (await fetchJsonOrThrow(url)) as { size: number };
    return data.size;
}

export async function fetchTransformMetaSize(transformName: string): Promise<number> {
    const url = `${API_BASE}/transforms/${encodeURIComponent(transformName)}/meta-size`;
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
