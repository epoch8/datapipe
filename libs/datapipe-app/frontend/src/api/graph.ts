const GRAPH_BASE =
    (process.env["REACT_APP_GET_GRAPH_URL"] as string) || "/api/v1alpha2/graph";
const API_BASE = GRAPH_BASE.replace(/\/graph$/, "");

export async function fetchGraph(stage?: string | null): Promise<unknown> {
    const url = stage ? `${GRAPH_BASE}?stage=${encodeURIComponent(stage)}` : GRAPH_BASE;
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Graph ${response.status}`);
    return response.json();
}

export async function fetchTableSize(tableName: string): Promise<number> {
    const response = await fetch(`${API_BASE}/tables/${encodeURIComponent(tableName)}/size`);
    if (!response.ok) throw new Error(`Table size ${response.status}`);
    const data = (await response.json()) as { size: number };
    return data.size;
}

export async function fetchTransformMetaSize(transformName: string): Promise<number> {
    const response = await fetch(
        `${API_BASE}/transforms/${encodeURIComponent(transformName)}/meta-size`,
    );
    if (!response.ok) throw new Error(`Transform meta size ${response.status}`);
    const data = (await response.json()) as { size: number };
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
