export type SortSpec = { field: string; direction: "asc" | "desc" };

export function parseSortParams(sortBy: string, sortDir: string): SortSpec[] {
    const fields = sortBy.split(",").map((s) => s.trim()).filter(Boolean);
    if (!fields.length) return [];
    const dirs = sortDir.split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
    const normalizedDirs = dirs.map((d) => (d === "asc" ? "asc" : "desc"));
    while (normalizedDirs.length < fields.length) {
        normalizedDirs.push(normalizedDirs[normalizedDirs.length - 1] ?? "desc");
    }
    return fields.map((field, i) => ({ field, direction: normalizedDirs[i] as "asc" | "desc" }));
}

export function serializeSortParams(sorts: SortSpec[]): { sort_by: string; sort_dir: string } {
    if (!sorts.length) return { sort_by: "", sort_dir: "" };
    return {
        sort_by: sorts.map((s) => s.field).join(","),
        sort_dir: sorts.map((s) => s.direction).join(","),
    };
}
