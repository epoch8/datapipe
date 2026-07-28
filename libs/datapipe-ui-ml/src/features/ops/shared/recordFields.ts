import type { OpsColumn } from "../../../types/opsSpecs";

function addUnique(target: string[], seen: Set<string>, key?: string | null) {
    if (!key || seen.has(key)) return;
    seen.add(key);
    target.push(key);
}

export function recordFieldOrder(opts: {
    record?: Record<string, unknown> | null;
    sourcePk?: Record<string, unknown> | null;
    highlightFields?: Array<string | null | undefined>;
    maxFields?: number;
}): string[] {
    const { record, sourcePk, highlightFields = [], maxFields = 32 } = opts;
    if (!record) return [];

    const seen = new Set<string>();
    const ordered: string[] = [];

    if (sourcePk) {
        for (const key of Object.keys(sourcePk)) {
            addUnique(ordered, seen, key);
        }
    }

    for (const key of highlightFields) {
        addUnique(ordered, seen, key ?? undefined);
    }

    for (const key of Object.keys(record).sort()) {
        addUnique(ordered, seen, key);
    }

    return ordered.slice(0, maxFields);
}

export function isPrimaryKeyField(field: string, sourcePk?: Record<string, unknown> | null): boolean {
    return Boolean(sourcePk && field in sourcePk);
}

export function columnSource(column: OpsColumn | { source: string }): string {
    return column.source;
}
