import type { OpsColumn, OpsSpecDetail } from "../../../types/opsSpecs";

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

export function frozenDatasetHighlightFields(spec: OpsSpecDetail): string[] {
    const frozen = spec.frozen_dataset;
    if (!frozen) return [];
    return [
        frozen.id_column,
        frozen.display_name_column,
        frozen.created_at_column,
        ...Object.values(frozen.split_columns ?? {}),
    ].filter((field): field is string => Boolean(field));
}

export function modelHighlightFields(spec: OpsSpecDetail): string[] {
    const model = spec.model;
    if (!model) return [];

    const fields: Array<string | null | undefined> = [
        model.id_column,
        model.display_name_column,
        model.created_at_column,
        model.artifact_uri_column,
        model.is_best_column,
    ];

    for (const column of spec.training?.extra_columns ?? []) {
        if (column.link_to === "frozen_dataset") {
            fields.push(column.source);
        }
    }

    for (const relation of spec.relations ?? []) {
        if (relation.from_entity === "model" && relation.to_entity === "frozen_dataset") {
            fields.push(relation.to_column);
        }
    }

    return fields.filter(Boolean) as string[];
}

export function isPrimaryKeyField(field: string, sourcePk?: Record<string, unknown> | null): boolean {
    return Boolean(sourcePk && field in sourcePk);
}

export function columnSource(column: OpsColumn | { source: string }): string {
    return column.source;
}
