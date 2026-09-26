import type { OpsSpecDetail } from "../../../types/opsSpecs";

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
