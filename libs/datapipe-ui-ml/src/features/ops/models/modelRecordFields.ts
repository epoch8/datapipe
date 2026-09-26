import type { OpsSpecDetail } from "../../../types/opsSpecs";

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

    for (const column of spec.training?.columns ?? []) {
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
