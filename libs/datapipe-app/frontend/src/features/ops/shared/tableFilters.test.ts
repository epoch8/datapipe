import {
    FILTER_OPERATORS,
    activeRules,
    chipKind,
    collectFilterColumns,
    expandChipValueRules,
    formatRule,
    isChipColumn,
    isSubsetEntityColumn,
    resolveColumnEntity,
    serializeFilterRules,
} from "./tableFilters";
import type { OpsColumn, OpsFilterRule } from "../../../types/opsSpecs";

const entityLinks = {
    model: "detection_model_id",
    subset: "subset_id",
    frozen_dataset: "detection_frozen_dataset_id",
};

const columns: OpsColumn[] = [
    { id: "model", label: "Model", source: "detection_model_id", filterable: true, link_to: "model" },
    { id: "subset", label: "Subset", source: "subset_id", kind: "chip", filterable: true },
];

describe("tableFilters", () => {
    it("keeps only allowed operators", () => {
        expect(FILTER_OPERATORS).toEqual([
            "contains",
            "not_contains",
            "regex",
            "equal",
            "not_equal",
            "is_empty",
        ]);
        expect(FILTER_OPERATORS).not.toContain("in");
        expect(FILTER_OPERATORS).not.toContain("greater_than");
    });

    it("formats active rules with raw source column names", () => {
        const rule: OpsFilterRule = { column_id: "detection_model_id", operator: "contains", value: "cat_dog" };
        expect(formatRule(rule, columns)).toBe("detection_model_id contains cat_dog");
    });

    it("deduplicates filter columns by source from spec schema", () => {
        const deduped = collectFilterColumns({
            primary_columns: [{ id: "subset", label: "Subset", source: "subset_id", kind: "chip", filterable: true }],
            metric_columns: [],
            filters: [{ id: "subset_filter", label: "Subset", source: "subset_id", kind: "chip", filterable: true }],
        });
        expect(deduped).toHaveLength(1);
        expect(deduped[0]?.source).toBe("subset_id");
    });

    it("resolves entity and chip behavior from entity_links", () => {
        expect(resolveColumnEntity(columns[1], entityLinks)).toBe("subset");
        expect(isSubsetEntityColumn(columns[1], entityLinks)).toBe(true);
        expect(isChipColumn(columns[1])).toBe(true);
        expect(chipKind("subset_id", columns, entityLinks)).toBe("subset");
        expect(chipKind("detection_model_id", columns, entityLinks)).toBe("model");
        expect(chipKind("detection_frozen_dataset_id", columns, entityLinks)).toBe("dataset");
    });

    it("serializes rules with normalized source column ids", () => {
        const rules: OpsFilterRule[] = [
            { column_id: "model", operator: "contains", value: "cat_dog" },
            { column_id: "subset_id", operator: "equal", value: "" },
            { column_id: "model", operator: "is_empty" },
        ];
        expect(serializeFilterRules(rules, columns)).toEqual([
            { column_id: "detection_model_id", operator: "contains", value: "cat_dog" },
            { column_id: "detection_model_id", operator: "is_empty" },
        ]);
        expect(activeRules(rules)).toHaveLength(2);
    });

    it("expands chip column comma values into separate equal rules", () => {
        const rules: OpsFilterRule[] = [{ column_id: "subset_id", operator: "equal", value: "train,val" }];
        expect(expandChipValueRules(rules, columns)).toEqual([
            { column_id: "subset_id", operator: "equal", value: "train" },
            { column_id: "subset_id", operator: "equal", value: "val" },
        ]);
    });
});
