import {
    FILTER_OPERATORS,
    activeRules,
    chipKind,
    collectFilterColumns,
    createClientId,
    decodeFiltersParam,
    encodeFiltersParam,
    expandChipValueRules,
    formatRule,
    formatRuleParts,
    mergeTableFilterState,
    isChipColumn,
    isSubsetEntityColumn,
    makeDefaultFilterRule,
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

describe("createClientId", () => {
    it("works without crypto.randomUUID (non-secure context)", () => {
        const original = globalThis.crypto;
        Object.defineProperty(globalThis, "crypto", {
            configurable: true,
            value: {
                getRandomValues: (bytes: Uint8Array) => {
                    for (let i = 0; i < bytes.length; i += 1) bytes[i] = i;
                    return bytes;
                },
            },
        });
        try {
            const id = createClientId();
            expect(id).toMatch(
                /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
            );
        } finally {
            Object.defineProperty(globalThis, "crypto", {
                configurable: true,
                value: original,
            });
        }
    });

    it("makeDefaultFilterRule does not throw without randomUUID", () => {
        const original = globalThis.crypto;
        Object.defineProperty(globalThis, "crypto", {
            configurable: true,
            value: undefined,
        });
        try {
            const rule = makeDefaultFilterRule(columns[0]);
            expect(typeof (rule as { id?: string }).id).toBe("string");
            expect((rule as { id: string }).id.length).toBeGreaterThan(0);
        } finally {
            Object.defineProperty(globalThis, "crypto", {
                configurable: true,
                value: original,
            });
        }
    });
});

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
        expect(formatRuleParts(rule, columns)).toEqual({
            label: "detection_model_id",
            operator: "contains",
            values: ["cat_dog"],
        });
    });

    it("splits comma-separated chip values for display", () => {
        const rule: OpsFilterRule = { column_id: "subset_id", operator: "equal", value: "train,val" };
        expect(formatRuleParts(rule, columns)).toEqual({
            label: "subset_id",
            operator: "equal",
            values: ["train", "val"],
        });
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

    it("roundtrips unicode filter values in URL state", () => {
        const rules: OpsFilterRule[] = [
            { column_id: "detection_model_id", operator: "contains", value: "кот_собака" },
        ];
        const restored = decodeFiltersParam(encodeFiltersParam(rules));
        expect(restored[0]?.value).toBe("кот_собака");
    });

    it("applies spec default_filters when URL has no filters param", () => {
        const params = new URLSearchParams();
        const state = mergeTableFilterState(
            params,
            { default_filters: [{ column_id: "subset_id", operator: "equal", value: "val" }] },
            columns,
        );
        expect(state.rules).toHaveLength(1);
        expect(state.rules[0]?.column_id).toBe("subset_id");
        expect(state.rules[0]?.value).toBe("val");
    });

    it("keeps URL filters when filters param is present", () => {
        const params = new URLSearchParams({
            filters: encodeURIComponent(JSON.stringify([{ column_id: "subset_id", operator: "equal", value: "train" }])),
        });
        const state = mergeTableFilterState(
            params,
            { default_filters: [{ column_id: "subset_id", operator: "equal", value: "val" }] },
            columns,
        );
        expect(state.rules[0]?.value).toBe("train");
    });
});
