import {
    columnMatchesSort,
    defaultSortState,
    resolveSortColumnId,
    resolveSortFromTableChange,
    sortOrderForColumn,
} from "./opsTableSort";
import type { OpsTableSchema } from "../../../types/opsSpecs";

const table: OpsTableSchema = {
    id: "model_metrics",
    title: "Model metrics",
    table: "pipeline_model__metrics_on_subset",
    metric_source: "pipeline_model__metrics_on_subset",
    primary_columns: [
        { id: "model", label: "Model", source: "detection_model_id", filterable: true },
        { id: "subset", label: "Subset", source: "subset_id", kind: "chip", filterable: true },
    ],
    metric_columns: [
        { id: "weighted_f1", label: "W-F1", source: "calc__weighted_f1_score", kind: "number", sortable: true },
    ],
    filters: [],
    default_sort: [["weighted_f1", "desc"]],
    entity_links: { model: "detection_model_id", subset: "subset_id" },
};

describe("opsTableSort", () => {
    it("reads default sort from spec", () => {
        expect(defaultSortState(table)).toEqual({ sort_by: "weighted_f1", sort_dir: "desc" });
    });

    it("resolves sort column id from source or column key", () => {
        expect(resolveSortColumnId("calc__weighted_f1_score", table)).toBe("weighted_f1");
        expect(resolveSortColumnId("weighted_f1", table)).toBe("weighted_f1");
    });

    it("matches sort by column id or source", () => {
        const column = table.metric_columns[0] as { id: string; source: string };
        expect(columnMatchesSort(column, "weighted_f1")).toBe(true);
        expect(columnMatchesSort(column, "calc__weighted_f1_score")).toBe(true);
        expect(sortOrderForColumn(column, { sort_by: "weighted_f1", sort_dir: "desc" })).toBe("descend");
    });

    it("cycles table sorter to asc and desc using column id", () => {
        expect(
            resolveSortFromTableChange(
                { field: "calc__weighted_f1_score", order: "ascend", columnKey: "weighted_f1" },
                table,
            ),
        ).toEqual({ sort_by: "weighted_f1", sort_dir: "asc" });

        expect(
            resolveSortFromTableChange(
                { field: "calc__weighted_f1_score", order: "descend", columnKey: "weighted_f1" },
                table,
            ),
        ).toEqual({ sort_by: "weighted_f1", sort_dir: "desc" });
    });

    it("clears sort when sorter order is null", () => {
        expect(resolveSortFromTableChange({ field: "calc__weighted_f1_score", order: null }, table)).toEqual({});
    });
});
