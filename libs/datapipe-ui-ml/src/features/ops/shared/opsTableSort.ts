import type { SortOrder, SorterResult } from "antd/es/table/interface";
import type { OpsColumn, OpsMetricColumn, OpsTableSchema } from "../../../types/opsSpecs";

export type OpsTableSortState = { sort_by?: string; sort_dir?: "asc" | "desc" };

function isGroup(column: OpsMetricColumn): column is { label: string; columns: OpsColumn[] } {
    return "columns" in column;
}

export function flattenOpsTableColumns(table: OpsTableSchema): OpsColumn[] {
    const flat: OpsColumn[] = [...table.primary_columns];
    for (const column of table.metric_columns) {
        if (isGroup(column)) flat.push(...column.columns);
        else flat.push(column);
    }
    return flat;
}

export function resolveSortColumnId(raw: string | number | undefined, table: OpsTableSchema): string | undefined {
    if (raw == null || raw === "") return undefined;
    const key = String(raw);
    const column = flattenOpsTableColumns(table).find((item) => item.id === key || item.source === key);
    return column?.id ?? key;
}

export function defaultSortState(table: OpsTableSchema): OpsTableSortState {
    const [column, direction] = table.default_sort[0] ?? [];
    return column && direction ? { sort_by: column, sort_dir: direction } : {};
}

export function columnMatchesSort(column: OpsColumn, sortBy?: string): boolean {
    if (!sortBy) return false;
    return column.id === sortBy || column.source === sortBy;
}

export function sortOrderForColumn(column: OpsColumn, sortState: OpsTableSortState): SortOrder | undefined {
    if (!columnMatchesSort(column, sortState.sort_by)) return undefined;
    return sortState.sort_dir === "asc" ? "ascend" : "descend";
}

/** Server-side sort only: show sorter UI without reordering rows locally. */
export function serverSideSorter(column: OpsColumn) {
    if (!column.sortable) return undefined;
    return {
        compare: () => 0,
        multiple: 1 as const,
    };
}

function sorterKey(value: unknown): string | undefined {
    if (value == null) return undefined;
    if (Array.isArray(value)) return value.map(String).join(".");
    return String(value);
}

export function resolveSortFromTableChange<Row extends object>(
    sorter: SorterResult<Row> | SorterResult<Row>[],
    table: OpsTableSchema,
): OpsTableSortState {
    const active = Array.isArray(sorter) ? sorter[0] : sorter;
    if (!active?.order) {
        return {};
    }
    const sortBy = resolveSortColumnId(sorterKey(active.columnKey) ?? sorterKey(active.field), table);
    if (!sortBy) {
        return {};
    }
    return {
        sort_by: sortBy,
        sort_dir: active.order === "ascend" ? "asc" : "desc",
    };
}
