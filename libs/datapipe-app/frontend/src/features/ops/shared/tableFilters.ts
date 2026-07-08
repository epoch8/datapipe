import type { OpsColumn, OpsFilterMode, OpsFilterOperator, OpsFilterRule, OpsMetricColumn } from "../../../types/opsSpecs";

export type { OpsFilterMode, OpsFilterOperator, OpsFilterRule };

export type OpsFilterState = {
    mode: OpsFilterMode;
    rules: OpsFilterRule[];
    search?: string;
};

export const FILTER_OPERATOR_LABELS: Record<OpsFilterOperator, string> = {
    contains: "contains",
    not_contains: "not contains",
    regex: "regex",
    equal: "equal",
    not_equal: "not equal",
    is_empty: "is empty",
};

export const FILTER_OPERATORS: OpsFilterOperator[] = [
    "contains",
    "not_contains",
    "regex",
    "equal",
    "not_equal",
    "is_empty",
];

/** Quick-pick values for subset entity columns (`entity_links.subset`). */
export const SUBSET_CHIP_VALUES = ["train", "val", "test"] as const;

export function resolveColumnEntity(
    column: OpsColumn,
    entityLinks: Record<string, string> = {},
): string | undefined {
    if (column.link_to) return column.link_to;
    for (const [entity, source] of Object.entries(entityLinks)) {
        if (source === column.source) return entity;
    }
    return column.id;
}

export function defaultOperatorForColumn(column: OpsColumn): OpsFilterOperator {
    return column.kind === "chip" ? "equal" : "contains";
}

export function makeDefaultFilterRule(column: OpsColumn): OpsFilterRule {
    return {
        id: crypto.randomUUID(),
        column_id: column.source,
        operator: defaultOperatorForColumn(column),
        value: "",
    } as OpsFilterRule & { id: string };
}

export function isChipColumn(column: OpsColumn | undefined): boolean {
    return column?.kind === "chip";
}

export function isSubsetEntityColumn(
    column: OpsColumn | undefined,
    entityLinks: Record<string, string> = {},
): boolean {
    return column ? resolveColumnEntity(column, entityLinks) === "subset" : false;
}

export function findFilterColumn(columnId: string, columns: OpsColumn[]): OpsColumn | undefined {
    return columns.find((col) => col.source === columnId || col.id === columnId);
}

export function normalizeRuleColumnId(columnId: string, columns: OpsColumn[]): string {
    return findFilterColumn(columnId, columns)?.source ?? columnId;
}

export function flattenMetricColumns(columns: OpsMetricColumn[]): OpsColumn[] {
    const flat: OpsColumn[] = [];
    for (const column of columns) {
        if ("columns" in column) flat.push(...column.columns);
        else flat.push(column);
    }
    return flat;
}

export function collectFilterColumns(table: {
    primary_columns: OpsColumn[];
    metric_columns: OpsMetricColumn[];
    filters?: OpsColumn[];
}): OpsColumn[] {
    const bySource = new Map<string, OpsColumn>();
    const add = (col: OpsColumn) => {
        if (!col.filterable || !col.source) return;
        if (!bySource.has(col.source)) bySource.set(col.source, col);
    };
    for (const col of table.primary_columns) add(col);
    for (const col of flattenMetricColumns(table.metric_columns)) add(col);
    for (const col of table.filters ?? []) add(col);
    return Array.from(bySource.values());
}

export function columnLabel(columnId: string, columns: OpsColumn[]): string {
    return normalizeRuleColumnId(columnId, columns);
}

export function formatRule(rule: OpsFilterRule, columns: OpsColumn[]): string {
    const label = columnLabel(rule.column_id, columns);
    const operator = FILTER_OPERATOR_LABELS[rule.operator];
    if (rule.operator === "is_empty") return `${label} ${operator}`;
    return `${label} ${operator} ${rule.value ?? ""}`.trim();
}

export function formatRuleParts(
    rule: OpsFilterRule,
    columns: OpsColumn[],
): { label: string; operator: string; values: string[] } {
    const label = columnLabel(rule.column_id, columns);
    const operator = FILTER_OPERATOR_LABELS[rule.operator];
    if (rule.operator === "is_empty") {
        return { label, operator, values: [] };
    }
    const values = (rule.value ?? "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
    return { label, operator, values };
}

export function chipKind(
    columnId: string,
    columns: OpsColumn[],
    entityLinks: Record<string, string> = {},
): "model" | "subset" | "dataset" | "source" | "default" {
    const column = findFilterColumn(columnId, columns);
    const entity = column ? resolveColumnEntity(column, entityLinks) : entityForSource(columnId, entityLinks);
    if (entity === "model") return "model";
    if (entity === "subset") return "subset";
    if (entity === "frozen_dataset" || entity === "dataset") return "dataset";
    if (columnId === "metric_source") return "source";
    return "default";
}

function entityForSource(source: string, entityLinks: Record<string, string>): string | undefined {
    for (const [entity, linkedSource] of Object.entries(entityLinks)) {
        if (linkedSource === source) return entity;
    }
    return undefined;
}

export function activeRules(rules: OpsFilterRule[]): OpsFilterRule[] {
    return rules.filter((rule) => rule.operator === "is_empty" || Boolean(rule.value?.trim()));
}

export function serializeFilterRules(rules: OpsFilterRule[], columns: OpsColumn[] = []): OpsFilterRule[] {
    return activeRules(rules).map(({ column_id, operator, value }) => ({
        column_id: normalizeRuleColumnId(column_id, columns),
        operator,
        ...(operator === "is_empty" ? {} : { value: value?.trim() }),
    }));
}

export type OpsFilterRuleWithId = OpsFilterRule & { id: string };

export function ensureRuleIds(rules: OpsFilterRule[]): OpsFilterRuleWithId[] {
    return rules.map((rule) => ({
        ...rule,
        id: (rule as OpsFilterRuleWithId).id ?? crypto.randomUUID(),
    }));
}

export function encodeFiltersParam(rules: OpsFilterRule[]): string {
    return encodeURIComponent(JSON.stringify(rules));
}

export function decodeFiltersParam(encoded: string): OpsFilterRule[] {
    try {
        const parsed = JSON.parse(decodeURIComponent(encoded)) as unknown;
        return Array.isArray(parsed) ? (parsed as OpsFilterRule[]) : [];
    } catch {
        try {
            const parsed = JSON.parse(atob(encoded)) as unknown;
            return Array.isArray(parsed) ? (parsed as OpsFilterRule[]) : [];
        } catch {
            return [];
        }
    }
}

export function parseUrlFilterState(searchParams: URLSearchParams): OpsFilterState {
    const mode = searchParams.get("mode") === "and" ? "and" : "or";
    const search = searchParams.get("search") ?? "";
    const encoded = searchParams.get("filters");
    if (!encoded) {
        return { mode, rules: [], search };
    }
    return { mode, rules: ensureRuleIds(decodeFiltersParam(encoded)), search };
}

export function mergeTableFilterState(
    searchParams: URLSearchParams,
    table: { default_filters?: OpsFilterRule[] },
    columns: OpsColumn[],
): OpsFilterState {
    const fromUrl = parseUrlFilterState(searchParams);
    if (searchParams.has("filters")) {
        return fromUrl;
    }
    const defaults = table.default_filters ?? [];
    if (!defaults.length) {
        return fromUrl;
    }
    return {
        mode: fromUrl.mode,
        search: fromUrl.search,
        rules: ensureRuleIds(
            defaults.map((rule) => ({
                column_id: normalizeRuleColumnId(rule.column_id, columns),
                operator: rule.operator,
                value: rule.value,
            })),
        ),
    };
}

export function expandChipValueRules(rules: OpsFilterRule[], columns: OpsColumn[]): OpsFilterRule[] {
    return rules.flatMap((rule) => {
        const column = findFilterColumn(rule.column_id, columns);
        if (!isChipColumn(column) || rule.operator !== "equal" || !rule.value?.includes(",")) {
            return [rule];
        }
        return rule.value
            .split(",")
            .map((item) => item.trim())
            .filter(Boolean)
            .map((value) => ({
                column_id: normalizeRuleColumnId(rule.column_id, columns),
                operator: rule.operator,
                value,
            }));
    });
}

export function writeUrlFilterState(
    params: URLSearchParams,
    state: OpsFilterState,
    columns: OpsColumn[] = [],
): URLSearchParams {
    const next = new URLSearchParams(params);
    if (state.search?.trim()) next.set("search", state.search.trim());
    else next.delete("search");
    if (state.mode === "and") next.set("mode", "and");
    else next.delete("mode");
    const rules = serializeFilterRules(state.rules, columns);
    if (rules.length) next.set("filters", encodeFiltersParam(rules));
    else next.delete("filters");
    return next;
}
