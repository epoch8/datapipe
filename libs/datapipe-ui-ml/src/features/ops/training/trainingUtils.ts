import type { TrainingExperimentRow, TrainingExperimentSummaryMeta } from "../../../types/opsMl";

export type DisplayParam = { label: string; value: string };

function formatParamValue(value: unknown): string {
    if (value === null || value === undefined) return "-";
    if (typeof value === "boolean") return value ? "true" : "false";
    if (typeof value === "number") {
        return Number.isInteger(value) ? String(value) : value.toFixed(3);
    }
    if (typeof value === "object") return JSON.stringify(value);
    return String(value);
}

/**
 * Normalize ``experiment.summary.display`` into an ordered list of label/value
 * pairs. The backend may send it either as an array of ``{label, value}`` or as
 * a plain object (spec §25).
 */
export function mainParams(summary: TrainingExperimentSummaryMeta | undefined): DisplayParam[] {
    if (!summary) return [];
    const display = summary.display;
    if (Array.isArray(display)) {
        return display.map((item) => ({
            label: String(item.label),
            value: formatParamValue(item.value),
        }));
    }
    if (display && typeof display === "object") {
        return Object.entries(display).map(([label, value]) => ({
            label,
            value: formatParamValue(value),
        }));
    }
    return [];
}

export function experimentName(exp: TrainingExperimentRow): string {
    return exp.display_name?.trim() || exp.id;
}

export function formatDateTime(value?: string | null): string {
    if (!value) return "-";
    if (/^\d{4}-\d{2}-\d{2}T/.test(value)) return value.slice(0, 16).replace("T", " ");
    return value;
}

/**
 * Generate a stable idempotency key for a training request. Uses
 * ``crypto.randomUUID`` when available and falls back to a manual v4-ish UUID
 * for older/test environments (spec §31).
 */
export function newClientRequestId(): string {
    const c = typeof globalThis !== "undefined" ? (globalThis.crypto as Crypto | undefined) : undefined;
    if (c && typeof c.randomUUID === "function") {
        return c.randomUUID();
    }
    return "req-xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (ch) => {
        const r = (Math.random() * 16) | 0;
        const v = ch === "x" ? r : (r & 0x3) | 0x8;
        return v.toString(16);
    });
}

type SchemaPropertyWithDefault = {
    default?: unknown;
};

/**
 * Extract default param values from a train-config JSON Schema so New experiment
 * forms can show every field already filled (form + Advanced JSON).
 */
export function defaultsFromTrainConfigSchema(
    schema?: Record<string, unknown> | null,
): Record<string, unknown> {
    if (!schema || typeof schema !== "object") return {};
    const properties = (schema as { properties?: unknown }).properties;
    if (!properties || typeof properties !== "object") return {};
    const defaults: Record<string, unknown> = {};
    for (const [key, raw] of Object.entries(properties as Record<string, SchemaPropertyWithDefault>)) {
        if (!raw || typeof raw !== "object") continue;
        if (!Object.prototype.hasOwnProperty.call(raw, "default")) continue;
        defaults[key] = raw.default;
    }
    return defaults;
}
