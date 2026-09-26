import React from "react";
import { Tag } from "antd";
import type { TrainingExperimentRow } from "../../../types/opsMl";

export type ExperimentDisplayStatus = "Archived" | "Standard" | "Editable" | "Locked";

/**
 * Compute the *display* status of an experiment for the UI badge.
 *
 * This is purely presentational. Actions (edit/delete/etc.) MUST be derived
 * from `row.capabilities`, never from this label.
 */
export function experimentDisplayStatus(row: TrainingExperimentRow): ExperimentDisplayStatus {
    if (!row.active) return "Archived";
    if (row.source !== "custom") return "Standard";
    if (row.capabilities.can_edit) return "Editable";
    return "Locked";
}

const STATUS_COLOR: Record<ExperimentDisplayStatus, string> = {
    Archived: "default",
    Standard: "blue",
    Editable: "green",
    Locked: "gold",
};

export function TrainingExperimentStatusTag({ row }: { row: TrainingExperimentRow }) {
    const status = experimentDisplayStatus(row);
    return (
        <Tag color={STATUS_COLOR[status]} className={`te-status te-status-${status.toLowerCase()}`}>
            {status}
        </Tag>
    );
}

/**
 * Render `row.summary.display` (the "main params" of an experiment). The backend
 * codec emits a human-friendly string, but older shapes may send a record or a
 * list of `{label, value}`; handle all defensively.
 */
export function formatExperimentParams(row: TrainingExperimentRow): string {
    const display = row.summary?.display as unknown;
    if (typeof display === "string") return display;
    if (Array.isArray(display)) {
        return display
            .map((item) => {
                if (item && typeof item === "object" && "label" in item) {
                    const entry = item as { label?: unknown; value?: unknown };
                    return `${String(entry.label)}: ${String(entry.value)}`;
                }
                return String(item);
            })
            .join(" · ");
    }
    if (display && typeof display === "object") {
        return Object.entries(display as Record<string, unknown>)
            .map(([key, value]) => `${key}: ${String(value)}`)
            .join(" · ");
    }
    // Fall back to a compact rendering of the raw params.
    const params = row.params ?? {};
    const keys = Object.keys(params).slice(0, 4);
    if (!keys.length) return "—";
    return keys.map((key) => `${key}: ${String(params[key])}`).join(" · ");
}
