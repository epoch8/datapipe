export const METRIC_ALIASES: Record<string, string> = {
    calc__images_support: "images_support",
    calc__support: "support",
    calc__TP: "TP",
    calc__FP: "FP",
    calc__FN: "FN",
    calc__iou_mean: "iou_mean",
    calc__accuracy: "accuracy",
    calc__precision: "precision",
    calc__recall: "recall",
    calc__f1_score: "f1_score",
    calc__mAP50: "mAP50",
    calc__mAP50_95: "mAP50_95",
    calc__pose_P: "pose_P",
    calc__pose_R: "pose_R",
    calc__pose_mAP50: "pose_mAP50",
    calc__pose_mAP50_95: "pose_mAP50_95",
    metrics_mAP_0_5: "mAP50",
    metrics_mAP_0_5_to_0_95: "mAP50_95",
};

export function normalizeMetricKey(key: string): string {
    return METRIC_ALIASES[key] ?? key.replace(/^calc__/, "");
}

export function readMetricNumber(
    metrics: Record<string, number | string | null | undefined> | undefined,
    key: string,
): number | null {
    if (!metrics || !(key in metrics)) return null;
    const raw = metrics[key];
    if (raw == null) return null;
    if (typeof raw === "string") {
        const stripped = raw.trim();
        if (stripped === "-" || stripped === "—") return 0;
    }
    if (typeof raw === "number") return Number.isNaN(raw) ? null : raw;
    const coerced = Number(raw);
    return Number.isNaN(coerced) ? null : coerced;
}

export function formatMetric(
    value: number | string | null | undefined,
    format: "float" | "percent" | "integer" | "string" = "float",
): string {
    if (value === null || value === undefined) return "—";
    if (typeof value === "string") return value || "—";
    if (Number.isNaN(value)) return "—";
    if (format === "integer") return Math.round(value).toLocaleString();
    if (format === "percent") return `${(value * 100).toFixed(1)}%`;
    if (format === "string") return String(value);
    if (Math.abs(value) >= 1000) return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
    return value.toFixed(3).replace(/\.?0+$/, "") || "0";
}

export function formatDelta(
    delta: number | null | undefined,
    higherIsBetter = true,
): { text: string; positive: boolean } {
    if (delta === null || delta === undefined || Number.isNaN(delta)) {
        return { text: "—", positive: false };
    }
    const sign = delta >= 0 ? "+" : "";
    const positive = higherIsBetter ? delta >= 0 : delta <= 0;
    return { text: `${sign}${delta.toFixed(3)}`, positive };
}

export function formatDeltaPct(
    pct: number | null | undefined,
    higherIsBetter = true,
): { text: string; positive: boolean } {
    if (pct === null || pct === undefined || Number.isNaN(pct)) {
        return { text: "—", positive: false };
    }
    const sign = pct >= 0 ? "+" : "";
    const positive = higherIsBetter ? pct >= 0 : pct <= 0;
    return { text: `(${sign}${pct.toFixed(1)}%)`, positive };
}

export const PRIMARY_METRIC_BY_TASK: Record<string, string> = {
    detection: "mAP50_95",
    classification: "weighted_f1_score",
    keypoints: "pose_mAP50_95",
    segmentation: "mAP50_95",
};

export function smoothPoints<T extends { y: number | null }>(
    points: T[],
    alpha: number,
): T[] {
    if (!alpha || alpha <= 0) return points;
    let prev: number | null = points[0]?.y ?? null;
    return points.map((p) => {
        if (p.y == null) return p;
        prev = prev == null ? p.y : alpha * prev + (1 - alpha) * p.y;
        return { ...p, y: prev };
    });
}

export const RUN_COLORS = ["#705CFF", "#116DFF", "#F97316", "#16A34A"];
