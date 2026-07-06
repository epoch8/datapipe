import type { MetricColumnGroup, MetricDefinition, MetricsModelRow, MetricsTableSchema } from "../../../types/ops";

const COUNT_KEYS = new Set([
    "images_support",
    "support",
    "TP",
    "FP",
    "FN",
    "images",
    "objects",
    "detections",
]);

function def(
    key: string,
    label: string,
    short: string,
    group: string,
    opts: Partial<MetricDefinition> = {},
): MetricDefinition {
    return {
        key,
        label,
        short_label: short,
        group,
        task_types: ["*"],
        format: "float",
        higher_is_better: true,
        visible_by_default: true,
        ...opts,
    };
}

function classificationGroups(): MetricColumnGroup[] {
    return [
        {
            key: "core",
            label: "Core",
            priority: 1,
            metrics: [
                def("weighted_f1_score", "Primary F1", "F1", "core", { primary: true }),
                def("accuracy", "Accuracy", "Acc", "core"),
            ],
        },
        {
            key: "weighted",
            label: "Weighted",
            priority: 2,
            metrics: [
                def("weighted_f1_score", "Weighted F1", "F1", "weighted"),
                def("weighted_precision", "Weighted P", "P", "weighted"),
                def("weighted_recall", "Weighted R", "R", "weighted"),
            ],
        },
        {
            key: "macro",
            label: "Macro",
            priority: 3,
            metrics: [
                def("macro_f1_score", "Macro F1", "F1", "macro"),
                def("macro_precision", "Macro P", "P", "macro"),
                def("macro_recall", "Macro R", "R", "macro"),
            ],
        },
        {
            key: "no_pseudo",
            label: "No pseudo",
            priority: 4,
            metrics: [
                def("weighted_without_pseudo_classes_f1_score", "W F1", "W-F1", "no_pseudo", { visible_by_default: false }),
                def("weighted_without_pseudo_classes_precision", "W Precision", "W-P", "no_pseudo", { visible_by_default: false }),
                def("weighted_without_pseudo_classes_recall", "W Recall", "W-R", "no_pseudo", { visible_by_default: false }),
                def("macro_without_pseudo_classes_f1_score", "M F1", "M-F1", "no_pseudo", { visible_by_default: false }),
                def("macro_without_pseudo_classes_precision", "M Precision", "M-P", "no_pseudo", { visible_by_default: false }),
                def("macro_without_pseudo_classes_recall", "M Recall", "M-R", "no_pseudo", { visible_by_default: false }),
            ],
        },
    ];
}

function countsGroup(): MetricColumnGroup {
    return {
        key: "counts",
        label: "Counts",
        priority: 90,
        metrics: [
            def("support", "Support", "Sup", "counts", { format: "integer" }),
            def("images_support", "Images", "Img", "counts", { format: "integer" }),
            def("images", "Images", "Img", "counts", { format: "integer" }),
            def("objects", "Objects", "Obj", "counts", { format: "integer" }),
            def("detections", "Detections", "Det", "counts", { format: "integer" }),
            def("TP", "TP", "TP", "counts", { format: "integer" }),
            def("FP", "FP", "FP", "counts", { format: "integer", higher_is_better: false }),
            def("FN", "FN", "FN", "counts", { format: "integer", higher_is_better: false }),
        ],
    };
}

function detectionGroups(): MetricColumnGroup[] {
    return [
        {
            key: "core",
            label: "Core",
            priority: 1,
            metrics: [
                def("mAP50_95", "mAP50-95", "mAP", "core", { primary: true }),
                def("mAP50", "mAP50", "mAP50", "core"),
            ],
        },
        {
            key: "weighted",
            label: "Weighted",
            priority: 2,
            metrics: [
                def("weighted_f1_score", "Weighted F1", "F1", "weighted"),
                def("weighted_precision", "Weighted P", "P", "weighted"),
                def("weighted_recall", "Weighted R", "R", "weighted"),
            ],
        },
        {
            key: "macro",
            label: "Macro",
            priority: 3,
            metrics: [
                def("macro_f1_score", "Macro F1", "F1", "macro"),
                def("macro_precision", "Macro P", "P", "macro"),
                def("macro_recall", "Macro R", "R", "macro"),
            ],
        },
        {
            key: "quality",
            label: "Detection",
            priority: 4,
            metrics: [
                def("precision", "Precision", "P", "quality"),
                def("recall", "Recall", "R", "quality"),
                def("f1_score", "F1", "F1", "quality"),
            ],
        },
    ];
}

export function inferTaskTypeFromMetrics(keys: string[]): string | undefined {
    const set = new Set(keys);
    if (set.has("mAP50") || set.has("mAP50_95")) return "detection";
    if (Array.from(set).some((k) => k.startsWith("pose_"))) return "keypoints";
    if (
        set.has("weighted_f1_score")
        || set.has("macro_f1_score")
        || set.has("weighted_precision")
        || set.has("accuracy")
    ) {
        return "classification";
    }
    return undefined;
}

function isBlobSchema(schema: MetricsTableSchema): boolean {
    const quality = schema.groups.find((g) => g.key === "quality");
    if (quality && quality.metrics.length > 4) return true;
    if (schema.groups.length === 1 && schema.groups[0].metrics.length > 4) return true;
    return false;
}

function filterSchemaToAvailable(
    schema: MetricsTableSchema,
    available: string[],
    opts?: { includeCounts?: boolean },
): MetricsTableSchema {
    const present = new Set(available.filter((k) => opts?.includeCounts || !COUNT_KEYS.has(k)));
    const seen = new Set<string>();
    const groups: MetricColumnGroup[] = [];

    for (const group of schema.groups) {
        const metrics = group.metrics.filter((m) => {
            if (seen.has(m.key)) return false;
            if (!present.has(m.key)) return false;
            seen.add(m.key);
            return true;
        });
        if (metrics.length) {
            groups.push({ ...group, metrics });
        }
    }

    const otherKeys = Array.from(present).filter((k) => !seen.has(k)).sort();
    if (otherKeys.length) {
        groups.push({
            key: "other",
            label: "Other",
            priority: 99,
            metrics: otherKeys.map((key) => def(key, key, key.slice(0, 4), "other", { visible_by_default: false })),
        });
    }

    const primary = schema.primary_metric && present.has(schema.primary_metric)
        ? schema.primary_metric
        : groups[0]?.metrics.find((m) => m.primary)?.key
            ?? groups[0]?.metrics[0]?.key
            ?? "f1_score";

    return { task_type: schema.task_type, primary_metric: primary, groups };
}

function fullCanonicalSchema(taskType: string): MetricsTableSchema {
    const tt = taskType === "cls" ? "classification" : taskType;
    let groups: MetricColumnGroup[];
    let primary = "f1_score";
    if (tt === "classification") {
        groups = [...classificationGroups(), countsGroup()];
        primary = "weighted_f1_score";
    } else if (tt === "detection") {
        groups = [...detectionGroups(), countsGroup()];
        primary = "mAP50_95";
    } else {
        groups = [countsGroup()];
    }
    return { task_type: tt, primary_metric: primary, groups };
}

function canonicalSchema(taskType: string, available: string[]): MetricsTableSchema {
    const tt = taskType === "cls" ? "classification" : taskType;
    let groups: MetricColumnGroup[];
    let primary = "f1_score";
    if (tt === "classification") {
        groups = classificationGroups();
        primary = "weighted_f1_score";
    } else if (tt === "detection") {
        groups = detectionGroups();
        primary = "mAP50_95";
    } else {
        groups = [];
    }
    return filterSchemaToAvailable({ task_type: tt, primary_metric: primary, groups }, available);
}

export function buildMetricSchema(
    taskType: string | undefined,
    availableMetrics: string[],
    backendSchema?: MetricsTableSchema,
): MetricsTableSchema {
    const available = availableMetrics.filter((k) => !COUNT_KEYS.has(k));
    const inferred =
        (taskType && taskType !== "auto" ? taskType : undefined)
        ?? inferTaskTypeFromMetrics(available)
        ?? (backendSchema?.task_type !== "custom" ? backendSchema?.task_type : undefined)
        ?? "classification";

    if (inferred === "classification" || inferred === "detection" || inferred === "cls") {
        return canonicalSchema(inferred, available);
    }

    if (backendSchema && !isBlobSchema(backendSchema)) {
        return filterSchemaToAvailable(backendSchema, available);
    }

    return canonicalSchema("classification", available);
}

export type MetricsViewMode = "detailed" | "all";

const ALL_GROUP_ORDER = ["core", "weighted", "macro", "no_pseudo", "quality", "localization", "pose", "task_specific", "counts", "other"];

function sortGroups(groups: MetricColumnGroup[]): MetricColumnGroup[] {
    return [...groups].sort(
        (a, b) => (ALL_GROUP_ORDER.indexOf(a.key) + 1 || 99) - (ALL_GROUP_ORDER.indexOf(b.key) + 1 || 99),
    );
}

export function collectRowMetricKeys(rows: MetricsModelRow[]): string[] {
    const keys = new Set<string>();
    for (const row of rows) {
        for (const [k, v] of Object.entries(row.metrics ?? {})) {
            if (v != null && typeof v === "number") keys.add(k);
        }
    }
    return Array.from(keys);
}

export function buildAllMetricsSchema(
    baseSchema: MetricsTableSchema,
    rows: MetricsModelRow[],
): MetricsTableSchema {
    const keys = collectRowMetricKeys(rows);
    if (!keys.length) return baseSchema;
    const full = fullCanonicalSchema(baseSchema.task_type);
    return filterSchemaToAvailable(full, keys, { includeCounts: true });
}

const DETAILED_GROUP_ORDER = ["core", "weighted", "macro", "no_pseudo", "quality", "localization", "pose", "task_specific"];

export function visibleGroups(
    schema: MetricsTableSchema,
    viewMode: MetricsViewMode,
): MetricColumnGroup[] {
    if (viewMode === "all") {
        return sortGroups(schema.groups);
    }
    const sorted = [...schema.groups].sort(
        (a, b) => (DETAILED_GROUP_ORDER.indexOf(a.key) + 1 || 99) - (DETAILED_GROUP_ORDER.indexOf(b.key) + 1 || 99),
    );
    return sorted.filter(
        (g) => !["other", "counts", "quality"].includes(g.key) || g.metrics.length <= 3,
    ).filter((g) => g.key !== "other");
}

export function primaryMetricDef(schema: MetricsTableSchema): MetricDefinition | undefined {
    for (const group of schema.groups) {
        const found = group.metrics.find((m) => m.primary || m.key === schema.primary_metric);
        if (found) return found;
    }
    return schema.groups[0]?.metrics[0];
}

export function groupMetricItems(
    rowMetrics: Record<string, number | null | undefined> | undefined,
    group: MetricColumnGroup,
    limit = 3,
) {
    return group.metrics
        .filter((m) => rowMetrics?.[m.key] != null)
        .slice(0, limit)
        .map((m) => ({
            label: m.short_label,
            value: rowMetrics?.[m.key],
            format: m.format,
        }));
}
