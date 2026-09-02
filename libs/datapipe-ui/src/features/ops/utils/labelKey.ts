export const DEFAULT_LABEL_KEY = "stage";

export function normalizeLabelKey(
    value: string | null | undefined,
    available: string[] = [],
): string {
    if (value && (!available.length || available.includes(value))) {
        return value;
    }
    // Trust backend order: stage first when present, else most popular key.
    return available[0] ?? DEFAULT_LABEL_KEY;
}

/** Build `/graph?...` with optional non-default label key. */
export function graphHref(label: string, labelKey: string = DEFAULT_LABEL_KEY): string {
    const params = new URLSearchParams();
    if (labelKey && labelKey !== DEFAULT_LABEL_KEY) {
        params.set("label_key", labelKey);
    }
    params.set("stage", label);
    return `/graph?${params.toString()}`;
}

export function labelKeyFromRunLabels(
    labels: [string, string][] | undefined | null,
): string {
    if (!labels?.length) return DEFAULT_LABEL_KEY;
    const stage = labels.find(([key]) => key === "stage");
    if (stage) return "stage";
    return labels[0]?.[0] ?? DEFAULT_LABEL_KEY;
}
