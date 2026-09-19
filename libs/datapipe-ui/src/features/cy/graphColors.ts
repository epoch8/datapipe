/** Semantic graph palette — prefers live CSS vars so light/dark theme swaps apply. */

function readCssVar(name: string, fallback: string): string {
    if (typeof document === "undefined") return fallback;
    const root =
        document.getElementById("datapipe-ui-root") ||
        document.querySelector("[data-ui-theme]") ||
        document.documentElement;
    const value = getComputedStyle(root).getPropertyValue(name).trim();
    return value || fallback;
}

export function resolveGraphColors() {
    return {
        canvas: {
            bg: readCssVar("--dp-graph-bg", "#FBFDFF"),
            dot: readCssVar("--dp-graph-dot", "#DCE7F5"),
        },
        table: {
            bg: readCssVar("--dp-table-bg", "#FFF3E5"),
            bg2: readCssVar("--dp-table-bg-2", "#FFE5C7"),
            border: readCssVar("--dp-table-border", "#FF7A1A"),
            icon: readCssVar("--dp-table-icon", "#FF6B00"),
            text: readCssVar("--dp-table-text", "#3E2407"),
        },
        transform: {
            bg: readCssVar("--dp-transform-bg", "#EFFFF3"),
            bg2: readCssVar("--dp-transform-bg-2", "#DCF8E5"),
            border: readCssVar("--dp-transform-border", "#16A34A"),
            icon: readCssVar("--dp-transform-icon", "#0FA83D"),
            text: readCssVar("--dp-transform-text", "#083B18"),
        },
        group: {
            bg: readCssVar("--dp-group-bg", "#EDF5FF"),
            bg2: readCssVar("--dp-group-bg-2", "#DBEAFE"),
            border: readCssVar("--dp-group-border", "#1677FF"),
            icon: readCssVar("--dp-group-icon", "#1677FF"),
            text: readCssVar("--dp-group-text", "#074899"),
            expandedBg: readCssVar("--dp-group-expanded-bg", "rgba(237, 245, 255, 0.48)"),
            expandedBorder: readCssVar("--dp-group-expanded-border", "rgba(22, 119, 255, 0.72)"),
        },
    } as const;
}

export function resolveEdgeColors() {
    return {
        default: readCssVar("--dp-edge", "#52677F"),
        active: readCssVar("--dp-edge-active", "#1677FF"),
        related: readCssVar("--dp-edge-related", "#5BA8A0"),
        error: readCssVar("--dp-edge-error", "#D92D20"),
        sequential: readCssVar("--dp-edge-sequential", "#0F766E"),
    } as const;
}

/** @deprecated Prefer resolveGraphColors() so theme switches take effect. */
export const graphColors = {
    canvas: { bg: "#FBFDFF", dot: "#DCE7F5" },
    table: {
        bg: "#FFF3E5",
        bg2: "#FFE5C7",
        border: "#FF7A1A",
        icon: "#FF6B00",
        text: "#3E2407",
    },
    transform: {
        bg: "#EFFFF3",
        bg2: "#DCF8E5",
        border: "#16A34A",
        icon: "#0FA83D",
        text: "#083B18",
    },
    group: {
        bg: "#EDF5FF",
        bg2: "#DBEAFE",
        border: "#1677FF",
        icon: "#1677FF",
        text: "#074899",
        expandedBg: "rgba(237, 245, 255, 0.48)",
        expandedBorder: "rgba(22, 119, 255, 0.72)",
    },
} as const;

/** @deprecated Prefer resolveEdgeColors(). */
export const edgeColors = {
    default: "#52677F",
    active: "#1677FF",
    related: "#5BA8A0",
    error: "#D92D20",
    sequential: "#0F766E",
} as const;
