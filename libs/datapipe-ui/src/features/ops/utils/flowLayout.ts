import type { GraphFlowLayout } from "../../../types/pipelineGraph";

export function parseFlowLayout(searchParams: URLSearchParams): GraphFlowLayout {
    const layout = searchParams.get("layout");
    if (
        layout === "snake" ||
        layout === "horizontal" ||
        layout === "vertical" ||
        layout === "horizontal_compact" ||
        layout === "vertical_compact"
    ) {
        return layout;
    }
    // Legacy `direction=` links.
    if (searchParams.get("direction") === "TB") return "vertical";
    if (searchParams.get("direction") === "LR") return "horizontal";
    return "snake";
}

export const FLOW_LAYOUT_SEGMENTED_OPTIONS = [
    { label: "Zigzag", value: "snake" },
    { label: "Horizontal", value: "horizontal" },
    { label: "H Compact", value: "horizontal_compact" },
    { label: "Vertical", value: "vertical" },
    { label: "V Compact", value: "vertical_compact" },
] as const;
