import Cytoscape from "cytoscape";
import {
    buildCollapsedLayout,
    GraphLayout,
} from "./incrementalLayout";

/**
 * Pipeline graph layout for `/graph`.
 *
 * Delegates to `buildCollapsedLayout`. Pass `preferChronology=false` for
 * compact (dependency-depth) packing. Label focus is handled via
 * `setGraphLabelFocus`; this function only places nodes.
 */
export function buildLabelColumnLayout(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Iterable<Cytoscape.EdgeDataDefinition>,
    expanded: Set<string>,
    orientation: "LR" | "TB",
    _labelKey: string,
    _preferredLabelOrder: string[] = [],
    pipelineOrders: Map<string, string[]> = new Map(),
    _labelColumnMap: Map<string, string> = new Map(),
    wrapRows = true,
    preferChronology = true,
): GraphLayout {
    return buildCollapsedLayout(
        nodes,
        edges,
        expanded,
        orientation,
        pipelineOrders,
        wrapRows,
        preferChronology,
    );
}

/** Map every label/container id to its top-level ancestor column key. */
export function buildLabelColumnMap(
    nodes: Array<{ id: string; parent_id?: string | null; kind?: string }>,
): Map<string, string> {
    const byId = new Map(nodes.map((node) => [node.id, node]));
    const result = new Map<string, string>();
    nodes.forEach((node) => {
        if (node.kind && node.kind !== "label" && node.kind !== "container") return;
        let current: string | null = node.id;
        const seen = new Set<string>();
        while (current && !seen.has(current)) {
            seen.add(current);
            const parentId: string | null = byId.get(current)?.parent_id ?? null;
            if (!parentId || !byId.has(parentId)) {
                result.set(node.id, current);
                return;
            }
            current = parentId;
        }
        result.set(node.id, node.id);
    });
    return result;
}

/** Top-level label/container ids in order_min order — used for label overview. */
export function topLevelLabelOrder(
    nodes: Array<{
        id: string;
        parent_id?: string | null;
        kind?: string;
        order_min?: number;
        order_max?: number;
    }>,
): string[] {
    return [...nodes]
        .filter(
            (node) =>
                !node.parent_id &&
                (node.kind === "label" || node.kind === "container"),
        )
        .sort(
            (a, b) =>
                (a.order_min ?? 0) - (b.order_min ?? 0) ||
                (a.order_max ?? 0) - (b.order_max ?? 0) ||
                a.id.localeCompare(b.id),
        )
        .map((node) => node.id);
}
