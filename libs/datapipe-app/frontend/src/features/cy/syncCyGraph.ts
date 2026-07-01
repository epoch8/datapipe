import Cytoscape from "cytoscape";
import { GraphData, MetaNode } from "../../types";
import {
    ANIMATION_MS,
    animateLayoutTransition,
    applyLayoutToCy,
    buildCollapsedLayout,
    cloneLayout,
    collapseGroupInLayout,
    expandGroupInLayout,
    fitGraphViewport,
    getInnerNodeIdsFromLayout,
    GraphLayout,
    stopLayoutAnimations,
} from "./incrementalLayout";
import { setNodeVisualOpacity } from "./htmlLabelOpacity";
import { reprocessData } from "./process";

export type CyElement = Cytoscape.ElementDefinition;

export type SyncMode = "fit" | "preserve";

export type SyncOptions = {
    mode: SyncMode;
    rankDir?: "TB" | "LR";
    anchorGroup?: string | null;
    expanding?: boolean;
    onLayoutComplete?: () => void;
};

const layoutStore = new WeakMap<Cytoscape.Core, GraphLayout>();
const layoutTimerStore = new WeakMap<Cytoscape.Core, number>();
const preExpandStore = new WeakMap<Cytoscape.Core, Map<string, GraphLayout>>();
const structureKeyStore = new WeakMap<Cytoscape.Core, string>();

function buildElements(data: GraphData, expanded: Set<string>): CyElement[] {
    const { nodes, edges } = reprocessData(data, expanded);
    const elements: CyElement[] = Array.from(nodes.entries())
        .sort(([, a], [, b]) => {
            const aParent = a.type === "group-expanded" ? 0 : 1;
            const bParent = b.type === "group-expanded" ? 0 : 1;
            return aParent - bParent;
        })
        .map(([nodeId, options]) => ({
            selectable: true,
            data: {
                id: nodeId,
                label: options.name || nodeId,
                ...options,
            },
        }));
    edges.forEach((edge) => {
        elements.push({
            grabbable: false,
            data: edge,
        });
    });
    return elements;
}

function edgeKey(source: string, target: string): string {
    return `${source}->${target}`;
}

function applyElementDiff(cy: Cytoscape.Core, target: CyElement[]) {
    const targetNodes = target.filter((el) => el.data.id);
    const targetEdges = target.filter((el) => el.data.source && el.data.target);
    const targetNodeIds = new Set(targetNodes.map((el) => el.data.id as string));
    const targetEdgeKeys = new Set(
        targetEdges.map((el) => edgeKey(el.data.source as string, el.data.target as string)),
    );

    cy.batch(() => {
        cy.nodes().forEach((node) => {
            if (!targetNodeIds.has(node.id())) {
                node.remove();
            }
        });
        cy.edges().forEach((edge) => {
            const key = edgeKey(edge.source().id(), edge.target().id());
            if (!targetEdgeKeys.has(key)) {
                edge.remove();
            }
        });

        targetNodes.forEach((el) => {
            const id = el.data.id as string;
            const existing = cy.getElementById(id);
            if (existing.nonempty()) {
                const node = existing as unknown as Cytoscape.NodeSingular;
                const nextParent = (el.data.parent as string) ?? null;
                const currentParent = node.isChild() ? node.parent().first().id() : null;
                node.data(el.data);
                if (nextParent !== currentParent) {
                    node.move({ parent: nextParent });
                }
            } else {
                cy.add(el);
            }
        });

        targetEdges.forEach((el) => {
            const key = edgeKey(el.data.source as string, el.data.target as string);
            const found = cy.edges().filter(
                (edge) => edgeKey(edge.source().id(), edge.target().id()) === key,
            );
            if (found.empty()) {
                cy.add(el);
            }
        });
    });
}

function captureCenters(cy: Cytoscape.Core): Map<string, { x: number; y: number }> {
    const centers = new Map<string, { x: number; y: number }>();
    cy.nodes().forEach((node) => {
        centers.set(node.id(), { ...node.position() });
    });
    return centers;
}

function getMetaPipelineOrder(data: GraphData, groupId: string): string[] {
    const meta = data.pipeline.find(
        (pipe): pipe is MetaNode => pipe.type === "meta" && pipe.name === groupId,
    );
    if (!meta) return [];
    return meta.graph.pipeline.filter((step) => step.type !== "meta").map((step) => step.name);
}

function pipelineOrdersFor(data: GraphData, expanded: Set<string>): Map<string, string[]> {
    const orders = new Map<string, string[]>();
    expanded.forEach((groupId) => {
        orders.set(groupId, getMetaPipelineOrder(data, groupId));
    });
    return orders;
}

function getInnerNodeIds(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    groupId: string,
): Set<string> {
    const ids = new Set<string>();
    nodes.forEach((data, id) => {
        if (data.metaGroup === groupId) ids.add(id);
    });
    return ids;
}

function savePreExpandLayout(cy: Cytoscape.Core, groupId: string, layout: GraphLayout): void {
    let groups = preExpandStore.get(cy);
    if (!groups) {
        groups = new Map();
        preExpandStore.set(cy, groups);
    }
    groups.set(groupId, cloneLayout(layout));
}

function takePreExpandLayout(cy: Cytoscape.Core, groupId: string): GraphLayout | undefined {
    const groups = preExpandStore.get(cy);
    const layout = groups?.get(groupId);
    groups?.delete(groupId);
    return layout;
}

function graphStructureKey(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Iterable<Cytoscape.EdgeDataDefinition>,
    expanded: Set<string>,
): string {
    const nodeIds = Array.from(nodes.keys()).sort();
    const edgeList = Array.from(edges)
        .map((edge) => `${edge.source as string}->${edge.target as string}`)
        .sort();
    const expandedIds = Array.from(expanded).sort();
    return JSON.stringify({ nodeIds, edgeList, expandedIds });
}

function clearLayoutTimer(cy: Cytoscape.Core): void {
    const prev = layoutTimerStore.get(cy);
    if (prev != null) {
        window.clearTimeout(prev);
        layoutTimerStore.delete(cy);
    }
}

function scheduleLayoutComplete(cy: Cytoscape.Core, options: SyncOptions): void {
    if (!options.onLayoutComplete) return;
    clearLayoutTimer(cy);
    const timer = window.setTimeout(() => {
        layoutTimerStore.delete(cy);
        if (!cy.destroyed()) {
            options.onLayoutComplete?.();
        }
    }, ANIMATION_MS + 40);
    layoutTimerStore.set(cy, timer);
}

/**
 * Sync graph elements and apply deterministic incremental layout.
 * Initial view uses layered DAG; expand/collapse only shifts the affected region.
 */
export function syncCyGraph(
    cy: Cytoscape.Core,
    data: GraphData,
    expanded: Set<string>,
    options: SyncOptions,
) {
    const rankDir = options.rankDir ?? "TB";
    const target = buildElements(data, expanded);
    const { nodes, edges } = reprocessData(data, expanded);
    const anchorGroup = options.anchorGroup ?? null;
    const previousLayout = layoutStore.get(cy);
    const fromCenters = captureCenters(cy);
    const currentStructureKey = graphStructureKey(nodes, edges, expanded);
    const previousStructureKey = structureKeyStore.get(cy);

    const pipelineOrders = pipelineOrdersFor(data, expanded);

    if (options.mode === "fit" || cy.nodes().empty() || !previousLayout) {
        stopLayoutAnimations(cy);
        clearLayoutTimer(cy);
        applyElementDiff(cy, target);
        const nextLayout = buildCollapsedLayout(nodes, edges, expanded, rankDir, pipelineOrders);
        applyLayoutToCy(cy, nextLayout);
        layoutStore.set(cy, nextLayout);
        structureKeyStore.set(cy, currentStructureKey);
        fitGraphViewport(cy);
        options.onLayoutComplete?.();
        return;
    }

    // Periodic refresh: update node data only, keep layout positions intact.
    if (
        !anchorGroup &&
        previousStructureKey === currentStructureKey &&
        options.mode === "preserve"
    ) {
        stopLayoutAnimations(cy);
        applyElementDiff(cy, target);
        options.onLayoutComplete?.();
        return;
    }

    if (anchorGroup && previousLayout.has(anchorGroup)) {
        stopLayoutAnimations(cy);
        clearLayoutTimer(cy);

        const workingLayout = cloneLayout(previousLayout);

        if (options.expanding) {
            savePreExpandLayout(cy, anchorGroup, workingLayout);
            applyElementDiff(cy, target);
            const nextLayout = expandGroupInLayout(
                workingLayout,
                anchorGroup,
                nodes,
                edges,
                rankDir,
                pipelineOrders.get(anchorGroup) ?? [],
            );
            const innerIds = getInnerNodeIds(nodes, anchorGroup);
            layoutStore.set(cy, nextLayout);
            structureKeyStore.set(cy, currentStructureKey);
            animateLayoutTransition(cy, fromCenters, nextLayout, {
                fadeIn: innerIds,
                onComplete: () => scheduleLayoutComplete(cy, options),
            });
            return;
        }

        const innerIds = getInnerNodeIdsFromLayout(previousLayout, anchorGroup);
        const restored = takePreExpandLayout(cy, anchorGroup);
        const collapsedLayout = restored
            ?? collapseGroupInLayout(
                workingLayout,
                anchorGroup,
                nodes,
                edges,
                rankDir,
                innerIds,
            );

        layoutStore.set(cy, collapsedLayout);
        structureKeyStore.set(cy, currentStructureKey);
        animateLayoutTransition(cy, fromCenters, collapsedLayout, {
            fadeOut: innerIds,
            onComplete: () => {
                applyElementDiff(cy, target);
                cy.nodes().forEach((node) => setNodeVisualOpacity(cy, node, 1));
                scheduleLayoutComplete(cy, options);
            },
        });
        return;
    }

    stopLayoutAnimations(cy);
    clearLayoutTimer(cy);
    applyElementDiff(cy, target);
    const nextLayout = buildCollapsedLayout(nodes, edges, expanded, rankDir, pipelineOrders);
    layoutStore.set(cy, nextLayout);
    structureKeyStore.set(cy, currentStructureKey);
    animateLayoutTransition(cy, fromCenters, nextLayout, {
        onComplete: () => scheduleLayoutComplete(cy, options),
    });
}

export { buildElements };
