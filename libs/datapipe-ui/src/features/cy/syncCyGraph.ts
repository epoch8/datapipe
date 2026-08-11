import Cytoscape from "cytoscape";
import { GraphData, MetaNode } from "../../types";
import {
    COLLAPSE_GROUP_FADE_DELAY,
    COLLAPSE_GROUP_FADE_MS,
    COLLAPSE_INNER_FADE_MS,
} from "./animationConstants";
import {
    ANIMATION_MS,
    animateFitViewport,
    animateLayoutTransition,
    applyLayoutToCy,
    BBox,
    buildCollapsedLayout,
    cloneLayout,
    collapseGroupInLayout,
    expandGroupInLayout,
    fitGraphViewport,
    getInnerNodeIdsFromLayout,
    GraphLayout,
    pinLayoutAnchorCenter,
    stopLayoutAnimations,
} from "./incrementalLayout";
import { setNodeVisualOpacity, ensureGroupExpandedVisible } from "./htmlLabelOpacity";
import { buildLabelColumnLayout } from "./columnLayout";
import { setSnakeRowOverlayLayout } from "./snakeRowOverlay";
import {
    addEdgesFromTarget,
    computeEdgeDiff,
    makeEdgeKey,
    resetEdgeOpacities,
} from "./edgeTransition";
import { reprocessData } from "./process";
import {
    flowLayoutRankDir,
    flowLayoutWrapRows,
    flowLayoutPreferChronology,
    resolveFlowLayout,
} from "../../types/pipelineGraph";
import type { GraphFlowLayout } from "../../types/pipelineGraph";

export type CyElement = Cytoscape.ElementDefinition;

export type SyncMode = "fit" | "preserve";

export type SyncOptions = {
    mode: SyncMode;
    rankDir?: "TB" | "LR";
    /** When set, overrides rankDir and enables/disables snake wrap. */
    flowLayout?: GraphFlowLayout;
    layoutMode?: "dag" | "columns";
    labelKey?: string;
    labelOrder?: string[];
    /** Maps nested label values to top-level column keys. */
    labelColumnMap?: Map<string, string>;
    anchorGroup?: string | null;
    expanding?: boolean;
    onLayoutComplete?: () => void;
};

const layoutStore = new WeakMap<Cytoscape.Core, GraphLayout>();
const layoutTimerStore = new WeakMap<Cytoscape.Core, number>();
const preExpandStore = new WeakMap<Cytoscape.Core, Map<string, GraphLayout>>();
const structureKeyStore = new WeakMap<Cytoscape.Core, string>();
const layoutConfigKeyStore = new WeakMap<Cytoscape.Core, string>();

function buildElements(
    data: GraphData,
    expanded: Set<string>,
    layoutMode: "dag" | "columns",
): CyElement[] {
    const { nodes, edges } = reprocessData(data, expanded);
    const elements: CyElement[] = Array.from(nodes.entries())
        .sort(([, a], [, b]) => {
            const aParent = a.type === "group-expanded" ? 0 : 1;
            const bParent = b.type === "group-expanded" ? 0 : 1;
            return aParent - bParent;
        })
        .map(([nodeId, options]) => ({
            // Selection is driven only by our DOM/gesture handlers. Keeping
            // nodes unselectable stops Cytoscape's native tap from racing
            // unselect↔select with those handlers (which flashes focus styles).
            selectable: false,
            grabbable: options.type !== "group" && options.type !== "group-expanded",
            data: {
                id: nodeId,
                label: options.name || nodeId,
                ...options,
            },
        }));
    edges.forEach((edge) => {
        elements.push({
            grabbable: false,
            data: {
                ...edge,
                layoutMode,
            },
        });
    });
    return elements;
}

function edgeKey(source: string, target: string): string {
    return makeEdgeKey(source, target);
}

function applyNodeDiff(cy: Cytoscape.Core, target: CyElement[], removeAbsent = true) {
    const targetNodes = target.filter((el) => el.data.id);
    const targetNodeIds = new Set(targetNodes.map((el) => el.data.id as string));

    cy.batch(() => {
        if (removeAbsent) {
            cy.nodes().forEach((node) => {
                if (!targetNodeIds.has(node.id())) {
                    node.remove();
                }
            });
        }

        targetNodes.forEach((el) => {
            const id = el.data.id as string;
            const existing = cy.getElementById(id);
            if (existing.nonempty()) {
                const node = existing as unknown as Cytoscape.NodeSingular;
                const nextParent = (el.data.parent as string) ?? null;
                const currentParent = node.isChild() ? node.parent().first().id() : null;
                node.data(el.data);
                // Cytoscape data() merges; fields absent from the next payload (e.g. metaGroup
                // after collapse) would otherwise stick and break expand/collapse membership.
                if (!("metaGroup" in el.data) && node.data("metaGroup") != null) {
                    node.removeData("metaGroup");
                }
                if (!("frameLabel" in el.data) && node.data("frameLabel") != null) {
                    node.removeData("frameLabel");
                }
                // Always keep nodes unselectable so only app gestures select.
                if (node.selectable()) node.unselectify();
                if (nextParent !== currentParent) {
                    node.move({ parent: nextParent });
                }
            } else {
                cy.add(el);
            }
        });
    });
}

function applyEdgeDiff(cy: Cytoscape.Core, target: CyElement[]) {
    const targetEdges = target.filter((el) => el.data.source && el.data.target);
    const targetEdgeKeys = new Set(
        targetEdges.map((el) => edgeKey(el.data.source as string, el.data.target as string)),
    );

    cy.batch(() => {
        cy.edges().forEach((edge) => {
            const key = edgeKey(edge.source().id(), edge.target().id());
            if (!targetEdgeKeys.has(key)) {
                edge.remove();
            }
        });

        targetEdges.forEach((el) => {
            const key = edgeKey(el.data.source as string, el.data.target as string);
            const found = cy.edges().filter(
                (edge) => edgeKey(edge.source().id(), edge.target().id()) === key,
            );
            if (found.empty()) {
                cy.add(el);
            } else {
                found.forEach((edge) => {
                    edge.data(el.data);
                });
            }
        });
    });
}

function applyElementDiff(cy: Cytoscape.Core, target: CyElement[]) {
    applyNodeDiff(cy, target, true);
    applyEdgeDiff(cy, target);
}

function captureCenters(cy: Cytoscape.Core): Map<string, { x: number; y: number }> {
    const centers = new Map<string, { x: number; y: number }>();
    cy.nodes().forEach((node) => {
        centers.set(node.id(), { ...node.position() });
    });
    return centers;
}

function getMetaPipelineOrder(data: GraphData, groupId: string): string[] {
    const meta = findMetaNodeInData(data, groupId);
    if (!meta) return [];
    return meta.graph.pipeline.filter((step) => step.type !== "meta").map((step) => step.name);
}

function findMetaNodeInData(data: GraphData, groupId: string): MetaNode | undefined {
    const sep = groupId.lastIndexOf("__");
    const orderKey =
        sep > 0 && /^\d{4}(?:\.\d{4})*$/.test(groupId.slice(sep + 2))
            ? groupId.slice(sep + 2)
            : null;
    const name = orderKey ? groupId.slice(0, sep) : groupId;

    const walk = (
        pipeline: GraphData["pipeline"],
        parentKey = "",
    ): MetaNode | undefined => {
        for (let index = 0; index < pipeline.length; index += 1) {
            const pipe = pipeline[index];
            if (pipe.type !== "meta") continue;
            const key = parentKey
                ? `${parentKey}.${String(index).padStart(4, "0")}`
                : String(index).padStart(4, "0");
            if (orderKey) {
                if (pipe.name === name && key === orderKey) return pipe;
            } else if (pipe.name === name) {
                return pipe;
            }
            const nested = walk(pipe.graph.pipeline, key);
            if (nested) return nested;
        }
        return undefined;
    };
    return walk(data.pipeline);
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

function expandedGroupsInLayout(layout: GraphLayout): Set<string> {
    const ids = new Set<string>();
    layout.forEach((entry, id) => {
        if (entry.node.type === "group-expanded" && entry.visible) {
            ids.add(id);
        }
    });
    return ids;
}

function sameIdSet(a: Set<string>, b: Set<string>): boolean {
    if (a.size !== b.size) return false;
    for (const id of Array.from(a)) {
        if (!b.has(id)) return false;
    }
    return true;
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

function commitLayout(
    cy: Cytoscape.Core,
    layout: GraphLayout,
    wrapRows: boolean,
): void {
    layoutStore.set(cy, layout);
    setSnakeRowOverlayLayout(cy, layout, wrapRows);
}

/**
 * Sync graph elements and apply layout.
 * Columns mode always rebuilds the full layered DAG (stable across any
 * expand/collapse order). Dag mode uses incremental expand/collapse morphs.
 */
export function syncCyGraph(
    cy: Cytoscape.Core,
    data: GraphData,
    expanded: Set<string>,
    options: SyncOptions,
) {
    const flowLayout = resolveFlowLayout(options.flowLayout, options.rankDir);
    const rankDir = flowLayoutRankDir(flowLayout);
    const wrapRows = flowLayoutWrapRows(flowLayout);
    const preferChronology = flowLayoutPreferChronology(flowLayout);
    const target = buildElements(data, expanded, options.layoutMode ?? "dag");
    const { nodes, edges } = reprocessData(data, expanded);
    const anchorGroup = options.anchorGroup ?? null;
    const previousLayout = layoutStore.get(cy);
    const fromCenters = captureCenters(cy);
    const currentStructureKey = graphStructureKey(nodes, edges, expanded);
    const previousStructureKey = structureKeyStore.get(cy);
    const currentLayoutConfigKey = JSON.stringify({
        layoutMode: options.layoutMode ?? "dag",
        flowLayout,
        rankDir,
        wrapRows,
        preferChronology,
        labelKey: options.labelKey ?? "stage",
        labelOrder: options.labelOrder ?? [],
        labelColumnMap: Array.from((options.labelColumnMap ?? new Map()).entries()).sort(),
    });
    const previousLayoutConfigKey = layoutConfigKeyStore.get(cy);

    const pipelineOrders = pipelineOrdersFor(data, expanded);
    const buildLayout = (): GraphLayout =>
        options.layoutMode === "columns"
            ? buildLabelColumnLayout(
                  nodes,
                  edges,
                  expanded,
                  rankDir,
                  options.labelKey ?? "stage",
                  options.labelOrder ?? [],
                  pipelineOrders,
                  options.labelColumnMap ?? new Map(),
                  wrapRows,
                  preferChronology,
              )
            : buildCollapsedLayout(
                  nodes,
                  edges,
                  expanded,
                  rankDir,
                  pipelineOrders,
                  wrapRows,
                  preferChronology,
              );

    const isInitial = cy.nodes().empty() || !previousLayout;

    if (isInitial) {
        // First render (nothing on screen to animate from): place + fit instantly.
        stopLayoutAnimations(cy);
        clearLayoutTimer(cy);
        applyElementDiff(cy, target);
        const nextLayout = buildLayout();
        applyLayoutToCy(cy, nextLayout);
        commitLayout(cy, nextLayout, wrapRows);
        structureKeyStore.set(cy, currentStructureKey);
        layoutConfigKeyStore.set(cy, currentLayoutConfigKey);
        // Sync cached viewport dimensions before fitting: on first paint the flex
        // container may not have reached its final width yet, and a stale
        // cy.width() makes the centering pan land off to one side.
        cy.resize();
        fitGraphViewport(cy);
        options.onLayoutComplete?.();
        return;
    }

    if (options.mode === "fit" && !anchorGroup) {
        // Stage/graph switch with an existing graph on screen: animate node
        // positions to the new layout while the camera pans/zooms to the new fit
        // *in parallel*. Previously we snapped the camera (fitGraphViewport) and
        // only then animated the graph, which read as a jarring two-step jump.
        // Skip when expanding/collapsing a blue meta — that must stay pinned in place.
        stopLayoutAnimations(cy);
        clearLayoutTimer(cy);
        applyElementDiff(cy, target);
        const nextLayout = buildLayout();
        commitLayout(cy, nextLayout, wrapRows);
        structureKeyStore.set(cy, currentStructureKey);
        layoutConfigKeyStore.set(cy, currentLayoutConfigKey);

        // Nodes that only exist in the new graph fade in at their target spot.
        const fadeIn = new Set<string>();
        nextLayout.forEach((entry, id) => {
            if (entry.visible && !fromCenters.has(id)) fadeIn.add(id);
        });

        animateLayoutTransition(cy, fromCenters, nextLayout, {
            fadeIn,
            onComplete: () => scheduleLayoutComplete(cy, options),
        });
        // Keep cached dimensions fresh so the fit target is centered on the real
        // (possibly just-resized) container width, not a stale one.
        cy.resize();
        // Start the viewport tween *after* animateLayoutTransition (its internal
        // cy.stop would otherwise cancel a camera animation started earlier).
        animateFitViewport(cy, nextLayout, ANIMATION_MS);
        return;
    }

    // Periodic refresh: update node data only, keep layout positions intact.
    if (
        !anchorGroup &&
        previousStructureKey === currentStructureKey &&
        previousLayoutConfigKey === currentLayoutConfigKey &&
        options.mode === "preserve"
    ) {
        stopLayoutAnimations(cy);
        applyElementDiff(cy, target);
        options.onLayoutComplete?.();
        return;
    }

    // Columns mode.
    // - Expand/collapse of a blue meta: full layered rebuild (stable for large
    //   metas like Train_*), then pin the anchor center so the card stays under
    //   the cursor. Incremental expandGroupInLayout neighbor-push turns big
    //   expands into chaos.
    // - Anything else: full layered-DAG rebuild (stable across layout-mode changes).
    if (options.layoutMode === "columns") {
        stopLayoutAnimations(cy);
        clearLayoutTimer(cy);

        if (anchorGroup && previousLayout?.has(anchorGroup)) {
            const edgeDiff = computeEdgeDiff(cy, target);
            const morphBoxes = new Map<string, { from: BBox; to: BBox }>();
            const fromEntry = previousLayout.get(anchorGroup);
            // Pin to the live cy center (what the user is looking at), not a
            // possibly-stale layout-store bbox — otherwise the meta flies off-graph.
            const cyAnchor = cy.getElementById(anchorGroup);
            const pinCenter = !cyAnchor.empty()
                ? {
                      x: (cyAnchor as Cytoscape.NodeSingular).position("x"),
                      y: (cyAnchor as Cytoscape.NodeSingular).position("y"),
                  }
                : fromEntry
                  ? {
                        x: fromEntry.bbox.x + fromEntry.bbox.w / 2,
                        y: fromEntry.bbox.y + fromEntry.bbox.h / 2,
                    }
                  : null;
            const fromBBox =
                !cyAnchor.empty() && pinCenter
                    ? {
                          x: pinCenter.x - (cyAnchor as Cytoscape.NodeSingular).width() / 2,
                          y: pinCenter.y - (cyAnchor as Cytoscape.NodeSingular).height() / 2,
                          w: (cyAnchor as Cytoscape.NodeSingular).width(),
                          h: (cyAnchor as Cytoscape.NodeSingular).height(),
                      }
                    : fromEntry
                      ? { ...fromEntry.bbox }
                      : null;

            if (options.expanding) {
                applyNodeDiff(cy, target, false);
                const expandedGroup = cy.getElementById(anchorGroup);
                if (!expandedGroup.empty()) {
                    ensureGroupExpandedVisible(expandedGroup as Cytoscape.NodeSingular);
                }
                addEdgesFromTarget(cy, target, new Set(edgeDiff.toAdd), 0);

                const nextLayout = buildLayout();
                if (pinCenter) pinLayoutAnchorCenter(nextLayout, anchorGroup, pinCenter);
                const toEntry = nextLayout.get(anchorGroup);
                if (fromBBox && toEntry) {
                    morphBoxes.set(anchorGroup, { from: fromBBox, to: { ...toEntry.bbox } });
                }

                commitLayout(cy, nextLayout, wrapRows);
                structureKeyStore.set(cy, currentStructureKey);
                layoutConfigKeyStore.set(cy, currentLayoutConfigKey);

                const innerIds = getInnerNodeIds(nodes, anchorGroup);
                animateLayoutTransition(cy, fromCenters, nextLayout, {
                    fadeIn: innerIds,
                    morphBoxes,
                    edgeFadeIn: new Set(edgeDiff.toAdd),
                    edgeFadeOut: new Set(edgeDiff.toRemove),
                    onComplete: () => {
                        applyLayoutToCy(cy, nextLayout);
                        applyElementDiff(cy, target);
                        cy.batch(() => {
                            cy.nodes().forEach((nodeEle) => {
                                const node = nodeEle as Cytoscape.NodeSingular;
                                const entry = nextLayout.get(node.id());
                                if (!entry?.visible) return;
                                if (node.data("type") === "group-expanded") {
                                    ensureGroupExpandedVisible(node);
                                } else {
                                    setNodeVisualOpacity(cy, node, 1);
                                }
                            });
                        });
                        resetEdgeOpacities(cy);
                        scheduleLayoutComplete(cy, options);
                    },
                });
                return;
            }

            // Collapse: full rebuild + pin, morph frame down, fade out inners.
            const innerIds = getInnerNodeIdsFromLayout(previousLayout, anchorGroup);
            getInnerNodeIds(nodes, anchorGroup).forEach((id) => {
                if (previousLayout.get(id)?.node.metaGroup === anchorGroup) {
                    innerIds.add(id);
                }
            });

            const nextLayout = buildLayout();
            if (pinCenter) pinLayoutAnchorCenter(nextLayout, anchorGroup, pinCenter);
            const toEntry = nextLayout.get(anchorGroup);
            if (fromBBox && toEntry) {
                morphBoxes.set(anchorGroup, { from: fromBBox, to: { ...toEntry.bbox } });
            }

            commitLayout(cy, nextLayout, wrapRows);
            structureKeyStore.set(cy, currentStructureKey);
            layoutConfigKeyStore.set(cy, currentLayoutConfigKey);

            // Keep the native blue frame visible while it morphs down; swap to the
            // HTML collapsed card in onComplete. Hiding it here killed the collapse anim.
            const groupEle = cy.getElementById(anchorGroup);
            if (!groupEle.empty()) {
                const groupNode = groupEle as Cytoscape.NodeSingular;
                groupNode.removeStyle("width");
                groupNode.removeStyle("height");
                if (groupNode.data("type") === "group-expanded") {
                    ensureGroupExpandedVisible(groupNode);
                }
            }

            animateLayoutTransition(cy, fromCenters, nextLayout, {
                fadeOut: innerIds,
                morphBoxes,
                edgeFadeIn: new Set(edgeDiff.toAdd),
                edgeFadeOut: new Set(edgeDiff.toRemove),
                onComplete: () => {
                    applyElementDiff(cy, target);
                    applyLayoutToCy(cy, nextLayout);
                    cy.batch(() => {
                        cy.nodes().forEach((nodeEle) => {
                            const node = nodeEle as Cytoscape.NodeSingular;
                            const entry = nextLayout.get(node.id());
                            if (!entry?.visible) return;
                            setNodeVisualOpacity(cy, node, 1);
                        });
                    });
                    resetEdgeOpacities(cy);
                    scheduleLayoutComplete(cy, options);
                },
            });
            return;
        }

        // Full rebuild (layout mode change, initial columns sync without anchor, etc.).
        const nextLayout = buildLayout();
        commitLayout(cy, nextLayout, wrapRows);
        structureKeyStore.set(cy, currentStructureKey);
        layoutConfigKeyStore.set(cy, currentLayoutConfigKey);

        applyElementDiff(cy, target);
        applyLayoutToCy(cy, nextLayout);
        cy.batch(() => {
            cy.nodes().forEach((nodeEle) => {
                const node = nodeEle as Cytoscape.NodeSingular;
                const entry = nextLayout.get(node.id());
                if (!entry?.visible) return;
                if (node.data("type") === "group-expanded") {
                    ensureGroupExpandedVisible(node);
                } else {
                    setNodeVisualOpacity(cy, node, 1);
                }
            });
        });
        resetEdgeOpacities(cy);
        scheduleLayoutComplete(cy, options);
        return;
    }

    // Expand/collapse (dag mode): incremental morph along rankDir.
    const expandRankDir = rankDir;

    if (anchorGroup && previousLayout.has(anchorGroup)) {
        stopLayoutAnimations(cy);
        clearLayoutTimer(cy);

        const workingLayout = cloneLayout(previousLayout);

        if (options.expanding) {
            savePreExpandLayout(cy, anchorGroup, workingLayout);
            const edgeDiff = computeEdgeDiff(cy, target);
            applyNodeDiff(cy, target, false);
            const expandedGroup = cy.getElementById(anchorGroup);
            if (!expandedGroup.empty()) {
                ensureGroupExpandedVisible(expandedGroup as Cytoscape.NodeSingular);
            }
            addEdgesFromTarget(cy, target, new Set(edgeDiff.toAdd), 0);
            const fromEntry = workingLayout.get(anchorGroup);
            const fromBBox = fromEntry ? { ...fromEntry.bbox } : null;
            const nextLayout = expandGroupInLayout(
                workingLayout,
                anchorGroup,
                nodes,
                edges,
                expandRankDir,
                pipelineOrders.get(anchorGroup) ?? [],
                wrapRows,
                preferChronology,
            );
            const innerIds = getInnerNodeIds(nodes, anchorGroup);
            commitLayout(cy, nextLayout, wrapRows);
            structureKeyStore.set(cy, currentStructureKey);
            layoutConfigKeyStore.set(cy, currentLayoutConfigKey);
            const morphBoxes = new Map<string, { from: BBox; to: BBox }>();
            const toEntry = nextLayout.get(anchorGroup);
            if (fromBBox && toEntry) {
                morphBoxes.set(anchorGroup, {
                    from: fromBBox,
                    to: { ...toEntry.bbox },
                });
            }
            animateLayoutTransition(cy, fromCenters, nextLayout, {
                fadeIn: innerIds,
                morphBoxes,
                edgeFadeIn: new Set(edgeDiff.toAdd),
                edgeFadeOut: new Set(edgeDiff.toRemove),
                onComplete: () => {
                    applyLayoutToCy(cy, nextLayout);
                    scheduleLayoutComplete(cy, options);
                },
            });
            return;
        }

        const innerIds = getInnerNodeIdsFromLayout(previousLayout, anchorGroup);
        getInnerNodeIds(nodes, anchorGroup).forEach((id) => {
            if (previousLayout.get(id)?.node.metaGroup === anchorGroup) {
                innerIds.add(id);
            }
        });
        const restored = takePreExpandLayout(cy, anchorGroup);
        // Only restore a pre-expand snapshot when it already matches the remaining
        // expanded set. Expanding B after A makes A's snapshot stale (B missing).
        const restoredUsable =
            restored && sameIdSet(expandedGroupsInLayout(restored), expanded)
                ? restored
                : null;
        const collapsedLayout = restoredUsable
            ?? collapseGroupInLayout(
                workingLayout,
                anchorGroup,
                nodes,
                edges,
                expandRankDir,
                innerIds,
            );

        commitLayout(cy, collapsedLayout, wrapRows);
        structureKeyStore.set(cy, currentStructureKey);
        layoutConfigKeyStore.set(cy, currentLayoutConfigKey);
        const edgeDiff = computeEdgeDiff(cy, target);
        addEdgesFromTarget(cy, target, new Set(edgeDiff.toAdd), 0);

        // Switch to collapsed HTML group immediately (opacity 0) so it can crossfade
        // with inner sub-steps instead of appearing after the blue frame disappears.
        applyNodeDiff(cy, target, false);
        const groupEle = cy.getElementById(anchorGroup);
        if (!groupEle.empty()) {
            const groupNode = groupEle as Cytoscape.NodeSingular;
            // Clear native-frame size bypasses left by expand morph before HTML morph.
            groupNode.removeStyle("width");
            groupNode.removeStyle("height");
            setNodeVisualOpacity(cy, groupNode, 0);
            groupNode.data(
                "labelRefresh",
                ((groupNode.data("labelRefresh") as number) ?? 0) + 1,
            );
        }

        const morphBoxes = new Map<string, { from: BBox; to: BBox }>();
        const fromEntry = previousLayout.get(anchorGroup);
        const toEntry = collapsedLayout.get(anchorGroup);
        if (fromEntry && toEntry) {
            morphBoxes.set(anchorGroup, {
                from: { ...fromEntry.bbox },
                to: { ...toEntry.bbox },
            });
        }

        animateLayoutTransition(cy, fromCenters, collapsedLayout, {
            fadeOut: innerIds,
            fadeIn: new Set([anchorGroup]),
            fadeOutTiming: { duration: COLLAPSE_INNER_FADE_MS },
            fadeInTiming: { delay: COLLAPSE_GROUP_FADE_DELAY, duration: COLLAPSE_GROUP_FADE_MS },
            morphBoxes,
            edgeFadeIn: new Set(edgeDiff.toAdd),
            edgeFadeOut: new Set(edgeDiff.toRemove),
            onComplete: () => {
                applyElementDiff(cy, target);
                applyLayoutToCy(cy, collapsedLayout);
                scheduleLayoutComplete(cy, options);
            },
        });
        return;
    }

    stopLayoutAnimations(cy);
    clearLayoutTimer(cy);
    applyElementDiff(cy, target);
    const nextLayout = buildLayout();
    commitLayout(cy, nextLayout, wrapRows);
    structureKeyStore.set(cy, currentStructureKey);
    layoutConfigKeyStore.set(cy, currentLayoutConfigKey);
    animateLayoutTransition(cy, fromCenters, nextLayout, {
        onComplete: () => scheduleLayoutComplete(cy, options),
    });
}

export { buildElements };
