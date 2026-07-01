import Cytoscape from "cytoscape";
import {
    groupBoxSize,
    stepNodeSize,
    tableNodeSize,
} from "./graphNodeLayout";
import {
    animateNodeVisualOpacity,
    ensureGroupExpandedVisible,
    nodeUsesHtmlLabel,
    setNodeVisualOpacity,
    stopHtmlOpacityAnimations,
} from "./htmlLabelOpacity";
import { ANIMATION_EASING, ANIMATION_MS } from "./animationConstants";
import { animateEdgeOpacityTransitions, resetEdgeOpacities } from "./edgeTransition";

export type BBox = { x: number; y: number; w: number; h: number };

export type LayoutEdge = { source: string; target: string };

export type MeasuredNode = {
    id: string;
    type: string;
    name: string;
    w: number;
    h: number;
    parent?: string;
    metaGroup?: string;
    child_count?: number;
    indexes?: string[];
};

export type LayoutEntry = {
    bbox: BBox;
    node: MeasuredNode;
    visible: boolean;
};

export type GraphLayout = Map<string, LayoutEntry>;

const GROUP_PADDING = { top: 56, bottom: 44, left: 44, right: 44 };
const RANK_SEP = 68;
const NODE_SEP = 58;
function bboxFromCenter(cx: number, cy: number, w: number, h: number): BBox {
    return { x: cx - w / 2, y: cy - h / 2, w, h };
}

function bboxCenter(bbox: BBox): { x: number; y: number } {
    return { x: bbox.x + bbox.w / 2, y: bbox.y + bbox.h / 2 };
}

function topCenter(bbox: BBox): { x: number; y: number } {
    return { x: bbox.x + bbox.w / 2, y: bbox.y };
}

function placeByTopCenter(anchor: { x: number; y: number }, size: { w: number; h: number }): BBox {
    return { x: anchor.x - size.w / 2, y: anchor.y, w: size.w, h: size.h };
}

function bboxesOverlap(a: BBox, b: BBox): boolean {
    return a.x < b.x + b.w && a.x + a.w > b.x && a.y < b.y + b.h && a.y + a.h > b.y;
}

export function measureNode(data: Cytoscape.NodeDataDefinition): MeasuredNode {
    const id = (data.id as string) || (data.name as string);
    const name = (data.name as string) || id;
    const compact = Boolean(data.metaGroup);
    const type = data.type as string;

    if (type === "group") {
        const size = groupBoxSize(name, (data.child_count as number) ?? 1);
        return { id, type, name, w: size.w, h: size.h, child_count: data.child_count as number };
    }
    if (type === "group-expanded") {
        const w = (data.boxW as number) ?? groupBoxSize(name, (data.child_count as number) ?? 1).w;
        const h = (data.boxH as number) ?? groupBoxSize(name, (data.child_count as number) ?? 1).h;
        return { id, type, name, w, h, child_count: data.child_count as number };
    }
    if (type === "table") {
        const size = tableNodeSize(name, (data.indexes as string[]) || [], compact);
        return {
            id,
            type,
            name,
            w: size.w,
            h: size.h,
            parent: data.parent as string | undefined,
            metaGroup: data.metaGroup as string | undefined,
            indexes: data.indexes as string[] | undefined,
        };
    }
    const size = stepNodeSize(name, compact);
    return {
        id,
        type,
        name,
        w: size.w,
        h: size.h,
        parent: data.parent as string | undefined,
        metaGroup: data.metaGroup as string | undefined,
    };
}

function sortIds(ids: string[]): string[] {
    return [...ids].sort((a, b) => a.localeCompare(b));
}

/**
 * Deterministic layered DAG layout (top-to-bottom or left-to-right).
 * Same inputs always produce the same node positions.
 */
export function layoutLayeredDag(
    nodes: Map<string, MeasuredNode>,
    edges: LayoutEdge[],
    rankDir: "TB" | "LR" = "TB",
): Map<string, BBox> {
    const ids = sortIds(Array.from(nodes.keys()));
    if (!ids.length) return new Map();

    const rank = new Map<string, number>();
    ids.forEach((id) => rank.set(id, 0));
    let changed = true;
    let guard = 0;
    while (changed && guard < ids.length + 1) {
        changed = false;
        guard += 1;
        edges.forEach(({ source, target }) => {
            if (!nodes.has(source) || !nodes.has(target)) return;
            const nextRank = (rank.get(source) ?? 0) + 1;
            if (nextRank > (rank.get(target) ?? 0)) {
                rank.set(target, nextRank);
                changed = true;
            }
        });
    }

    const ranks = new Map<number, string[]>();
    ids.forEach((id) => {
        const r = rank.get(id) ?? 0;
        if (!ranks.has(r)) ranks.set(r, []);
        ranks.get(r)?.push(id);
    });
    ranks.forEach((rankIds, r) => ranks.set(r, sortIds(rankIds)));

    const positions = new Map<string, BBox>();
    const sortedRankKeys = sortIds(Array.from(ranks.keys()).map(String)).map(Number);

    if (rankDir === "TB") {
        let yCursor = 0;
        sortedRankKeys.forEach((r) => {
            const rankIds = ranks.get(r) ?? [];
            const widths = rankIds.map((id) => nodes.get(id)?.w ?? 0);
            const totalWidth =
                widths.reduce((sum, w) => sum + w, 0) + NODE_SEP * Math.max(0, rankIds.length - 1);
            let xCursor = -totalWidth / 2;
            rankIds.forEach((id, index) => {
                const node = nodes.get(id);
                if (!node) return;
                const cx = xCursor + node.w / 2;
                const cy = yCursor + node.h / 2;
                positions.set(id, bboxFromCenter(cx, cy, node.w, node.h));
                xCursor += node.w + (index < rankIds.length - 1 ? NODE_SEP : 0);
            });
            const rankHeight = Math.max(...rankIds.map((id) => nodes.get(id)?.h ?? 0));
            yCursor += rankHeight + RANK_SEP;
        });
    } else {
        let xCursor = 0;
        sortedRankKeys.forEach((r) => {
            const rankIds = ranks.get(r) ?? [];
            const heights = rankIds.map((id) => nodes.get(id)?.h ?? 0);
            const totalHeight =
                heights.reduce((sum, h) => sum + h, 0) + NODE_SEP * Math.max(0, rankIds.length - 1);
            let yCursor = -totalHeight / 2;
            rankIds.forEach((id, index) => {
                const node = nodes.get(id);
                if (!node) return;
                const cx = xCursor + node.w / 2;
                const cy = yCursor + node.h / 2;
                positions.set(id, bboxFromCenter(cx, cy, node.w, node.h));
                yCursor += node.h + (index < rankIds.length - 1 ? NODE_SEP : 0);
            });
            const rankWidth = Math.max(...rankIds.map((id) => nodes.get(id)?.w ?? 0));
            xCursor += rankWidth + RANK_SEP;
        });
    }

    return positions;
}

function innerGraphBBox(positions: Map<string, BBox>): BBox {
    let x1 = Infinity;
    let y1 = Infinity;
    let x2 = -Infinity;
    let y2 = -Infinity;
    positions.forEach((bbox) => {
        x1 = Math.min(x1, bbox.x);
        y1 = Math.min(y1, bbox.y);
        x2 = Math.max(x2, bbox.x + bbox.w);
        y2 = Math.max(y2, bbox.y + bbox.h);
    });
    if (!Number.isFinite(x1)) return { x: 0, y: 0, w: 0, h: 0 };
    return { x: x1, y: y1, w: x2 - x1, h: y2 - y1 };
}

function layoutVerticalStack(
    children: Map<string, MeasuredNode>,
    orderedIds: string[],
): Map<string, BBox> {
    const positions = new Map<string, BBox>();
    const order = orderedIds.filter((id) => children.has(id));
    if (!order.length) return positions;

    let yCursor = 0;
    let maxW = 0;
    order.forEach((id) => {
        maxW = Math.max(maxW, children.get(id)?.w ?? 0);
    });

    order.forEach((id) => {
        const node = children.get(id);
        if (!node) return;
        const cx = maxW / 2;
        const cy = yCursor + node.h / 2;
        positions.set(id, bboxFromCenter(cx, cy, node.w, node.h));
        yCursor += node.h + RANK_SEP;
    });
    return positions;
}

export function layoutInnerGraph(
    children: Map<string, MeasuredNode>,
    edges: LayoutEdge[],
    pipelineOrder: string[] = [],
): { positions: Map<string, BBox>; contentBBox: BBox } {
    const hasInternalEdges = edges.some(
        ({ source, target }) => children.has(source) && children.has(target),
    );
    const positions = hasInternalEdges
        ? layoutLayeredDag(children, edges, "TB")
        : layoutVerticalStack(children, pipelineOrder.length ? pipelineOrder : sortIds(Array.from(children.keys())));
    return { positions, contentBBox: innerGraphBBox(positions) };
}

function normalizeLayout(layout: GraphLayout, margin = 80): void {
    let minX = Infinity;
    let minY = Infinity;
    layout.forEach((entry) => {
        if (!entry.visible) return;
        minX = Math.min(minX, entry.bbox.x);
        minY = Math.min(minY, entry.bbox.y);
    });
    if (!Number.isFinite(minX) || !Number.isFinite(minY)) return;
    const shiftX = margin - minX;
    const shiftY = margin - minY;
    if (shiftX === 0 && shiftY === 0) return;
    layout.forEach((entry) => {
        if (!entry.visible) return;
        entry.bbox.x += shiftX;
        entry.bbox.y += shiftY;
    });
}

export function cloneLayout(layout: GraphLayout): GraphLayout {
    const next = new Map<string, LayoutEntry>();
    layout.forEach((entry, id) => {
        next.set(id, {
            bbox: { ...entry.bbox },
            node: { ...entry.node },
            visible: entry.visible,
        });
    });
    return next;
}

function getGroupMembers(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    groupId: string,
): Set<string> {
    const members = new Set<string>();
    nodes.forEach((data, id) => {
        if (data.metaGroup === groupId) members.add(id);
    });
    return members;
}

function edgesToList(edges: Iterable<Cytoscape.EdgeDataDefinition>): LayoutEdge[] {
    return Array.from(edges).map((edge) => ({
        source: edge.source as string,
        target: edge.target as string,
    }));
}

function getBoundaryNodes(
    members: Set<string>,
    edges: Iterable<Cytoscape.EdgeDataDefinition>,
): Set<string> {
    const boundary = new Set<string>();
    for (const edge of Array.from(edges)) {
        const source = edge.source as string;
        const target = edge.target as string;
        const sourceIn = members.has(source);
        const targetIn = members.has(target);
        if (sourceIn !== targetIn) {
            if (sourceIn) boundary.add(source);
            if (targetIn) boundary.add(target);
        }
    }
    return boundary;
}

function horizontalOverlap(a: BBox, b: BBox, margin = 0): boolean {
    return a.x - margin < b.x + b.w + margin && a.x + a.w + margin > b.x - margin;
}

function sortedExternalEntries(
    layout: GraphLayout,
    exclude: Set<string>,
): Array<[string, LayoutEntry]> {
    return Array.from(layout.entries())
        .filter(([id, entry]) => !exclude.has(id) && entry.visible)
        .sort(([, a], [, b]) => a.bbox.y - b.bbox.y || a.bbox.x - b.bbox.x);
}

function reassignExternalRowsOnExpand(
    layout: GraphLayout,
    oldGroupBBox: BBox,
    newGroupBBox: BBox,
    exclude: Set<string>,
): void {
    const startY = newGroupBBox.y + newGroupBBox.h + NODE_SEP;

    const snapshots = sortedExternalEntries(layout, exclude)
        .filter(([, entry]) => entry.bbox.y >= oldGroupBBox.y)
        .map(([, entry]) => ({ entry, origY: entry.bbox.y, origX: entry.bbox.x }));

    if (!snapshots.length) return;

    const rows: Array<typeof snapshots> = [];
    snapshots.forEach((snap) => {
        const row = rows.find((r) => Math.abs(r[0].origY - snap.origY) <= 32);
        if (row) row.push(snap);
        else rows.push([snap]);
    });
    rows.sort((a, b) => a[0].origY - b[0].origY);

    let yCursor = startY;
    rows.forEach((row) => {
        row.sort((a, b) => a.origX - b.origX);
        const rowHeight = Math.max(...row.map((s) => s.entry.bbox.h));
        row.forEach((snap) => {
            snap.entry.bbox.y = yCursor;
        });
        yCursor += rowHeight + RANK_SEP;
    });

    // Resolve horizontal crowding within each new row.
    rows.forEach((row) => {
        for (let i = 1; i < row.length; i += 1) {
            const prev = row[i - 1].entry.bbox;
            const curr = row[i].entry;
            const minX = prev.x + prev.w + NODE_SEP;
            if (curr.bbox.x < minX) {
                curr.bbox.x = minX;
            }
        }
    });
}

/**
 * Upfront layout for expand/collapse: top-center anchor stays fixed; external nodes
 * at/ below the group top move by the group size delta. On expand, rows below the
 * blue frame are repacked from the pre-expand row structure.
 */
function computeExternalPositionsAfterGroupResize(
    layout: GraphLayout,
    oldGroupBBox: BBox,
    newGroupBBox: BBox,
    exclude: Set<string>,
    rankDir: "TB" | "LR",
): void {
    if (rankDir === "TB") {
        const deltaH = newGroupBBox.h - oldGroupBBox.h;
        if (deltaH === 0) return;

        if (deltaH > 0) {
            reassignExternalRowsOnExpand(layout, oldGroupBBox, newGroupBBox, exclude);
            return;
        }

        // Collapse fallback (when pre-expand snapshot is unavailable).
        sortedExternalEntries(layout, exclude).forEach(([, entry]) => {
            if (entry.bbox.y >= newGroupBBox.y) {
                entry.bbox.y += deltaH;
            }
        });
        return;
    }

    const deltaW = newGroupBBox.w - oldGroupBBox.w;
    if (deltaW === 0) return;

    const floorX = newGroupBBox.x + newGroupBBox.w + NODE_SEP;
    sortedExternalEntries(layout, exclude).forEach(([, entry]) => {
        if (entry.bbox.x < oldGroupBBox.x) return;

        let x = entry.bbox.x + deltaW;
        const candidate: BBox = { ...entry.bbox, x };
        if (deltaW > 0 && bboxesOverlap(candidate, newGroupBBox)) {
            x = floorX;
        }
        entry.bbox.x = x;
    });
}

export function ensureLayoutCoversNodes(
    layout: GraphLayout,
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    cy?: Cytoscape.Core,
): void {
    nodes.forEach((data, id) => {
        if (layout.has(id)) return;
        if (data.metaGroup) return;
        if (data.type === "group-expanded") return;

        const measured = measureNode({ ...data, id });
        let bbox: BBox;
        if (cy) {
            const ele = cy.getElementById(id);
            if (!ele.empty()) {
                const center = (ele as Cytoscape.NodeSingular).position();
                bbox = bboxFromCenter(center.x, center.y, measured.w, measured.h);
            } else {
                bbox = bboxFromCenter(0, 0, measured.w, measured.h);
            }
        } else {
            bbox = bboxFromCenter(0, 0, measured.w, measured.h);
        }
        layout.set(id, { bbox, node: measured, visible: true });
    });
}

export function expandGroupInLayout(
    layout: GraphLayout,
    groupId: string,
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Iterable<Cytoscape.EdgeDataDefinition>,
    rankDir: "TB" | "LR",
    pipelineOrder: string[] = [],
): GraphLayout {
    const next = cloneLayout(layout);
    const groupEntry = next.get(groupId);
    if (!groupEntry) return next;

    const members = getGroupMembers(nodes, groupId);
    const boundary = getBoundaryNodes(members, edges);
    const innerIds = sortIds(
        Array.from(members).filter((id) => {
            const data = nodes.get(id);
            return data && !(data.type === "table" && boundary.has(id));
        }),
    );

    const innerNodes = new Map<string, MeasuredNode>();
    innerIds.forEach((id) => {
        const data = nodes.get(id);
        if (!data) return;
        innerNodes.set(id, measureNode({ ...data, id }));
    });

    const edgeList = edgesToList(edges);
    const innerEdges: LayoutEdge[] = edgeList.filter(
        ({ source, target }) => innerNodes.has(source) && innerNodes.has(target),
    );

    const { positions: innerPositions, contentBBox } = layoutInnerGraph(
        innerNodes,
        innerEdges,
        pipelineOrder.length ? pipelineOrder : innerIds,
    );
    const expandedSize = {
        w: contentBBox.w + GROUP_PADDING.left + GROUP_PADDING.right,
        h: contentBBox.h + GROUP_PADDING.top + GROUP_PADDING.bottom,
    };

    const anchor = topCenter(groupEntry.bbox);
    const oldBBox = { ...groupEntry.bbox };
    const groupBBox = placeByTopCenter(anchor, expandedSize);
    const innerOrigin = {
        x: groupBBox.x + GROUP_PADDING.left - contentBBox.x,
        y: groupBBox.y + GROUP_PADDING.top - contentBBox.y,
    };

    groupEntry.bbox = groupBBox;
    groupEntry.node = {
        ...groupEntry.node,
        type: "group-expanded",
        w: groupBBox.w,
        h: groupBBox.h,
    };
    groupEntry.visible = true;

    innerIds.forEach((id) => {
        const innerBBox = innerPositions.get(id);
        const measured = innerNodes.get(id);
        if (!innerBBox || !measured) return;
        next.set(id, {
            bbox: {
                x: innerOrigin.x + innerBBox.x,
                y: innerOrigin.y + innerBBox.y,
                w: innerBBox.w,
                h: innerBBox.h,
            },
            node: { ...measured, metaGroup: groupId },
            visible: true,
        });
    });

    const exclude = new Set<string>([groupId, ...innerIds]);
    computeExternalPositionsAfterGroupResize(next, oldBBox, groupBBox, exclude, rankDir);
    return next;
}

export function getInnerNodeIdsFromLayout(
    layout: GraphLayout,
    groupId: string,
): Set<string> {
    const ids = new Set<string>();
    layout.forEach((entry, id) => {
        if (entry.node.metaGroup === groupId) ids.add(id);
    });
    return ids;
}

export function collapseGroupInLayout(
    layout: GraphLayout,
    groupId: string,
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Iterable<Cytoscape.EdgeDataDefinition>,
    rankDir: "TB" | "LR",
    layoutInnerIds?: Set<string>,
): GraphLayout {
    const next = cloneLayout(layout);
    const groupEntry = next.get(groupId);
    if (!groupEntry) return next;

    const oldBBox = { ...groupEntry.bbox };
    const members = layoutInnerIds ?? getGroupMembers(nodes, groupId);
    const data = nodes.get(groupId);
    const collapsed = measureNode({
        ...(data ?? { type: "group", name: groupId }),
        type: "group",
        name: groupId,
        child_count: data?.child_count ?? members.size,
    });

    const anchor = topCenter(oldBBox);
    const collapsedBBox = placeByTopCenter(anchor, { w: collapsed.w, h: collapsed.h });

    members.forEach((id) => next.delete(id));

    groupEntry.bbox = collapsedBBox;
    groupEntry.node = collapsed;
    groupEntry.visible = true;

    const exclude = new Set<string>([groupId]);
    computeExternalPositionsAfterGroupResize(next, oldBBox, collapsedBBox, exclude, rankDir);
    return next;
}

export function buildCollapsedLayout(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Iterable<Cytoscape.EdgeDataDefinition>,
    expanded: Set<string>,
    rankDir: "TB" | "LR",
    pipelineOrders: Map<string, string[]> = new Map(),
): GraphLayout {
    const layout: GraphLayout = new Map();
    const edgeList = edgesToList(edges);
    const layoutNodes = new Map<string, MeasuredNode>();
    nodes.forEach((data, id) => {
        if (data.parent || data.metaGroup) return;
        if (data.type === "group-expanded") return;
        layoutNodes.set(id, measureNode({ ...data, id }));
    });

    const positions = layoutLayeredDag(layoutNodes, edgeList, rankDir);
    layoutNodes.forEach((node, id) => {
        const bbox = positions.get(id);
        if (!bbox) return;
        layout.set(id, { bbox, node, visible: true });
    });

    sortIds(Array.from(expanded)).forEach((groupId) => {
        if (!layout.has(groupId)) return;
        const expandedLayout = expandGroupInLayout(
            layout,
            groupId,
            nodes,
            edges,
            rankDir,
            pipelineOrders.get(groupId) ?? [],
        );
        expandedLayout.forEach((entry, id) => layout.set(id, entry));
    });

    normalizeLayout(layout);
    return layout;
}

export function syncLayoutFromCy(cy: Cytoscape.Core, layout: GraphLayout): GraphLayout {
    const synced = cloneLayout(layout);
    synced.forEach((entry, id) => {
        const ele = cy.getElementById(id);
        if (ele.empty()) return;
        const node = ele as Cytoscape.NodeSingular;
        const center = node.position();
        synced.set(id, {
            ...entry,
            bbox: bboxFromCenter(center.x, center.y, entry.bbox.w, entry.bbox.h),
        });
    });
    return synced;
}

export function layoutToCenters(layout: GraphLayout): Map<string, { x: number; y: number }> {
    const centers = new Map<string, { x: number; y: number }>();
    layout.forEach((entry, id) => {
        if (!entry.visible) return;
        centers.set(id, bboxCenter(entry.bbox));
    });
    return centers;
}

export function applyLayoutToCy(cy: Cytoscape.Core, layout: GraphLayout): void {
    cy.batch(() => {
        // Parent compound nodes first so child positions stay stable.
        const ordered = [
            ...Array.from(layout.entries()).filter(([, e]) => e.node.type === "group-expanded"),
            ...Array.from(layout.entries()).filter(([, e]) => e.node.type !== "group-expanded"),
        ];
        ordered.forEach(([id, entry]) => {
            if (!entry.visible) return;
            const ele = cy.getElementById(id);
            if (ele.empty()) return;
            const node = ele as Cytoscape.NodeSingular;
            const center = bboxCenter(entry.bbox);
            node.position(center);
            if (entry.node.type === "group" || entry.node.type === "group-expanded") {
                node.data({
                    boxW: entry.bbox.w,
                    boxH: entry.bbox.h,
                });
            }
            if (entry.node.type === "group-expanded") {
                ensureGroupExpandedVisible(node);
            }
        });
    });
}

export function stopLayoutAnimations(cy: Cytoscape.Core): void {
    if (cy.destroyed()) return;
    stopHtmlOpacityAnimations(cy);
    cy.stop(true, true);
}

export type LayoutTransitionOptions = {
    fadeIn?: Set<string>;
    fadeOut?: Set<string>;
    /** Animate width/height (e.g. group-expanded ↔ collapsed group). */
    morphBoxes?: Map<string, { from: BBox; to: BBox }>;
    /** Edge keys (`source->target`) to crossfade during the transition. */
    edgeFadeIn?: Set<string>;
    edgeFadeOut?: Set<string>;
    onComplete?: () => void;
};

export function animateLayoutTransition(
    cy: Cytoscape.Core,
    fromCenters: Map<string, { x: number; y: number }>,
    toLayout: GraphLayout,
    options?: LayoutTransitionOptions,
): void {
    stopLayoutAnimations(cy);

    const toCenters = layoutToCenters(toLayout);
    const fadeIn = options?.fadeIn ?? new Set<string>();
    const fadeOut = options?.fadeOut ?? new Set<string>();
    const morphBoxes = options?.morphBoxes ?? new Map<string, { from: BBox; to: BBox }>();

    fadeIn.forEach((id) => {
        const node = cy.getElementById(id);
        if (!node.empty()) {
            setNodeVisualOpacity(cy, node as Cytoscape.NodeSingular, 0);
        }
    });

    fadeOut.forEach((id) => {
        const node = cy.getElementById(id);
        if (node.empty()) return;
        const n = node as Cytoscape.NodeSingular;
        if (nodeUsesHtmlLabel(n)) {
            setNodeVisualOpacity(cy, n, 1);
        }
    });

    applyLayoutToCy(cy, toLayout);

    type AnimSpec = {
        id: string;
        target?: { x: number; y: number };
        fadeIn: boolean;
        fadeOut: boolean;
        move: boolean;
        morph?: { from: BBox; to: BBox };
    };

    const specs: AnimSpec[] = [];

    toCenters.forEach((target, id) => {
        const from = fromCenters.get(id);
        const isFadeIn = fadeIn.has(id);
        const isFadeOut = fadeOut.has(id);
        const morph = morphBoxes.get(id);
        const move = Boolean(from) || Boolean(morph);
        if (!move && !isFadeIn && !isFadeOut) {
            return;
        }
        specs.push({ id, target, fadeIn: isFadeIn, fadeOut: isFadeOut, move, morph });
    });

    morphBoxes.forEach((morph, id) => {
        if (specs.some((spec) => spec.id === id)) return;
        const target = toCenters.get(id);
        if (!target) return;
        specs.push({
            id,
            target,
            fadeIn: fadeIn.has(id),
            fadeOut: fadeOut.has(id),
            move: true,
            morph,
        });
    });

    fadeOut.forEach((id) => {
        if (toCenters.has(id)) return;
        specs.push({ id, fadeIn: false, fadeOut: true, move: false });
    });

    const edgeFadeIn = options?.edgeFadeIn ?? new Set<string>();
    const edgeFadeOut = options?.edgeFadeOut ?? new Set<string>();
    const totalWork = specs.length + edgeFadeIn.size + edgeFadeOut.size;

    if (!totalWork) {
        options?.onComplete?.();
        return;
    }

    let completed = 0;
    const finishOne = () => {
        completed += 1;
        if (completed >= totalWork) {
            cy.nodes().forEach((node) => {
                const n = node as Cytoscape.NodeSingular;
                if (n.data("type") === "group-expanded") {
                    ensureGroupExpandedVisible(n);
                } else if (nodeUsesHtmlLabel(n)) {
                    const targetOpacity = fadeOut.has(node.id()) ? 0 : 1;
                    setNodeVisualOpacity(cy, n, targetOpacity);
                }
            });
            resetEdgeOpacities(cy);
            options?.onComplete?.();
        }
    };

    animateEdgeOpacityTransitions(cy, edgeFadeIn, edgeFadeOut, finishOne);

    if (!specs.length) {
        return;
    }

    specs.forEach(({ id, target, fadeIn: isFadeIn, fadeOut: isFadeOut, move, morph }) => {
        const node = cy.getElementById(id);
        if (node.empty()) {
            finishOne();
            return;
        }

        const nodeEl = node as Cytoscape.NodeSingular;
        const from = fromCenters.get(id);

        if (morph && target) {
            const fromCenter = bboxCenter(morph.from);
            const shrinking = morph.to.w * morph.to.h < morph.from.w * morph.from.h;
            const isNativeGroupFrame = nodeEl.data("type") === "group-expanded";
            nodeEl.position(fromCenter);
            nodeEl.style("width", morph.from.w);
            nodeEl.style("height", morph.from.h);
            nodeEl.data("boxW", morph.from.w);
            nodeEl.data("boxH", morph.from.h);
            if (shrinking && isNativeGroupFrame) {
                nodeEl.style("opacity", 1);
            } else if (isNativeGroupFrame) {
                ensureGroupExpandedVisible(nodeEl);
            }
        } else if (from) {
            nodeEl.position(from);
        } else if (target) {
            nodeEl.position(target);
        }

        const fromOpacity = isFadeIn ? 0 : 1;
        const toOpacity = isFadeOut ? 0 : 1;
        const isMorphingNativeGroup = Boolean(morph && nodeEl.data("type") === "group-expanded");
        if (!isMorphingNativeGroup) {
            setNodeVisualOpacity(cy, nodeEl, fromOpacity);
        }

        let positionDone = !move || !target;
        let opacityDone = !isFadeIn && !isFadeOut;

        const maybeFinish = () => {
            if (positionDone && opacityDone) {
                finishOne();
            }
        };

        if (morph && target) {
            const shrinking = morph.to.w * morph.to.h < morph.from.w * morph.from.h;
            const isNativeGroupFrame = nodeEl.data("type") === "group-expanded";
            const morphStyle: Record<string, number> = {
                width: morph.to.w,
                height: morph.to.h,
            };
            if (shrinking && isNativeGroupFrame) {
                morphStyle.opacity = 0;
            }

            nodeEl.animate(
                {
                    position: target,
                    style: morphStyle,
                },
                {
                    duration: ANIMATION_MS,
                    easing: ANIMATION_EASING,
                    complete: () => {
                        nodeEl.removeStyle("width");
                        nodeEl.removeStyle("height");
                        nodeEl.data("boxW", morph.to.w);
                        nodeEl.data("boxH", morph.to.h);
                        if (shrinking && isNativeGroupFrame) {
                            nodeEl.style("opacity", 0);
                        } else if (isNativeGroupFrame) {
                            ensureGroupExpandedVisible(nodeEl);
                        }
                        positionDone = true;
                        maybeFinish();
                    },
                },
            );
        } else if (move && target) {
            nodeEl.animate(
                { position: target },
                {
                    duration: ANIMATION_MS,
                    easing: ANIMATION_EASING,
                    complete: () => {
                        positionDone = true;
                        maybeFinish();
                    },
                },
            );
        }

        if (isFadeIn || isFadeOut) {
            animateNodeVisualOpacity(cy, id, fromOpacity, toOpacity, ANIMATION_MS, () => {
                opacityDone = true;
                maybeFinish();
            });
        } else if (move && !isMorphingNativeGroup) {
            setNodeVisualOpacity(cy, nodeEl, 1);
        }
    });
}

export function fitGraphViewport(cy: Cytoscape.Core): void {
    cy.fit(cy.elements(), 60);
    const fitZoom = cy.zoom();
    cy.minZoom(Math.min(0.05, fitZoom * 0.3));
    cy.maxZoom(Math.max(2.5, fitZoom * 6));

    const READABLE_ZOOM = 0.4;
    if (fitZoom < READABLE_ZOOM) {
        const bb = cy.elements().boundingBox();
        cy.zoom(READABLE_ZOOM);
        cy.pan({
            x: cy.width() / 2 - READABLE_ZOOM * (bb.x1 + bb.w / 2),
            y: 90 - READABLE_ZOOM * bb.y1,
        });
    }
}

export { ANIMATION_MS, ANIMATION_EASING } from "./animationConstants";
export { GROUP_PADDING };
