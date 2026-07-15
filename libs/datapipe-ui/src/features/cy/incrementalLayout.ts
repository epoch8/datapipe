import Cytoscape from "cytoscape";
import {
    groupBoxSize,
    stepNodeSize,
    tableNodeSize,
} from "./graphNodeLayout";
import { getTransformPrimaryKeys } from "./nodeKeyChips";
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

export type LayoutEdge = {
    source: string;
    target: string;
    sequential?: boolean;
    synthetic?: boolean;
};

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
    pipelineIndex?: number;
    pipelineOrderKey?: string;
};

export type LayoutEntry = {
    bbox: BBox;
    node: MeasuredNode;
    visible: boolean;
};

export type GraphLayout = Map<string, LayoutEntry>;

const GROUP_PADDING = { top: 56, bottom: 44, left: 44, right: 44 };
const RANK_SEP = 48;
const NODE_SEP = 48;
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
    // Inner (metaGroup) nodes use the same card size as top-level nodes so
    // expanded groups read like the rest of the graph.
    const type = data.type as string;

    if (type === "group") {
        const tpk = getTransformPrimaryKeys(data);
        const size = groupBoxSize(name, (data.child_count as number) ?? 1, tpk);
        return {
            id,
            type,
            name,
            w: size.w,
            h: size.h,
            child_count: data.child_count as number,
            pipelineIndex: data.pipelineIndex as number | undefined,
            pipelineOrderKey: data.pipelineOrderKey as string | undefined,
        };
    }
    if (type === "group-expanded") {
        const tpk = getTransformPrimaryKeys(data);
        const w = (data.boxW as number) ?? groupBoxSize(name, (data.child_count as number) ?? 1, tpk).w;
        const h = (data.boxH as number) ?? groupBoxSize(name, (data.child_count as number) ?? 1, tpk).h;
        return {
            id,
            type,
            name,
            w,
            h,
            child_count: data.child_count as number,
            pipelineIndex: data.pipelineIndex as number | undefined,
            pipelineOrderKey: data.pipelineOrderKey as string | undefined,
        };
    }
    if (type === "table") {
        const size = tableNodeSize(name, (data.indexes as string[]) || [], false);
        return {
            id,
            type,
            name,
            w: size.w,
            h: size.h,
            parent: data.parent as string | undefined,
            metaGroup: data.metaGroup as string | undefined,
            indexes: data.indexes as string[] | undefined,
            pipelineIndex: data.pipelineIndex as number | undefined,
            pipelineOrderKey: data.pipelineOrderKey as string | undefined,
        };
    }
    const tpk = getTransformPrimaryKeys(data);
    const size = stepNodeSize(name, false, tpk);
    return {
        id,
        type,
        name,
        w: size.w,
        h: size.h,
        parent: data.parent as string | undefined,
        metaGroup: data.metaGroup as string | undefined,
        pipelineIndex: data.pipelineIndex as number | undefined,
        pipelineOrderKey: data.pipelineOrderKey as string | undefined,
    };
}

function sortIds(ids: string[]): string[] {
    return [...ids].sort((a, b) => a.localeCompare(b));
}

function comparePipelineOrder(
    a: string,
    b: string,
    nodes: Map<string, MeasuredNode>,
): number {
    const na = nodes.get(a);
    const nb = nodes.get(b);

    const oa = na?.pipelineOrderKey;
    const ob = nb?.pipelineOrderKey;
    if (oa && ob && oa !== ob) {
        return oa.localeCompare(ob);
    }

    const ia = na?.pipelineIndex;
    const ib = nb?.pipelineIndex;
    if (ia != null && ib != null && ia !== ib) {
        return ia - ib;
    }

    return a.localeCompare(b);
}

function sortRankIds(ids: string[], nodes: Map<string, MeasuredNode>): string[] {
    return [...ids].sort((a, b) => comparePipelineOrder(a, b, nodes));
}

/** Count crossings between two consecutive ordered layers (straight-line bipartite). */
export function countBipartiteCrossings(
    upper: string[],
    lower: string[],
    edges: LayoutEdge[],
): number {
    const upperIndex = new Map(upper.map((id, index) => [id, index]));
    const lowerIndex = new Map(lower.map((id, index) => [id, index]));
    const pairs: Array<{ u: number; v: number }> = [];
    edges.forEach(({ source, target }) => {
        const u = upperIndex.get(source);
        const v = lowerIndex.get(target);
        if (u != null && v != null) {
            pairs.push({ u, v });
            return;
        }
        const uRev = upperIndex.get(target);
        const vRev = lowerIndex.get(source);
        if (uRev != null && vRev != null) pairs.push({ u: uRev, v: vRev });
    });
    let crossings = 0;
    for (let i = 0; i < pairs.length; i += 1) {
        for (let j = i + 1; j < pairs.length; j += 1) {
            const a = pairs[i];
            const b = pairs[j];
            if ((a.u - b.u) * (a.v - b.v) < 0) crossings += 1;
        }
    }
    return crossings;
}

function layerNeighborMap(
    layer: string[],
    other: string[],
    edges: LayoutEdge[],
): Map<string, string[]> {
    const layerSet = new Set(layer);
    const otherSet = new Set(other);
    const neighbors = new Map<string, string[]>();
    layer.forEach((id) => neighbors.set(id, []));
    edges.forEach(({ source, target }) => {
        if (layerSet.has(source) && otherSet.has(target)) {
            neighbors.get(source)?.push(target);
        } else if (layerSet.has(target) && otherSet.has(source)) {
            neighbors.get(target)?.push(source);
        }
    });
    return neighbors;
}

function orderByBarycenter(
    layer: string[],
    otherOrder: Map<string, number>,
    neighbors: Map<string, string[]>,
    nodes: Map<string, MeasuredNode>,
): string[] {
    const scored = layer.map((id, index) => {
        const neigh = neighbors.get(id) ?? [];
        const positions = neigh
            .map((n) => otherOrder.get(n))
            .filter((pos): pos is number => pos != null);
        const bary =
            positions.length > 0
                ? positions.reduce((sum, pos) => sum + pos, 0) / positions.length
                : index;
        return { id, bary, index };
    });
    scored.sort((a, b) => {
        if (a.bary !== b.bary) return a.bary - b.bary;
        const byPipe = comparePipelineOrder(a.id, b.id, nodes);
        if (byPipe !== 0) return byPipe;
        return a.index - b.index;
    });
    return scored.map((entry) => entry.id);
}

/** Adjacent transposition pass — swap neighbors if that cuts crossings. */
function transposeLayerPair(
    upper: string[],
    lower: string[],
    edges: LayoutEdge[],
    _nodes: Map<string, MeasuredNode>,
    fixUpper: boolean,
): { upper: string[]; lower: string[]; improved: boolean } {
    let improved = false;
    const nextUpper = [...upper];
    const nextLower = [...lower];
    const movable = fixUpper ? nextLower : nextUpper;

    let swapped = true;
    while (swapped) {
        swapped = false;
        for (let i = 0; i < movable.length - 1; i += 1) {
            const before = countBipartiteCrossings(nextUpper, nextLower, edges);
            const tmp = movable[i];
            movable[i] = movable[i + 1];
            movable[i + 1] = tmp;
            const after = countBipartiteCrossings(nextUpper, nextLower, edges);
            if (after < before) {
                swapped = true;
                improved = true;
            } else {
                movable[i + 1] = movable[i];
                movable[i] = tmp;
            }
        }
    }
    return { upper: nextUpper, lower: nextLower, improved };
}

/**
 * Sugiyama-style crossing minimization: barycenter sweeps + transpose, seeded by
 * pipeline order. Keeps existing rank assignment; only reorders within layers.
 */
export function minimizeLayerCrossings(
    ranks: Map<number, string[]>,
    edges: LayoutEdge[],
    nodes: Map<string, MeasuredNode>,
    maxIters = 24,
): Map<number, string[]> {
    const rankKeys = Array.from(ranks.keys()).sort((a, b) => a - b);
    if (rankKeys.length < 2) return ranks;

    const ordered = new Map<number, string[]>();
    rankKeys.forEach((r) => ordered.set(r, sortRankIds(ranks.get(r) ?? [], nodes)));

    const totalCrossings = () => {
        let total = 0;
        for (let i = 0; i < rankKeys.length - 1; i += 1) {
            total += countBipartiteCrossings(
                ordered.get(rankKeys[i]) ?? [],
                ordered.get(rankKeys[i + 1]) ?? [],
                edges,
            );
        }
        return total;
    };

    let best = new Map(Array.from(ordered.entries()).map(([k, v]) => [k, [...v]]));
    let bestScore = totalCrossings();
    let prevScore = bestScore;

    for (let iter = 0; iter < maxIters; iter += 1) {
        // Downward sweep: order layer i+1 from layer i.
        for (let i = 0; i < rankKeys.length - 1; i += 1) {
            const upper = ordered.get(rankKeys[i]) ?? [];
            const lower = ordered.get(rankKeys[i + 1]) ?? [];
            const upperOrder = new Map(upper.map((id, index) => [id, index]));
            const neighbors = layerNeighborMap(lower, upper, edges);
            ordered.set(
                rankKeys[i + 1],
                orderByBarycenter(lower, upperOrder, neighbors, nodes),
            );
        }

        // Upward sweep: order layer i from layer i+1.
        for (let i = rankKeys.length - 1; i > 0; i -= 1) {
            const lower = ordered.get(rankKeys[i]) ?? [];
            const upper = ordered.get(rankKeys[i - 1]) ?? [];
            const lowerOrder = new Map(lower.map((id, index) => [id, index]));
            const neighbors = layerNeighborMap(upper, lower, edges);
            ordered.set(
                rankKeys[i - 1],
                orderByBarycenter(upper, lowerOrder, neighbors, nodes),
            );
        }

        // Local transpose refinement on each consecutive pair.
        for (let i = 0; i < rankKeys.length - 1; i += 1) {
            const upperKey = rankKeys[i];
            const lowerKey = rankKeys[i + 1];
            let upper = ordered.get(upperKey) ?? [];
            let lower = ordered.get(lowerKey) ?? [];
            const down = transposeLayerPair(upper, lower, edges, nodes, true);
            upper = down.upper;
            lower = down.lower;
            const up = transposeLayerPair(upper, lower, edges, nodes, false);
            ordered.set(upperKey, up.upper);
            ordered.set(lowerKey, up.lower);
        }

        const score = totalCrossings();
        if (score < bestScore) {
            bestScore = score;
            best = new Map(Array.from(ordered.entries()).map(([k, v]) => [k, [...v]]));
        }
        if (score >= prevScore) break;
        prevScore = score;
    }

    return best;
}

function densifyRanks(rank: Map<string, number>): Map<string, number> {
    const sorted = Array.from(new Set(rank.values())).sort((a, b) => a - b);
    const remap = new Map(sorted.map((value, index) => [value, index]));
    const next = new Map<string, number>();
    rank.forEach((value, id) => {
        next.set(id, remap.get(value) ?? value);
    });
    return next;
}

function hasPath(
    graph: Map<string, Set<string>>,
    from: string,
    to: string,
    visited: Set<string> = new Set(),
): boolean {
    if (from === to) return true;
    if (visited.has(from)) return false;
    visited.add(from);
    const nexts = Array.from(graph.get(from) ?? []);
    for (let i = 0; i < nexts.length; i += 1) {
        if (hasPath(graph, nexts[i], to, visited)) return true;
    }
    return false;
}

function edgePipelinePriority(edge: LayoutEdge, nodes: Map<string, MeasuredNode>): number {
    const srcIdx = nodes.get(edge.source)?.pipelineIndex ?? 9999;
    const tgtIdx = nodes.get(edge.target)?.pipelineIndex ?? 9999;
    return srcIdx <= tgtIdx ? 0 : 1;
}

/** Edges used only for vertical rank — cycle-closing edges are dropped. */
export function makeAcyclicRankEdges(
    edges: LayoutEdge[],
    nodes: Map<string, MeasuredNode>,
): LayoutEdge[] {
    const sorted = [...edges].sort(
        (a, b) => edgePipelinePriority(a, nodes) - edgePipelinePriority(b, nodes),
    );
    const result: LayoutEdge[] = [];
    const graph = new Map<string, Set<string>>();

    sorted.forEach((edge) => {
        if (!nodes.has(edge.source) || !nodes.has(edge.target)) return;
        if (hasPath(graph, edge.target, edge.source)) return;
        result.push(edge);
        if (!graph.has(edge.source)) graph.set(edge.source, new Set());
        graph.get(edge.source)?.add(edge.target);
    });
    return result;
}

function longestPathRanks(
    nodes: Map<string, MeasuredNode>,
    edges: LayoutEdge[],
): Map<string, number> {
    const ids = sortIds(Array.from(nodes.keys()));
    const rank = new Map<string, number>();
    ids.forEach((id) => rank.set(id, 0));
    let changed = true;
    let guard = 0;
    while (changed && guard < ids.length + 1) {
        changed = false;
        guard += 1;
        for (const { source, target } of edges) {
            if (!nodes.has(source) || !nodes.has(target)) continue;
            const nextRank = (rank.get(source) ?? 0) + 1;
            if (nextRank > (rank.get(target) ?? 0)) {
                rank.set(target, nextRank);
                changed = true;
            }
        }
    }
    return rank;
}

export function buildRankEdges(
    nodes: Map<string, MeasuredNode>,
    renderEdges: LayoutEdge[],
): LayoutEdge[] {
    const rankCandidates = renderEdges.filter((edge) => !edge.sequential && !edge.synthetic);
    return makeAcyclicRankEdges(rankCandidates, nodes);
}

function computeRanks(
    nodes: Map<string, MeasuredNode>,
    renderEdges: LayoutEdge[],
    rankEdges?: LayoutEdge[],
): Map<string, number> {
    const edgesForRank = rankEdges ?? buildRankEdges(nodes, renderEdges);
    return densifyRanks(longestPathRanks(nodes, edgesForRank));
}

/**
 * Deterministic layered layout (top-to-bottom or left-to-right).
 * Vertical rank from dependency depth; within-layer order uses Sugiyama-style
 * barycenter crossing minimization (seeded by pipeline order).
 */
export function layoutLayeredDag(
    nodes: Map<string, MeasuredNode>,
    edges: LayoutEdge[],
    rankDir: "TB" | "LR" = "TB",
    rankEdges?: LayoutEdge[],
): Map<string, BBox> {
    const ids = sortIds(Array.from(nodes.keys()));
    if (!ids.length) return new Map();

    const rank = computeRanks(nodes, edges, rankEdges);

    const ranks = new Map<number, string[]>();
    ids.forEach((id) => {
        const r = rank.get(id) ?? 0;
        if (!ranks.has(r)) ranks.set(r, []);
        ranks.get(r)?.push(id);
    });
    // Use all render edges for crossing reduction (incl. long spans between
    // adjacent layers after densify); rankEdges only drive layer assignment.
    const orderedRanks = minimizeLayerCrossings(ranks, edges, nodes);

    const positions = new Map<string, BBox>();
    const sortedRankKeys = Array.from(orderedRanks.keys()).sort((a, b) => a - b);

    if (rankDir === "TB") {
        let yCursor = 0;
        sortedRankKeys.forEach((r) => {
            const rankIds = orderedRanks.get(r) ?? [];
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
            const rankHeight = Math.max(...rankIds.map((id) => nodes.get(id)?.h ?? 0), 0);
            yCursor += rankHeight + RANK_SEP;
        });
    } else {
        let xCursor = 0;
        sortedRankKeys.forEach((r) => {
            const rankIds = orderedRanks.get(r) ?? [];
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
            const rankWidth = Math.max(...rankIds.map((id) => nodes.get(id)?.w ?? 0), 0);
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
    const rankEdges = hasInternalEdges ? buildRankEdges(children, edges) : edges;
    const positions = hasInternalEdges
        ? layoutLayeredDag(children, edges, "TB", rankEdges)
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
        sequential: edge.sequential as boolean | undefined,
        synthetic: edge.synthetic as boolean | undefined,
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

type LayoutMoveUnit = {
    members: Array<{ entry: LayoutEntry; origX: number; origY: number }>;
    originX: number;
    originY: number;
    width: number;
    height: number;
};

/**
 * Group a blue frame with its inner nodes so expand/collapse shifts them as one
 * rigid body. Without this, row-repacking treats each child as its own node and
 * they drift out of the frame when another group expands above/beside them.
 */
function collectLayoutMoveUnits(layout: GraphLayout, exclude: Set<string>): LayoutMoveUnit[] {
    const claimed = new Set<string>();
    const units: LayoutMoveUnit[] = [];

    layout.forEach((entry, id) => {
        if (exclude.has(id) || !entry.visible) return;
        if (entry.node.type !== "group-expanded") return;

        const members: LayoutMoveUnit["members"] = [
            { entry, origX: entry.bbox.x, origY: entry.bbox.y },
        ];
        claimed.add(id);

        layout.forEach((childEntry, childId) => {
            if (exclude.has(childId) || !childEntry.visible) return;
            if (childEntry.node.metaGroup !== id) return;
            members.push({
                entry: childEntry,
                origX: childEntry.bbox.x,
                origY: childEntry.bbox.y,
            });
            claimed.add(childId);
        });

        let x1 = Infinity;
        let y1 = Infinity;
        let x2 = -Infinity;
        let y2 = -Infinity;
        members.forEach((m) => {
            x1 = Math.min(x1, m.origX);
            y1 = Math.min(y1, m.origY);
            x2 = Math.max(x2, m.origX + m.entry.bbox.w);
            y2 = Math.max(y2, m.origY + m.entry.bbox.h);
        });

        units.push({
            members,
            originX: x1,
            originY: y1,
            width: x2 - x1,
            height: y2 - y1,
        });
    });

    layout.forEach((entry, id) => {
        if (exclude.has(id) || !entry.visible || claimed.has(id)) return;
        units.push({
            members: [{ entry, origX: entry.bbox.x, origY: entry.bbox.y }],
            originX: entry.bbox.x,
            originY: entry.bbox.y,
            width: entry.bbox.w,
            height: entry.bbox.h,
        });
    });

    return units;
}

function translateUnit(unit: LayoutMoveUnit, dx: number, dy: number): void {
    if (dx === 0 && dy === 0) return;
    unit.members.forEach((m) => {
        m.entry.bbox.x = m.origX + dx;
        m.entry.bbox.y = m.origY + dy;
    });
}

function reassignExternalRowsOnExpand(
    layout: GraphLayout,
    oldGroupBBox: BBox,
    newGroupBBox: BBox,
    exclude: Set<string>,
): void {
    const startY = newGroupBBox.y + newGroupBBox.h + NODE_SEP;

    const units = collectLayoutMoveUnits(layout, exclude)
        .filter((unit) => unit.originY >= oldGroupBBox.y)
        .sort((a, b) => a.originY - b.originY || a.originX - b.originX);

    if (!units.length) return;

    const rows: LayoutMoveUnit[][] = [];
    units.forEach((unit) => {
        const row = rows.find((r) => Math.abs(r[0].originY - unit.originY) <= 32);
        if (row) row.push(unit);
        else rows.push([unit]);
    });
    rows.sort((a, b) => a[0].originY - b[0].originY);

    let yCursor = startY;
    rows.forEach((row) => {
        row.sort((a, b) => a.originX - b.originX);
        const rowHeight = Math.max(...row.map((u) => u.height));
        row.forEach((unit) => {
            translateUnit(unit, 0, yCursor - unit.originY);
        });
        yCursor += rowHeight + RANK_SEP;
    });

    // Resolve horizontal crowding within each new row (units stay rigid).
    rows.forEach((row) => {
        for (let i = 1; i < row.length; i += 1) {
            const prev = row[i - 1];
            const curr = row[i];
            const prevRight = Math.max(
                ...prev.members.map((m) => m.entry.bbox.x + m.entry.bbox.w),
            );
            const currLeft = Math.min(...curr.members.map((m) => m.entry.bbox.x));
            const minLeft = prevRight + NODE_SEP;
            if (currLeft < minLeft) {
                const dx = minLeft - currLeft;
                curr.members.forEach((m) => {
                    m.entry.bbox.x += dx;
                });
            }
        }
    });
}

/**
 * Upfront layout for expand/collapse: top-center anchor stays fixed; external nodes
 * at/ below the group top move by the group size delta. On expand, rows below the
 * blue frame are repacked from the pre-expand row structure. Expanded groups move
 * as rigid clusters (frame + inners) so multi-expand stays coherent.
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
        // Uniform delta keeps other expanded groups' internals aligned with their frame.
        collectLayoutMoveUnits(layout, exclude).forEach((unit) => {
            if (unit.originY >= newGroupBBox.y) {
                translateUnit(unit, 0, deltaH);
            }
        });
        return;
    }

    const deltaW = newGroupBBox.w - oldGroupBBox.w;
    if (deltaW === 0) return;

    const floorX = newGroupBBox.x + newGroupBBox.w + NODE_SEP;
    collectLayoutMoveUnits(layout, exclude).forEach((unit) => {
        if (unit.originX < oldGroupBBox.x) return;

        let dx = deltaW;
        const candidate: BBox = {
            x: unit.originX + dx,
            y: unit.originY,
            w: unit.width,
            h: unit.height,
        };
        if (deltaW > 0 && bboxesOverlap(candidate, newGroupBBox)) {
            dx = floorX - unit.originX;
        }
        translateUnit(unit, dx, 0);
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
        // reprocessData marks expanded metas as group-expanded. Seed them here as a
        // collapsed group footprint so expandGroupInLayout can find and grow them —
        // skipping left the blue frame stuck at default 450×168 with no boxW/boxH.
        if (data.type === "group-expanded") {
            layoutNodes.set(
                id,
                measureNode({
                    ...data,
                    id,
                    type: "group",
                }),
            );
            return;
        }
        layoutNodes.set(id, measureNode({ ...data, id }));
    });

    const positions = layoutLayeredDag(layoutNodes, edgeList, rankDir, buildRankEdges(layoutNodes, edgeList));
    layoutNodes.forEach((node, id) => {
        const bbox = positions.get(id);
        if (!bbox) return;
        layout.set(id, { bbox, node, visible: true });
    });

    sortIds(Array.from(expanded))
        .filter((groupId) => layout.has(groupId))
        // Top-to-bottom (then left-to-right) so an upper expand never row-repacks
        // a group that was already expanded below it.
        .sort((a, b) => {
            const ea = layout.get(a);
            const eb = layout.get(b);
            if (!ea || !eb) return a.localeCompare(b);
            return ea.bbox.y - eb.bbox.y || ea.bbox.x - eb.bbox.x || a.localeCompare(b);
        })
        .forEach((groupId) => {
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
                // Drop any leftover style bypass from an interrupted morph — width/height
                // are mapped from boxW/boxH; a stale bypass leaves the frame squashed.
                node.removeStyle("width");
                node.removeStyle("height");
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

const morphRafStore = new WeakMap<Cytoscape.Core, Set<number>>();
const pendingMorphStore = new WeakMap<
    Cytoscape.Core,
    Map<
        string,
        {
            to: BBox;
            toCenter: { x: number; y: number };
            opacityTo?: number;
            onComplete?: () => void;
        }
    >
>();

function trackMorphRaf(cy: Cytoscape.Core, id: number): void {
    let set = morphRafStore.get(cy);
    if (!set) {
        set = new Set();
        morphRafStore.set(cy, set);
    }
    set.add(id);
}

function getPendingMorphs(cy: Cytoscape.Core) {
    let map = pendingMorphStore.get(cy);
    if (!map) {
        map = new Map();
        pendingMorphStore.set(cy, map);
    }
    return map;
}

function stopMorphAnimations(cy: Cytoscape.Core): void {
    const rafs = morphRafStore.get(cy);
    if (rafs) {
        rafs.forEach((id) => cancelAnimationFrame(id));
        rafs.clear();
    }
    // Jump unfinished morphs to their end state (mirrors cy.stop(true, true)).
    // Cancelling mid-morph without this leaves group-expanded stuck at the
    // collapsed 450×168 frame (the "squashed blue node" after expand/collapse).
    const pending = pendingMorphStore.get(cy);
    if (!pending?.size) return;
    pending.forEach((morph, nodeId) => {
        const ele = cy.getElementById(nodeId);
        if (!ele.empty()) {
            const node = ele as Cytoscape.NodeSingular;
            node.removeStyle("width");
            node.removeStyle("height");
            node.data({ boxW: morph.to.w, boxH: morph.to.h });
            node.position(morph.toCenter);
            if (typeof morph.opacityTo === "number") {
                node.style("opacity", morph.opacityTo);
            }
        }
        morph.onComplete?.();
    });
    pending.clear();
}

function easeInOutCubic(t: number): number {
    return t < 0.5 ? 4 * t * t * t : 1 - (-2 * t + 2) ** 3 / 2;
}

/**
 * Animate group frame size via boxW/boxH data (stylesheet mappers), not style
 * width/height bypasses. Cytoscape's style-animate of width fights function-mapped
 * stylesheet rules and leaves expand stuck at the collapsed size.
 */
function animateNodeBoxMorph(
    cy: Cytoscape.Core,
    node: Cytoscape.NodeSingular,
    from: BBox,
    to: BBox,
    fromCenter: { x: number; y: number },
    toCenter: { x: number; y: number },
    options: {
        duration?: number;
        opacityFrom?: number;
        opacityTo?: number;
        onComplete?: () => void;
    } = {},
): void {
    const duration = options.duration ?? ANIMATION_MS;
    const start = performance.now();
    const fadeOpacity =
        typeof options.opacityFrom === "number" && typeof options.opacityTo === "number";
    const nodeId = node.id();
    const pending = getPendingMorphs(cy);

    node.removeStyle("width");
    node.removeStyle("height");
    node.position(fromCenter);
    node.data({ boxW: from.w, boxH: from.h });
    if (fadeOpacity) {
        node.style("opacity", options.opacityFrom!);
    }

    pending.set(nodeId, {
        to,
        toCenter,
        opacityTo: fadeOpacity ? options.opacityTo : undefined,
        onComplete: options.onComplete,
    });

    const finish = () => {
        pending.delete(nodeId);
        if (cy.destroyed() || node.removed()) {
            options.onComplete?.();
            return;
        }
        node.removeStyle("width");
        node.removeStyle("height");
        node.data({ boxW: to.w, boxH: to.h });
        node.position(toCenter);
        if (fadeOpacity) {
            node.style("opacity", options.opacityTo!);
        }
        options.onComplete?.();
    };

    const tick = (now: number) => {
        if (!pending.has(nodeId)) return; // stopped / jumped to end
        if (cy.destroyed() || node.removed()) {
            pending.delete(nodeId);
            options.onComplete?.();
            return;
        }
        const t = Math.min(1, (now - start) / duration);
        const e = easeInOutCubic(t);
        const w = from.w + (to.w - from.w) * e;
        const h = from.h + (to.h - from.h) * e;
        const x = fromCenter.x + (toCenter.x - fromCenter.x) * e;
        const y = fromCenter.y + (toCenter.y - fromCenter.y) * e;

        node.position({ x, y });
        node.removeStyle("width");
        node.removeStyle("height");
        node.data({ boxW: w, boxH: h });
        if (fadeOpacity) {
            const op = options.opacityFrom! + (options.opacityTo! - options.opacityFrom!) * e;
            node.style("opacity", op);
        }

        if (t < 1) {
            trackMorphRaf(cy, requestAnimationFrame(tick));
            return;
        }
        finish();
    };

    trackMorphRaf(cy, requestAnimationFrame(tick));
}

export function stopLayoutAnimations(cy: Cytoscape.Core): void {
    if (cy.destroyed()) return;
    stopHtmlOpacityAnimations(cy);
    stopMorphAnimations(cy);
    cy.stop(true, true);
}

export type OpacityTiming = {
    delay?: number;
    duration?: number;
};

export type LayoutTransitionOptions = {
    fadeIn?: Set<string>;
    fadeOut?: Set<string>;
    /** Animate width/height (e.g. group-expanded ↔ collapsed group). */
    morphBoxes?: Map<string, { from: BBox; to: BBox }>;
    /** Edge keys (`source->target`) to crossfade during the transition. */
    edgeFadeIn?: Set<string>;
    edgeFadeOut?: Set<string>;
    fadeInTiming?: OpacityTiming | Map<string, OpacityTiming>;
    fadeOutTiming?: OpacityTiming | Map<string, OpacityTiming>;
    onComplete?: () => void;
};

function resolveOpacityTiming(
    id: string,
    timing?: OpacityTiming | Map<string, OpacityTiming>,
): OpacityTiming {
    if (!timing) return {};
    if (timing instanceof Map) return timing.get(id) ?? {};
    return timing;
}

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
                    // Only leave fadeOut nodes invisible when they are gone from the
                    // target layout. Boundary input tables must stay visible after collapse
                    // even if they were wrongly included in fadeOut during expand.
                    const leaves = fadeOut.has(node.id()) && !toLayout.has(node.id());
                    setNodeVisualOpacity(cy, n, leaves ? 0 : 1);
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
            const isNativeGroupFrame = nodeEl.data("type") === "group-expanded";
            nodeEl.position(fromCenter);
            nodeEl.removeStyle("width");
            nodeEl.removeStyle("height");
            nodeEl.data("boxW", morph.from.w);
            nodeEl.data("boxH", morph.from.h);
            if (isNativeGroupFrame) {
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
            animateNodeBoxMorph(
                cy,
                nodeEl,
                morph.from,
                morph.to,
                bboxCenter(morph.from),
                target,
                {
                    duration: ANIMATION_MS,
                    // Shrinking native frame fades out while the HTML collapsed card fades in.
                    ...(shrinking && isNativeGroupFrame
                        ? { opacityFrom: 1, opacityTo: 0 }
                        : {}),
                    onComplete: () => {
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
            const timing = resolveOpacityTiming(
                id,
                isFadeIn ? options?.fadeInTiming : options?.fadeOutTiming,
            );
            animateNodeVisualOpacity(
                cy,
                id,
                fromOpacity,
                toOpacity,
                timing.duration ?? ANIMATION_MS,
                () => {
                    opacityDone = true;
                    maybeFinish();
                },
                timing.delay ?? 0,
            );
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

const READABLE_ZOOM = 0.4;
const FIT_PADDING = 60;

/** Bounding box (model coords) of all visible entries in a layout. */
export function layoutContentBBox(layout: GraphLayout): BBox | null {
    let x1 = Infinity;
    let y1 = Infinity;
    let x2 = -Infinity;
    let y2 = -Infinity;
    layout.forEach((entry) => {
        if (!entry.visible) return;
        x1 = Math.min(x1, entry.bbox.x);
        y1 = Math.min(y1, entry.bbox.y);
        x2 = Math.max(x2, entry.bbox.x + entry.bbox.w);
        y2 = Math.max(y2, entry.bbox.y + entry.bbox.h);
    });
    if (!Number.isFinite(x1)) return null;
    return { x: x1, y: y1, w: x2 - x1, h: y2 - y1 };
}

export type Viewport = { zoom: number; pan: { x: number; y: number }; minZoom: number; maxZoom: number };

/**
 * Target pan/zoom to fit `bb` (model coords) into the viewport, mirroring the
 * clamping and readable-zoom rules of `fitGraphViewport` but *without* mutating
 * the camera — so callers can animate toward it instead of snapping.
 */
export function fitViewportForBBox(cy: Cytoscape.Core, bb: BBox): Viewport | null {
    const W = cy.width();
    const H = cy.height();
    if (!bb.w || !bb.h || !W || !H) return null;

    const rawZoom = Math.min((W - 2 * FIT_PADDING) / bb.w, (H - 2 * FIT_PADDING) / bb.h);
    const minZoom = Math.min(0.05, rawZoom * 0.3);
    const maxZoom = Math.max(2.5, rawZoom * 6);
    let zoom = Math.max(minZoom, Math.min(maxZoom, rawZoom));

    let pan: { x: number; y: number };
    if (zoom < READABLE_ZOOM) {
        zoom = READABLE_ZOOM;
        pan = {
            x: W / 2 - READABLE_ZOOM * (bb.x + bb.w / 2),
            y: 90 - READABLE_ZOOM * bb.y,
        };
    } else {
        pan = {
            x: W / 2 - zoom * (bb.x + bb.w / 2),
            y: H / 2 - zoom * (bb.y + bb.h / 2),
        };
    }
    return { zoom, pan, minZoom, maxZoom };
}

/**
 * Smoothly pan/zoom the viewport to fit `layout`, running concurrently with a
 * node layout transition so the camera glides to the new frame instead of
 * snapping before the graph animates.
 */
export function animateFitViewport(
    cy: Cytoscape.Core,
    layout: GraphLayout,
    duration: number = ANIMATION_MS,
): void {
    const bb = layoutContentBBox(layout);
    if (!bb) return;
    const target = fitViewportForBBox(cy, bb);
    if (!target) return;

    // Widen bounds first so the target zoom is always reachable by the tween.
    cy.minZoom(target.minZoom);
    cy.maxZoom(target.maxZoom);
    cy.animate(
        { zoom: target.zoom, pan: target.pan },
        { duration, easing: ANIMATION_EASING },
    );
}

export { ANIMATION_MS, ANIMATION_EASING } from "./animationConstants";
export { GROUP_PADDING };
