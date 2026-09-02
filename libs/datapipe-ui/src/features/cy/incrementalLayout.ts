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
import {
    pauseInternalEdgeOverlayPaths,
    resumeInternalEdgeOverlayPaths,
} from "./internalEdgeOverlay";

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
    /** Zigzag row index from LR wrap (even L→R, odd R→L). */
    snakeRow?: number;
};

export type GraphLayout = Map<string, LayoutEntry>;

const GROUP_PADDING = { top: 56, bottom: 44, left: 44, right: 44 };
const RANK_SEP = 48;
const NODE_SEP = 48;
/** Vertical gap between wrapped LR rows of pipeline steps. */
const ROW_SEP = 96;
/** Extra horizontal gap between adjacent pipeline-step blocks on one row. */
const BLOCK_SEP = 72;

/** Snake row ids produced by the last `placeLayeredLrWrapped` for a positions map. */
const snakeRowsByPositions = new WeakMap<Map<string, BBox>, Map<string, number>>();

export function getSnakeRowsForPositions(
    positions: Map<string, BBox>,
): Map<string, number> | undefined {
    return snakeRowsByPositions.get(positions);
}
function bboxFromCenter(cx: number, cy: number, w: number, h: number): BBox {
    return { x: cx - w / 2, y: cy - h / 2, w, h };
}

function bboxCenter(bbox: BBox): { x: number; y: number } {
    return { x: bbox.x + bbox.w / 2, y: bbox.y + bbox.h / 2 };
}

/** Shift every visible bbox so `nodeId`'s center lands on `center`. */
export function pinLayoutAnchorCenter(
    layout: GraphLayout,
    nodeId: string,
    center: { x: number; y: number },
): void {
    const entry = layout.get(nodeId);
    if (!entry?.visible) return;
    const current = bboxCenter(entry.bbox);
    const dx = center.x - current.x;
    const dy = center.y - current.y;
    if (dx === 0 && dy === 0) return;
    layout.forEach((item) => {
        if (!item.visible) return;
        item.bbox.x += dx;
        item.bbox.y += dy;
    });
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

/** Normalized lateral position of a node in its own layer (0..1). */
function layerNormalizedPos(
    id: string,
    nodeRank: Map<string, number>,
    ordered: Map<number, string[]>,
): number | null {
    const rank = nodeRank.get(id);
    if (rank == null) return null;
    const layer = ordered.get(rank);
    if (!layer?.length) return null;
    const index = layer.indexOf(id);
    if (index < 0) return null;
    return layer.length === 1 ? 0.5 : index / (layer.length - 1);
}

/**
 * Crossings on the cut between two consecutive ranks, including long-span edges
 * (e.g. sequential dashed hops) that jump over intermediate layers.
 */
export function countLayerCutCrossings(
    ordered: Map<number, string[]>,
    nodeRank: Map<string, number>,
    upperRank: number,
    lowerRank: number,
    edges: LayoutEdge[],
): number {
    const pairs: Array<{ u: number; v: number }> = [];
    edges.forEach(({ source, target }) => {
        let src = source;
        let tgt = target;
        let rs = nodeRank.get(src);
        let rt = nodeRank.get(tgt);
        if (rs == null || rt == null || rs === rt) return;
        if (rs > rt) {
            src = target;
            tgt = source;
            const tmp = rs;
            rs = rt;
            rt = tmp;
        }
        // Edge crosses this cut if it starts at/above upper and ends at/below lower.
        if (rs > upperRank || rt < lowerRank) return;
        const u = layerNormalizedPos(src, nodeRank, ordered);
        const v = layerNormalizedPos(tgt, nodeRank, ordered);
        if (u == null || v == null) return;
        pairs.push({ u, v });
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

function buildNodeRankMap(ordered: Map<number, string[]>): Map<string, number> {
    const nodeRank = new Map<string, number>();
    ordered.forEach((ids, rank) => {
        ids.forEach((id) => nodeRank.set(id, rank));
    });
    return nodeRank;
}

/**
 * Neighbors for barycenter: adjacent-layer edges plus long-span ends on the
 * far side (so sequential dashed hops pull endpoints into uncrossed lanes).
 */
function layerNeighborPositions(
    layer: string[],
    layerRank: number,
    toward: "above" | "below",
    nodeRank: Map<string, number>,
    ordered: Map<number, string[]>,
    edges: LayoutEdge[],
): Map<string, number[]> {
    const layerSet = new Set(layer);
    const positions = new Map<string, number[]>();
    layer.forEach((id) => positions.set(id, []));

    edges.forEach(({ source, target }) => {
        const tryPush = (from: string, to: string) => {
            if (!layerSet.has(from)) return;
            const toRank = nodeRank.get(to);
            if (toRank == null) return;
            if (toward === "above" && !(toRank < layerRank)) return;
            if (toward === "below" && !(toRank > layerRank)) return;
            const pos = layerNormalizedPos(to, nodeRank, ordered);
            if (pos == null) return;
            positions.get(from)?.push(pos);
        };
        tryPush(source, target);
        tryPush(target, source);
    });

    return positions;
}

function orderByFarBarycenter(
    layer: string[],
    neighborPositions: Map<string, number[]>,
    nodes: Map<string, MeasuredNode>,
): string[] {
    const scored = layer.map((id, index) => {
        const positions = neighborPositions.get(id) ?? [];
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
    ordered: Map<number, string[]>,
    nodeRank: Map<string, number>,
    upperRank: number,
    lowerRank: number,
    edges: LayoutEdge[],
    fixUpper: boolean,
): { upper: string[]; lower: string[]; improved: boolean } {
    let improved = false;
    const nextUpper = [...(ordered.get(upperRank) ?? [])];
    const nextLower = [...(ordered.get(lowerRank) ?? [])];
    const movable = fixUpper ? nextLower : nextUpper;
    const scratch = new Map(ordered);
    scratch.set(upperRank, nextUpper);
    scratch.set(lowerRank, nextLower);

    let swapped = true;
    while (swapped) {
        swapped = false;
        for (let i = 0; i < movable.length - 1; i += 1) {
            const before = countLayerCutCrossings(
                scratch,
                nodeRank,
                upperRank,
                lowerRank,
                edges,
            );
            const tmp = movable[i];
            movable[i] = movable[i + 1];
            movable[i + 1] = tmp;
            const after = countLayerCutCrossings(
                scratch,
                nodeRank,
                upperRank,
                lowerRank,
                edges,
            );
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
 * Long-span edges (sequential dashed hops) count on every cut they cross so they
 * avoid colliding with solid mid-layer routes.
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
    const nodeRank = buildNodeRankMap(ordered);

    const totalCrossings = () => {
        let total = 0;
        for (let i = 0; i < rankKeys.length - 1; i += 1) {
            total += countLayerCutCrossings(
                ordered,
                nodeRank,
                rankKeys[i],
                rankKeys[i + 1],
                edges,
            );
        }
        return total;
    };

    let best = new Map(Array.from(ordered.entries()).map(([k, v]) => [k, [...v]]));
    let bestScore = totalCrossings();
    let prevScore = bestScore;

    for (let iter = 0; iter < maxIters; iter += 1) {
        // Downward sweep: order layer i+1 from everything above (incl. long spans).
        for (let i = 0; i < rankKeys.length - 1; i += 1) {
            const lowerKey = rankKeys[i + 1];
            const lower = ordered.get(lowerKey) ?? [];
            const neighborPos = layerNeighborPositions(
                lower,
                lowerKey,
                "above",
                nodeRank,
                ordered,
                edges,
            );
            ordered.set(lowerKey, orderByFarBarycenter(lower, neighborPos, nodes));
        }

        // Upward sweep: order layer i from everything below.
        for (let i = rankKeys.length - 1; i > 0; i -= 1) {
            const upperKey = rankKeys[i - 1];
            const upper = ordered.get(upperKey) ?? [];
            const neighborPos = layerNeighborPositions(
                upper,
                upperKey,
                "below",
                nodeRank,
                ordered,
                edges,
            );
            ordered.set(upperKey, orderByFarBarycenter(upper, neighborPos, nodes));
        }

        // Local transpose refinement on each consecutive pair.
        for (let i = 0; i < rankKeys.length - 1; i += 1) {
            const upperKey = rankKeys[i];
            const lowerKey = rankKeys[i + 1];
            const down = transposeLayerPair(
                ordered,
                nodeRank,
                upperKey,
                lowerKey,
                edges,
                true,
            );
            ordered.set(upperKey, down.upper);
            ordered.set(lowerKey, down.lower);
            const up = transposeLayerPair(
                ordered,
                nodeRank,
                upperKey,
                lowerKey,
                edges,
                false,
            );
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

/**
 * Collapse `0018.out.0000` / `0013.0000` → `0018` / `0013` for step grouping.
 */
function primaryPipelineOrderKey(key: string): string {
    const match = /^(\d{4})/.exec(key);
    return match ? match[1] : key;
}

/**
 * Within one pipeline step: inputs → transform(s) → outputs as separate layers
 * so LR reads `<table> → <step> → <table>` instead of stacking roles in one column.
 * 0 = `.in.`, 1 = step/meta, 2 = `.out.`.
 */
function pipelineOrderPhase(key: string): number {
    if (/\.in\./.test(key)) return 0;
    if (/\.out\./.test(key)) return 2;
    return 1;
}

/**
 * Prefer pipeline chronology (`pipelineOrderKey`) for layer assignment so late
 * steps like fiftyone stay on the right even when a back-edge dependency would
 * push them left under classic longest-path ranking.
 *
 * Layers advance on (primary step, role phase) so each step expands to up to
 * three columns — not one mega-column and not one column per unique key.
 */
function chronologicalRanks(
    nodes: Map<string, MeasuredNode>,
): Map<string, number> | null {
    const keyed: Array<{ id: string; primary: string; phase: number; key: string }> = [];
    nodes.forEach((node, id) => {
        if (!node.pipelineOrderKey) return;
        keyed.push({
            id,
            primary: primaryPipelineOrderKey(node.pipelineOrderKey),
            phase: pipelineOrderPhase(node.pipelineOrderKey),
            key: node.pipelineOrderKey,
        });
    });
    if (!keyed.length || keyed.length < nodes.size * 0.5) return null;

    keyed.sort(
        (a, b) =>
            a.primary.localeCompare(b.primary) ||
            a.phase - b.phase ||
            a.key.localeCompare(b.key) ||
            a.id.localeCompare(b.id),
    );

    const rank = new Map<string, number>();
    let layer = 0;
    let prevPrimary = keyed[0].primary;
    let prevPhase = keyed[0].phase;
    keyed.forEach(({ id, primary, phase }) => {
        if (primary !== prevPrimary || phase !== prevPhase) {
            layer += 1;
            prevPrimary = primary;
            prevPhase = phase;
        }
        rank.set(id, layer);
    });

    // Unkeyed leftovers sit after the last chronological layer.
    nodes.forEach((_node, id) => {
        if (!rank.has(id)) rank.set(id, layer + 1);
    });
    return densifyRanks(rank);
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
    // Sequential/synthetic next-step hops stay out of dependency ranking when
    // chronology is unavailable; chronologicalRanks uses pipelineOrderKey instead.
    const rankCandidates = renderEdges.filter((edge) => !edge.sequential && !edge.synthetic);
    return makeAcyclicRankEdges(rankCandidates, nodes);
}

function computeRanks(
    nodes: Map<string, MeasuredNode>,
    renderEdges: LayoutEdge[],
    rankEdges?: LayoutEdge[],
    preferChronology = true,
): Map<string, number> {
    // Snake (wrapped LR) keeps pipeline chronology. Flat horizontal / vertical
    // use dependency depth so parallel sources share a column (prototype lanes).
    if (preferChronology) {
        const chrono = chronologicalRanks(nodes);
        if (chrono) return chrono;
    }
    const edgesForRank = rankEdges ?? buildRankEdges(nodes, renderEdges);
    return densifyRanks(longestPathRanks(nodes, edgesForRank));
}

/**
 * Aim for a roughly square step grid so long pipelines don't become one
 * ultra-wide ribbon. Short graphs stay on a single row.
 */
function targetBlocksPerRow(blockCount: number): number {
    if (blockCount <= 5) return blockCount;
    return Math.max(3, Math.ceil(Math.sqrt(blockCount)));
}

function rankColumnHeight(rankIds: string[], nodes: Map<string, MeasuredNode>): number {
    const heights = rankIds.map((id) => nodes.get(id)?.h ?? 0);
    return heights.reduce((sum, h) => sum + h, 0) + NODE_SEP * Math.max(0, rankIds.length - 1);
}

function rankColumnWidth(rankIds: string[], nodes: Map<string, MeasuredNode>): number {
    return Math.max(...rankIds.map((id) => nodes.get(id)?.w ?? 0), 0);
}

/**
 * Group consecutive LR layers that share a pipeline primary (`0003`, …) into
 * one step block so wrapping never splits `<in> → <step> → <out>`.
 */
function groupRanksIntoStepBlocks(
    sortedRankKeys: number[],
    orderedRanks: Map<number, string[]>,
    nodes: Map<string, MeasuredNode>,
): number[][] {
    const blocks: number[][] = [];
    let current: number[] | null = null;
    let prevPrimary: string | null = null;

    sortedRankKeys.forEach((r) => {
        const rankIds = orderedRanks.get(r) ?? [];
        let primary: string | null = null;
        for (const id of rankIds) {
            const key = nodes.get(id)?.pipelineOrderKey;
            if (key) {
                primary = primaryPipelineOrderKey(key);
                break;
            }
        }
        const sameStep =
            current !== null &&
            primary !== null &&
            prevPrimary !== null &&
            primary === prevPrimary;
        if (!sameStep || current === null) {
            current = [r];
            blocks.push(current);
            prevPrimary = primary;
            return;
        }
        current.push(r);
    });
    return blocks;
}

/**
 * Place LR layers left-to-right, wrapping whole pipeline-step blocks onto new
 * rows so chronology stays readable without an endless horizontal strip.
 *
 * Rows alternate direction (snake / boustrophedon): even L→R, odd R→L, so the
 * eye continues from the end of one row into the start of the next on the same
 * side. Within a block, `in → step → out` follows the row direction so the
 * snake always reads inputs then step then outputs (mirrored on RTL rows).
 */
function placeLayeredLrWrapped(
    nodes: Map<string, MeasuredNode>,
    orderedRanks: Map<number, string[]>,
): Map<string, BBox> {
    const sortedRankKeys = Array.from(orderedRanks.keys()).sort((a, b) => a - b);
    const blocks = groupRanksIntoStepBlocks(sortedRankKeys, orderedRanks, nodes);
    const perRow = targetBlocksPerRow(blocks.length);

    const blockSize = blocks.map((rankKeys) => {
        let width = 0;
        let height = 0;
        rankKeys.forEach((r, index) => {
            const rankIds = orderedRanks.get(r) ?? [];
            width += rankColumnWidth(rankIds, nodes);
            if (index < rankKeys.length - 1) width += RANK_SEP;
            height = Math.max(height, rankColumnHeight(rankIds, nodes));
        });
        return { width, height };
    });

    const rowContentWidth = (rowStart: number, rowEnd: number): number => {
        let width = 0;
        for (let bi = rowStart; bi < rowEnd; bi += 1) {
            width += blockSize[bi].width;
            if (bi < rowEnd - 1) width += BLOCK_SEP;
        }
        return width;
    };

    let maxContentWidth = 0;
    for (let rowStart = 0; rowStart < blocks.length; rowStart += perRow) {
        maxContentWidth = Math.max(
            maxContentWidth,
            rowContentWidth(rowStart, Math.min(rowStart + perRow, blocks.length)),
        );
    }

    const positions = new Map<string, BBox>();
    const snakeRows = new Map<string, number>();
    let yRow = 0;
    for (let rowStart = 0; rowStart < blocks.length; rowStart += perRow) {
        const rowEnd = Math.min(rowStart + perRow, blocks.length);
        const rowIndex = Math.floor(rowStart / perRow);
        const rtl = rowIndex % 2 === 1;
        const rowHeight = Math.max(
            ...blockSize.slice(rowStart, rowEnd).map((size) => size.height),
            0,
        );
        const contentWidth = rowContentWidth(rowStart, rowEnd);
        // Right-align RTL (and short trailing RTL) rows so the continuation sits
        // under the previous row's exit instead of jumping back to x=0.
        let xCursor = rtl ? maxContentWidth - contentWidth : 0;

        const placeOrder: number[] = [];
        for (let bi = rowStart; bi < rowEnd; bi += 1) placeOrder.push(bi);
        if (rtl) placeOrder.reverse();

        placeOrder.forEach((bi, orderIndex) => {
            const rankKeys = blocks[bi];
            // RTL rows: mirror in→step→out so snake R→L still reads inputs first.
            const ranksToPlace = rtl ? [...rankKeys].reverse() : rankKeys;
            let x = xCursor;
            ranksToPlace.forEach((r, ri) => {
                const rankIds = orderedRanks.get(r) ?? [];
                const colW = rankColumnWidth(rankIds, nodes);
                const colH = rankColumnHeight(rankIds, nodes);
                let yCursor = yRow + (rowHeight - colH) / 2;
                rankIds.forEach((id, index) => {
                    const node = nodes.get(id);
                    if (!node) return;
                    const cx = x + colW / 2;
                    const cy = yCursor + node.h / 2;
                    positions.set(id, bboxFromCenter(cx, cy, node.w, node.h));
                    snakeRows.set(id, rowIndex);
                    yCursor += node.h + (index < rankIds.length - 1 ? NODE_SEP : 0);
                });
                x += colW + (ri < ranksToPlace.length - 1 ? RANK_SEP : 0);
            });
            xCursor += blockSize[bi].width + (orderIndex < placeOrder.length - 1 ? BLOCK_SEP : 0);
        });
        yRow += rowHeight + ROW_SEP;
    }
    snakeRowsByPositions.set(positions, snakeRows);
    return positions;
}

/**
 * Single-row LR placement (no snake wrap) — used for the flat horizontal ribbon.
 */
function placeLayeredLrFlat(
    nodes: Map<string, MeasuredNode>,
    orderedRanks: Map<number, string[]>,
): Map<string, BBox> {
    const positions = new Map<string, BBox>();
    const sortedRankKeys = Array.from(orderedRanks.keys()).sort((a, b) => a - b);
    let xCursor = 0;
    sortedRankKeys.forEach((r) => {
        const rankIds = orderedRanks.get(r) ?? [];
        const colW = rankColumnWidth(rankIds, nodes);
        const colH = rankColumnHeight(rankIds, nodes);
        let yCursor = -colH / 2;
        rankIds.forEach((id, index) => {
            const node = nodes.get(id);
            if (!node) return;
            const cx = xCursor + colW / 2;
            const cy = yCursor + node.h / 2;
            positions.set(id, bboxFromCenter(cx, cy, node.w, node.h));
            yCursor += node.h + (index < rankIds.length - 1 ? NODE_SEP : 0);
        });
        xCursor += colW + RANK_SEP;
    });
    return positions;
}

/**
 * Deterministic layered layout (top-to-bottom or left-to-right).
 *
 * When `preferChronology` is true (snake / horizontal / vertical): layers follow
 * pipeline order keys when present (inputs → step → outputs per step).
 * When false (compact H/V): layers follow dependency depth (longest-path) so
 * parallel sources stack in one column — matching the stage-lane prototype.
 *
 * LR + wrapRows: snake grid (even L→R, odd R→L).
 * LR without wrap / TB: flat layered ribbon / stack.
 * Within-layer order uses Sugiyama-style barycenter crossing minimization.
 */
export function layoutLayeredDag(
    nodes: Map<string, MeasuredNode>,
    edges: LayoutEdge[],
    rankDir: "TB" | "LR" = "TB",
    rankEdges?: LayoutEdge[],
    wrapRows = true,
    preferChronology = true,
): Map<string, BBox> {
    const ids = sortIds(Array.from(nodes.keys()));
    if (!ids.length) return new Map();

    const rank = computeRanks(nodes, edges, rankEdges, preferChronology);

    const ranks = new Map<number, string[]>();
    ids.forEach((id) => {
        const r = rank.get(id) ?? 0;
        if (!ranks.has(r)) ranks.set(r, []);
        ranks.get(r)?.push(id);
    });
    // Use all render edges for crossing reduction (incl. long spans between
    // adjacent layers after densify); rankEdges only drive layer assignment.
    const orderedRanks = minimizeLayerCrossings(ranks, edges, nodes);

    if (rankDir === "LR") {
        return wrapRows
            ? placeLayeredLrWrapped(nodes, orderedRanks)
            : placeLayeredLrFlat(nodes, orderedRanks);
    }

    const positions = new Map<string, BBox>();
    const sortedRankKeys = Array.from(orderedRanks.keys()).sort((a, b) => a - b);

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
    const seen = new Set<string>();
    const order: string[] = [];
    orderedIds.forEach((id) => {
        if (!children.has(id) || seen.has(id)) return;
        seen.add(id);
        order.push(id);
    });
    // Place any members missing from pipelineOrder (e.g. second meta with the same
    // display name whose children were not in the first meta's order list).
    sortIds(Array.from(children.keys())).forEach((id) => {
        if (seen.has(id)) return;
        seen.add(id);
        order.push(id);
    });
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
    rankDir: "TB" | "LR" = "TB",
    wrapRows = true,
    preferChronology = true,
): { positions: Map<string, BBox>; contentBBox: BBox } {
    void wrapRows;
    // Expanded-meta children share a parent prefix (`0001.0000`, `0001.0001`, …).
    // chronologicalRanks only uses the first `\d{4}`, so every inner would land in
    // one vertical column and the blue frame would tower out of the snake. Strip
    // the shared prefix so inners lay out along the flow like a mini-pipeline.
    const layoutNodes = stripSharedPipelineOrderPrefix(children);
    const hasInternalEdges = edges.some(
        ({ source, target }) => layoutNodes.has(source) && layoutNodes.has(target),
    );
    const rankEdges = hasInternalEdges ? buildRankEdges(layoutNodes, edges) : edges;
    // Match the outer graph orientation so expanded metas don't flip axis mid-pipeline.
    // Never nest a zigzag inside the blue frame (outer snake already wraps).
    const positions = hasInternalEdges
        ? layoutLayeredDag(layoutNodes, edges, rankDir, rankEdges, false, preferChronology)
        : layoutVerticalStack(
              layoutNodes,
              pipelineOrder.length ? pipelineOrder : sortIds(Array.from(layoutNodes.keys())),
          );
    return { positions, contentBBox: innerGraphBBox(positions) };
}

/**
 * If every node shares the same leading `NNNN.` order segment (typical inside an
 * expanded meta), strip it so chronological ranking can advance per child step.
 */
function stripSharedPipelineOrderPrefix(
    nodes: Map<string, MeasuredNode>,
): Map<string, MeasuredNode> {
    let current = nodes;
    for (let guard = 0; guard < 8; guard += 1) {
        const keys = Array.from(current.values())
            .map((n) => n.pipelineOrderKey)
            .filter((k): k is string => Boolean(k));
        if (keys.length < current.size * 0.5) return current;
        const primaries = new Set(keys.map((k) => primaryPipelineOrderKey(k)));
        if (primaries.size !== 1) return current;
        const primary = Array.from(primaries)[0];
        const prefix = `${primary}.`;
        if (!keys.some((k) => k.startsWith(prefix))) return current;
        const next = new Map<string, MeasuredNode>();
        current.forEach((node, id) => {
            const key = node.pipelineOrderKey;
            next.set(id, {
                ...node,
                pipelineOrderKey: key?.startsWith(prefix) ? key.slice(prefix.length) : key,
            });
        });
        current = next;
    }
    return current;
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
            snakeRow: entry.snakeRow,
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
 * Upfront layout for expand/collapse: group grows/shrinks from its center;
 * neighbors on each side move with the corresponding edge so the frame doesn't
 * slide over them. Expanded groups move as rigid clusters (frame + inners).
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

        const dyTop = newGroupBBox.y - oldGroupBBox.y;
        const dyBottom =
            newGroupBBox.y + newGroupBBox.h - (oldGroupBBox.y + oldGroupBBox.h);

        if (deltaH > 0) {
            // Nodes fully above the old frame follow the rising top edge.
            collectLayoutMoveUnits(layout, exclude).forEach((unit) => {
                if (unit.originY + unit.height <= oldGroupBBox.y) {
                    translateUnit(unit, 0, dyTop);
                }
            });
            // Nodes at/below the old top are repacked under the new bottom.
            reassignExternalRowsOnExpand(layout, oldGroupBBox, newGroupBBox, exclude);
            return;
        }

        // Collapse: pull both sides back toward the center.
        collectLayoutMoveUnits(layout, exclude).forEach((unit) => {
            if (unit.originY + unit.height <= oldGroupBBox.y) {
                translateUnit(unit, 0, dyTop);
            } else if (unit.originY >= oldGroupBBox.y) {
                translateUnit(unit, 0, dyBottom);
            }
        });
        return;
    }

    const deltaW = newGroupBBox.w - oldGroupBBox.w;
    if (deltaW === 0) return;

    const dxLeft = newGroupBBox.x - oldGroupBBox.x;
    const dxRight =
        newGroupBBox.x + newGroupBBox.w - (oldGroupBBox.x + oldGroupBBox.w);
    const floorX = newGroupBBox.x + newGroupBBox.w + NODE_SEP;

    collectLayoutMoveUnits(layout, exclude).forEach((unit) => {
        if (unit.originX + unit.width <= oldGroupBBox.x) {
            translateUnit(unit, dxLeft, 0);
            return;
        }
        if (unit.originX >= oldGroupBBox.x + oldGroupBBox.w) {
            translateUnit(unit, dxRight, 0);
            return;
        }
        if (unit.originX < oldGroupBBox.x) return;

        // Overlapping the old frame's horizontal band — park to the right.
        let dx = dxRight;
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
    wrapRows = true,
    preferChronology = true,
): GraphLayout {
    void wrapRows;
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
        rankDir,
        // Never nest a zigzag inside the blue frame — that towers out of the
        // outer snake corridor. Inners stay a single strip along the outer axis
        // so expand grows along the flow and downstream nodes shift further.
        false,
        preferChronology,
    );
    const expandedSize = {
        w: contentBBox.w + GROUP_PADDING.left + GROUP_PADDING.right,
        h: contentBBox.h + GROUP_PADDING.top + GROUP_PADDING.bottom,
    };

    const oldCenter = bboxCenter(groupEntry.bbox);
    const oldBBox = { ...groupEntry.bbox };
    // Grow from the card center so expand doesn't slide the frame away.
    const groupBBox = bboxFromCenter(oldCenter.x, oldCenter.y, expandedSize.w, expandedSize.h);
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
            snakeRow: groupEntry.snakeRow,
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

    const oldCenter = bboxCenter(oldBBox);
    const collapsedBBox = bboxFromCenter(oldCenter.x, oldCenter.y, collapsed.w, collapsed.h);

    members.forEach((id) => next.delete(id));

    groupEntry.bbox = collapsedBBox;
    groupEntry.node = collapsed;
    groupEntry.visible = true;

    const exclude = new Set<string>([groupId]);
    computeExternalPositionsAfterGroupResize(next, oldBBox, collapsedBBox, exclude, rankDir);
    return next;
}

type ExpandedGroupPlan = {
    size: { w: number; h: number };
    contentBBox: BBox;
    innerIds: string[];
    innerNodes: Map<string, MeasuredNode>;
    innerPositions: Map<string, BBox>;
};

/** Measure an expanded meta's inner DAG and the blue-frame size that contains it. */
function planExpandedGroup(
    groupId: string,
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edgeList: LayoutEdge[],
    rankDir: "TB" | "LR",
    pipelineOrder: string[],
    _wrapRows = true,
    preferChronology = true,
): ExpandedGroupPlan | null {
    const members = getGroupMembers(nodes, groupId);
    if (!members.size) return null;
    const boundary = getBoundaryNodes(members, edgeList);
    const innerIds = sortIds(
        Array.from(members).filter((id) => {
            const data = nodes.get(id);
            return data && !(data.type === "table" && boundary.has(id));
        }),
    );
    if (!innerIds.length) return null;

    const innerNodes = new Map<string, MeasuredNode>();
    innerIds.forEach((id) => {
        const data = nodes.get(id);
        if (!data) return;
        innerNodes.set(id, measureNode({ ...data, id }));
    });
    const innerEdges: LayoutEdge[] = edgeList.filter(
        ({ source, target }) => innerNodes.has(source) && innerNodes.has(target),
    );
    const { positions: innerPositions, contentBBox } = layoutInnerGraph(
        innerNodes,
        innerEdges,
        pipelineOrder.length ? pipelineOrder : innerIds,
        rankDir,
        // Flat strip along the outer axis — see expandGroupInLayout.
        false,
        preferChronology,
    );
    return {
        size: {
            w: contentBBox.w + GROUP_PADDING.left + GROUP_PADDING.right,
            h: contentBBox.h + GROUP_PADDING.top + GROUP_PADDING.bottom,
        },
        contentBBox,
        innerIds,
        innerNodes,
        innerPositions,
    };
}

function placeExpandedInners(
    layout: GraphLayout,
    groupId: string,
    plan: ExpandedGroupPlan,
): void {
    const groupEntry = layout.get(groupId);
    if (!groupEntry) return;
    const groupBBox = groupEntry.bbox;
    groupEntry.node = {
        ...groupEntry.node,
        type: "group-expanded",
        w: groupBBox.w,
        h: groupBBox.h,
    };
    groupEntry.visible = true;
    const innerOrigin = {
        x: groupBBox.x + GROUP_PADDING.left - plan.contentBBox.x,
        y: groupBBox.y + GROUP_PADDING.top - plan.contentBBox.y,
    };
    const snakeRow = groupEntry.snakeRow;
    plan.innerIds.forEach((id) => {
        const innerBBox = plan.innerPositions.get(id);
        const measured = plan.innerNodes.get(id);
        if (!innerBBox || !measured) return;
        layout.set(id, {
            bbox: {
                x: innerOrigin.x + innerBBox.x,
                y: innerOrigin.y + innerBBox.y,
                w: innerBBox.w,
                h: innerBBox.h,
            },
            node: { ...measured, metaGroup: groupId },
            visible: true,
            snakeRow,
        });
    });
}

export function buildCollapsedLayout(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Iterable<Cytoscape.EdgeDataDefinition>,
    expanded: Set<string>,
    rankDir: "TB" | "LR",
    pipelineOrders: Map<string, string[]> = new Map(),
    wrapRows = true,
    preferChronology = true,
): GraphLayout {
    const layout: GraphLayout = new Map();
    const edgeList = edgesToList(edges);

    // Pre-measure every expanded meta so the outer layered DAG reserves the real
    // footprint up front. Sequential expandGroupInLayout pushes (especially LR)
    // cascade across multi-expand and leave a sparse/chaotic Fit view.
    const plans = new Map<string, ExpandedGroupPlan>();
    sortIds(Array.from(expanded)).forEach((groupId) => {
        const plan = planExpandedGroup(
            groupId,
            nodes,
            edgeList,
            rankDir,
            pipelineOrders.get(groupId) ?? [],
            wrapRows,
            preferChronology,
        );
        if (plan) plans.set(groupId, plan);
    });

    const layoutNodes = new Map<string, MeasuredNode>();
    nodes.forEach((data, id) => {
        if (data.parent || data.metaGroup) return;
        const plan = plans.get(id);
        if (plan) {
            const base = measureNode({
                ...data,
                id,
                type: "group",
            });
            layoutNodes.set(id, {
                ...base,
                type: "group-expanded",
                w: plan.size.w,
                h: plan.size.h,
            });
            return;
        }
        // reprocessData marks expanded metas as group-expanded. Seed a collapsed
        // footprint when we have no inner plan so incremental expand can still grow.
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

    // Expanded metas are visual frames: dependency edges attach to inners / boundary
    // tables. Project member endpoints onto their meta so the outer DAG still
    // ranks FindBestModel (etc.) on the correct side instead of rank 0.
    const memberToGroup = new Map<string, string>();
    nodes.forEach((data, id) => {
        if (typeof data.metaGroup === "string") {
            memberToGroup.set(id, data.metaGroup);
        }
    });
    const outerEdges: LayoutEdge[] = [];
    const seenOuter = new Set<string>();
    edgeList.forEach((edge) => {
        const source = memberToGroup.get(edge.source) ?? edge.source;
        const target = memberToGroup.get(edge.target) ?? edge.target;
        if (!layoutNodes.has(source) || !layoutNodes.has(target) || source === target) return;
        const key = `${source}\0${target}\0${edge.sequential ? 1 : 0}\0${edge.synthetic ? 1 : 0}`;
        if (seenOuter.has(key)) return;
        seenOuter.add(key);
        outerEdges.push({ ...edge, source, target });
    });

    const positions = layoutLayeredDag(
        layoutNodes,
        outerEdges,
        rankDir,
        buildRankEdges(layoutNodes, outerEdges),
        wrapRows,
        preferChronology,
    );
    const snakeRows = getSnakeRowsForPositions(positions);
    layoutNodes.forEach((node, id) => {
        const bbox = positions.get(id);
        if (!bbox) return;
        layout.set(id, {
            bbox,
            node,
            visible: true,
            snakeRow: snakeRows?.get(id),
        });
    });

    plans.forEach((plan, groupId) => {
        placeExpandedInners(layout, groupId, plan);
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

/** One zigzag (snake) row: travel direction + padded row box in model space. */
export type SnakeRowGuide = {
    rtl: boolean;
    /** Padded axis-aligned box covering the row's top-level nodes. */
    x1: number;
    y1: number;
    x2: number;
    y2: number;
    /** Padded leftmost node column — left-turn tube. */
    leftX1: number;
    leftX2: number;
    /** Padded rightmost node column — right-turn tube. */
    rightX1: number;
    rightX2: number;
};

/**
 * Recover snake rows from a finished layout (even L→R, odd R→L).
 * Prefers `snakeRow` stamped by LR wrap placement — Y-clustering alone splits
 * stacked column nodes into fake rows and breaks the S-corridor.
 */
export function computeSnakeRowGuides(layout: GraphLayout): SnakeRowGuide[] {
    type Top = { cx: number; cy: number; w: number; h: number; snakeRow?: number };
    const tops: Top[] = [];
    let hasSnakeRows = false;
    layout.forEach((entry) => {
        if (!entry.visible) return;
        if (entry.node.metaGroup) return;
        const c = bboxCenter(entry.bbox);
        if (entry.snakeRow !== undefined) hasSnakeRows = true;
        tops.push({
            cx: c.x,
            cy: c.y,
            w: entry.bbox.w,
            h: entry.bbox.h,
            snakeRow: entry.snakeRow,
        });
    });
    if (tops.length < 1) return [];

    const rowBuckets = new Map<number, Top[]>();
    if (hasSnakeRows) {
        tops.forEach((n) => {
            if (n.snakeRow === undefined) return;
            const bucket = rowBuckets.get(n.snakeRow) ?? [];
            bucket.push(n);
            rowBuckets.set(n.snakeRow, bucket);
        });
    } else {
        // Fallback for non-wrapped layouts: cluster by Y (best-effort).
        tops.sort((a, b) => a.cy - b.cy || a.cx - b.cx);
        const medianH =
            [...tops].map((n) => n.h).sort((a, b) => a - b)[Math.floor(tops.length / 2)] ?? 120;
        const rowTol = Math.max(ROW_SEP * 0.45, medianH * 0.55);
        let rowIndex = 0;
        tops.forEach((n) => {
            const last = rowBuckets.get(rowIndex);
            if (last && Math.abs(last[0].cy - n.cy) <= rowTol) {
                last.push(n);
                return;
            }
            if (last) rowIndex += 1;
            rowBuckets.set(rowIndex, [n]);
        });
    }

    const padX = 28;
    const padY = 22;
    const colPad = 18;
    return Array.from(rowBuckets.keys())
        .sort((a, b) => a - b)
        .map((rowIndex) => {
            const row = rowBuckets.get(rowIndex) ?? [];
            const rtl = rowIndex % 2 === 1;
            let x1 = Infinity;
            let y1 = Infinity;
            let x2 = -Infinity;
            let y2 = -Infinity;
            let left = row[0];
            let right = row[0];
            row.forEach((n) => {
                x1 = Math.min(x1, n.cx - n.w / 2);
                y1 = Math.min(y1, n.cy - n.h / 2);
                x2 = Math.max(x2, n.cx + n.w / 2);
                y2 = Math.max(y2, n.cy + n.h / 2);
                if (n.cx - n.w / 2 < left.cx - left.w / 2) left = n;
                if (n.cx + n.w / 2 > right.cx + right.w / 2) right = n;
            });
            return {
                rtl,
                x1: x1 - padX,
                y1: y1 - padY,
                x2: x2 + padX,
                y2: y2 + padY,
                leftX1: left.cx - left.w / 2 - colPad,
                leftX2: left.cx + left.w / 2 + colPad,
                rightX1: right.cx - right.w / 2 - colPad,
                rightX2: right.cx + right.w / 2 + colPad,
            };
        });
}

/**
 * Closed S-shaped polygon around zigzag rows: padded row boxes joined by
 * turn tubes over the rightmost nodes (after L→R) or leftmost nodes (after R→L).
 * Vertical connectors sit on those end columns so the snake direction is obvious.
 */
export function computeSnakeBoundaryPolygon(rows: SnakeRowGuide[]): Array<{ x: number; y: number }> {
    if (!rows.length) return [];
    if (rows.length === 1) {
        const r = rows[0];
        return [
            { x: r.x1, y: r.y1 },
            { x: r.x2, y: r.y1 },
            { x: r.x2, y: r.y2 },
            { x: r.x1, y: r.y2 },
        ];
    }

    /** Turn tube anchored at the outer edge, width ≈ end-column node. */
    const turnBounds = (i: number): { xInner: number; xOuter: number } => {
        const a = rows[i];
        const b = rows[i + 1];
        if (i % 2 === 0) {
            const xOuter = Math.max(a.x2, b.x2);
            const colW = Math.max(a.rightX2 - a.rightX1, b.rightX2 - b.rightX1, 72);
            return { xInner: xOuter - colW, xOuter };
        }
        const xOuter = Math.min(a.x1, b.x1);
        const colW = Math.max(a.leftX2 - a.leftX1, b.leftX2 - b.leftX1, 72);
        return { xInner: xOuter + colW, xOuter };
    };
    const rightTurn = (i: number) => i % 2 === 0;

    const pts: Array<{ x: number; y: number }> = [];
    const n = rows.length;
    const first = rows[0];
    const last = rows[n - 1];

    // Clockwise: top → right chain (left-turn notches) → bottom → left chain (right-turn notches).
    pts.push({ x: first.x1, y: first.y1 });
    pts.push({ x: first.x2, y: first.y1 });

    for (let i = 0; i < n - 1; i += 1) {
        const a = rows[i];
        const b = rows[i + 1];
        const { xInner, xOuter } = turnBounds(i);
        if (rightTurn(i)) {
            // Vertical down the rightmost-node column, through the inter-row gap.
            pts.push({ x: xOuter, y: a.y1 });
            pts.push({ x: xOuter, y: a.y2 });
            pts.push({ x: xOuter, y: b.y1 });
            if (Math.abs(b.x2 - xOuter) > 0.5) {
                pts.push({ x: b.x2, y: b.y1 });
            }
        } else {
            // Left turn: notch on the right; outer vertical is on the left (return path).
            pts.push({ x: a.x2, y: a.y2 });
            pts.push({ x: xInner, y: a.y2 });
            pts.push({ x: xInner, y: b.y1 });
            pts.push({ x: b.x2, y: b.y1 });
        }
    }

    pts.push({ x: last.x2, y: last.y2 });
    pts.push({ x: last.x1, y: last.y2 });

    for (let i = n - 1; i >= 1; i -= 1) {
        const b = rows[i];
        const a = rows[i - 1];
        const { xInner, xOuter } = turnBounds(i - 1);
        if (rightTurn(i - 1)) {
            // Right turn: notch on the left between rows.
            pts.push({ x: b.x1, y: b.y1 });
            pts.push({ x: xInner, y: b.y1 });
            pts.push({ x: xInner, y: a.y2 });
            pts.push({ x: a.x1, y: a.y2 });
        } else {
            // Vertical up the leftmost-node column, through the inter-row gap.
            pts.push({ x: xOuter, y: b.y1 });
            pts.push({ x: xOuter, y: a.y2 });
            if (Math.abs(a.x1 - xOuter) > 0.5) {
                pts.push({ x: a.x1, y: a.y2 });
            }
        }
    }

    pts.push({ x: first.x1, y: first.y1 });
    return simplifyOrthogonal(pts);
}

/**
 * Vertical turn tubes between consecutive snake rows (rightmost after L→R,
 * leftmost after R→L). Drawn explicitly so the zigzag drop is obvious.
 */
export function computeSnakeTurnConnectors(
    rows: SnakeRowGuide[],
): Array<{ x1: number; y1: number; x2: number; y2: number }> {
    const out: Array<{ x1: number; y1: number; x2: number; y2: number }> = [];
    for (let i = 0; i < rows.length - 1; i += 1) {
        const a = rows[i];
        const b = rows[i + 1];
        const y1 = a.y2;
        const y2 = b.y1;
        if (y2 - y1 < 1) continue;
        if (i % 2 === 0) {
            const x2 = Math.max(a.x2, b.x2);
            const colW = Math.max(a.rightX2 - a.rightX1, b.rightX2 - b.rightX1, 72);
            out.push({ x1: x2 - colW, y1, x2, y2 });
        } else {
            const x1 = Math.min(a.x1, b.x1);
            const colW = Math.max(a.leftX2 - a.leftX1, b.leftX2 - b.leftX1, 72);
            out.push({ x1, y1, x2: x1 + colW, y2 });
        }
    }
    return out;
}

/** Drop consecutive duplicate / colinear orthogonal points. */
function simplifyOrthogonal(points: Array<{ x: number; y: number }>): Array<{ x: number; y: number }> {
    if (points.length < 2) return points;
    const out: Array<{ x: number; y: number }> = [];
    const push = (p: { x: number; y: number }) => {
        const prev = out[out.length - 1];
        if (prev && Math.abs(prev.x - p.x) < 0.5 && Math.abs(prev.y - p.y) < 0.5) return;
        const a = out[out.length - 2];
        const b = out[out.length - 1];
        if (a && b) {
            const colinearH = Math.abs(a.y - b.y) < 0.5 && Math.abs(b.y - p.y) < 0.5;
            const colinearV = Math.abs(a.x - b.x) < 0.5 && Math.abs(b.x - p.x) < 0.5;
            if (colinearH || colinearV) {
                out[out.length - 1] = p;
                return;
            }
        }
        out.push(p);
    };
    points.forEach(push);
    return out;
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

const morphRafStore = new WeakMap<Cytoscape.Core, Map<string, number>>();
const pendingMorphStore = new WeakMap<
    Cytoscape.Core,
    Map<
        string,
        {
            to: BBox;
            toCenter: { x: number; y: number };
            opacityTo?: number;
            safetyTimer?: number;
            intervalTimer?: number;
            onComplete?: () => void;
        }
    >
>();

function trackMorphRaf(cy: Cytoscape.Core, nodeId: string, id: number): void {
    let map = morphRafStore.get(cy);
    if (!map) {
        map = new Map();
        morphRafStore.set(cy, map);
    }
    map.set(nodeId, id);
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
    // Do NOT call morph.onComplete — a newer sync owns the transition; firing
    // the old callback applied a stale layout on top of the new one.
    const pending = pendingMorphStore.get(cy);
    if (!pending?.size) return;
    pending.forEach((morph, nodeId) => {
        if (morph.safetyTimer != null) {
            window.clearTimeout(morph.safetyTimer);
        }
        if (morph.intervalTimer != null) {
            window.clearInterval(morph.intervalTimer);
        }
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
    });
    pending.clear();
}

function easeInOutCubic(t: number): number {
    return t < 0.5 ? 4 * t * t * t : 1 - (-2 * t + 2) ** 3 / 2;
}

/**
 * Animate group frame size. During the tween we bypass stylesheet width/height
 * mappers with style() so we don't fire cy `data` every frame (that storms HTML
 * label + overlay handlers and can stall the morph). On finish we clear the
 * bypass and commit boxW/boxH for steady-state mapping.
 *
 * Driven by setInterval (not rAF): cytoscape/overlay work cancels stray
 * animation frames and was leaving expand stuck mid-size.
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

    node.position(fromCenter);
    node.style({ width: from.w, height: from.h });
    if (fadeOpacity) {
        node.style("opacity", options.opacityFrom!);
    }

    let finished = false;
    let intervalTimer = 0;
    let safetyTimer = 0;

    const finish = () => {
        if (finished) return;
        // Superseded by stopMorphAnimations — do not run onComplete.
        if (!pending.has(nodeId)) {
            finished = true;
            window.clearInterval(intervalTimer);
            window.clearTimeout(safetyTimer);
            return;
        }
        finished = true;
        window.clearInterval(intervalTimer);
        window.clearTimeout(safetyTimer);
        pending.delete(nodeId);
        morphRafStore.get(cy)?.delete(nodeId);
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

    pending.set(nodeId, {
        to,
        toCenter,
        opacityTo: fadeOpacity ? options.opacityTo : undefined,
        onComplete: options.onComplete,
    });

    const tick = () => {
        if (finished || !pending.has(nodeId)) return;
        if (cy.destroyed() || node.removed()) {
            finish();
            return;
        }
        const now = performance.now();
        const t = Math.min(1, (now - start) / duration);
        const e = easeInOutCubic(t);
        const w = from.w + (to.w - from.w) * e;
        const h = from.h + (to.h - from.h) * e;
        const x = fromCenter.x + (toCenter.x - fromCenter.x) * e;
        const y = fromCenter.y + (toCenter.y - fromCenter.y) * e;

        const cur = node.position();
        if (cur.x !== x || cur.y !== y) {
            node.position({ x, y });
        }
        node.style({ width: w, height: h });
        if (fadeOpacity) {
            const op = options.opacityFrom! + (options.opacityTo! - options.opacityFrom!) * e;
            node.style("opacity", op);
        }

        if (t >= 1) {
            finish();
        }
    };

    intervalTimer = window.setInterval(tick, 16);
    safetyTimer = window.setTimeout(finish, duration + 80);
    const entry = pending.get(nodeId);
    if (entry) {
        entry.safetyTimer = safetyTimer;
        entry.intervalTimer = intervalTimer;
    }
    tick();
}

export function stopLayoutAnimations(cy: Cytoscape.Core): void {
    if (cy.destroyed()) return;
    stopHtmlOpacityAnimations(cy);
    stopMorphAnimations(cy);
    cy.stop(true, true);
    // A superseded transition may have left the overlay paused.
    resumeInternalEdgeOverlayPaths(cy);
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
    // stopLayoutAnimations resumes the overlay; pause again for this tween.
    pauseInternalEdgeOverlayPaths(cy);

    const toCenters = layoutToCenters(toLayout);
    const fadeIn = options?.fadeIn ?? new Set<string>();
    const fadeOut = options?.fadeOut ?? new Set<string>();
    const morphBoxes = options?.morphBoxes ?? new Map<string, { from: BBox; to: BBox }>();
    const groupMorphing = morphBoxes.size > 0;

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

    // Apply size metadata. For group morph, keep current positions so we can
    // tween neighbors from→to (applyLayoutToCy would snap them and look like
    // the graph "flew away"). Non-morph transitions still snap via applyLayoutToCy.
    if (groupMorphing) {
        cy.batch(() => {
            toLayout.forEach((entry, id) => {
                if (!entry.visible) return;
                const ele = cy.getElementById(id);
                if (ele.empty()) return;
                const node = ele as Cytoscape.NodeSingular;
                if (entry.node.type === "group" || entry.node.type === "group-expanded") {
                    if (morphBoxes.has(id)) return;
                    node.removeStyle("width");
                    node.removeStyle("height");
                    node.data({ boxW: entry.bbox.w, boxH: entry.bbox.h });
                }
            });
        });
    } else {
        applyLayoutToCy(cy, toLayout);
    }

    // Group expand/collapse: morph the blue frame AND slide neighbors so the
    // zigzag rebuild reads as a coordinated shift (camera stays put).
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
        const moved =
            Boolean(from) &&
            (Math.abs(from!.x - target.x) > 0.5 || Math.abs(from!.y - target.y) > 0.5);
        const move = Boolean(morph) || moved || (!groupMorphing && Boolean(from));
        if (!move && !isFadeIn && !isFadeOut) {
            return;
        }
        specs.push({
            id,
            target,
            fadeIn: isFadeIn,
            fadeOut: isFadeOut,
            move,
            morph,
        });
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

    const finishTransition = () => {
        resumeInternalEdgeOverlayPaths(cy);
        options?.onComplete?.();
    };

    if (!totalWork) {
        finishTransition();
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
            finishTransition();
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
            // Match animateNodeBoxMorph: style bypass for the start size.
            nodeEl.style({ width: morph.from.w, height: morph.from.h });
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
                    onComplete: () => {
                        if (isNativeGroupFrame) {
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
            const explicit = resolveOpacityTiming(
                id,
                isFadeIn ? options?.fadeInTiming : options?.fadeOutTiming,
            );
            // Reveal expand inners after the frame has mostly grown so they aren't
            // visible outside the still-collapsed blue box.
            const morphFadeInDefaults =
                groupMorphing && isFadeIn && !morph
                    ? { delay: Math.round(ANIMATION_MS * 0.4), duration: Math.round(ANIMATION_MS * 0.6) }
                    : {};
            const timing = { ...morphFadeInDefaults, ...explicit };
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

export type FitViewportOptions = {
    /** @deprecated Ignored — Fit always shows the full target without a zoom floor. */
    readable?: boolean;
};

/**
 * Fit the camera to show the full graph. Always fit everything — zooming only to
 * expanded blues left the rest of the pipeline off-screen (and after browser-back
 * restore made blues look like they "flew" outside the graph).
 */
export function fitGraphViewport(cy: Cytoscape.Core, _options?: FitViewportOptions): void {
    const target = cy.elements();
    cy.fit(target, FIT_PADDING);
    const fitZoom = cy.zoom();
    cy.minZoom(Math.min(0.05, fitZoom * 0.3));
    cy.maxZoom(Math.max(2.5, fitZoom * 6));
}

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
 * Target pan/zoom to fit `bb` (model coords) into the viewport, mirroring
 * `fitGraphViewport` but *without* mutating the camera — so callers can animate.
 */
export function fitViewportForBBox(
    cy: Cytoscape.Core,
    bb: BBox,
    _options?: FitViewportOptions,
): Viewport | null {
    const W = cy.width();
    const H = cy.height();
    if (!bb.w || !bb.h || !W || !H) return null;

    const rawZoom = Math.min((W - 2 * FIT_PADDING) / bb.w, (H - 2 * FIT_PADDING) / bb.h);
    const minZoom = Math.min(0.05, rawZoom * 0.3);
    const maxZoom = Math.max(2.5, rawZoom * 6);
    const zoom = Math.max(minZoom, Math.min(maxZoom, rawZoom));
    const pan = {
        x: W / 2 - zoom * (bb.x + bb.w / 2),
        y: H / 2 - zoom * (bb.y + bb.h / 2),
    };
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
    options?: FitViewportOptions,
): void {
    const bb = layoutContentBBox(layout);
    if (!bb) return;
    const target = fitViewportForBBox(cy, bb, options);
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
