import Cytoscape from "cytoscape";
import {
    buildCollapsedLayout,
    cloneLayout,
    GraphLayout,
} from "./incrementalLayout";

/** Gap between packed label columns. */
const COLUMN_GAP = 180;
/** Gap kept between occupied ranks inside one label after empty ranks are removed. */
const RANK_GAP = 48;
/**
 * Roots whose left edges differ by less than this are treated as one rank so
 * within-rank relative X is preserved while empty ranks can be compressed.
 */
const RANK_CLUSTER_EPS = 64;

function labelValues(
    data: Cytoscape.NodeDataDefinition | undefined,
    labelKey: string,
): string[] {
    const labels = (data?.labels as string[][] | undefined) ?? [];
    return labels
        .filter((label) => label.length >= 2 && label[0] === labelKey)
        .map((label) => label[1]);
}

function primaryLabels(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Iterable<Cytoscape.EdgeDataDefinition>,
    labelKey: string,
): Map<string, string> {
    const result = new Map<string, string>();
    nodes.forEach((data, id) => {
        const direct = labelValues(data, labelKey);
        if (direct.length) result.set(id, direct[0]);
    });

    const edgeList = Array.from(edges);
    // Tables do not carry step labels. Prefer their producer's primary label,
    // then fall back to a consumer. A few passes cover table/group boundaries.
    for (let pass = 0; pass < 3; pass += 1) {
        edgeList.forEach((edge) => {
            const source = edge.source as string;
            const target = edge.target as string;
            if (!result.has(target) && nodes.get(target)?.type === "table" && result.has(source)) {
                result.set(target, result.get(source)!);
            }
            if (!result.has(source) && nodes.get(source)?.type === "table" && result.has(target)) {
                result.set(source, result.get(target)!);
            }
        });
    }
    return result;
}

function rootId(
    id: string,
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
): string {
    let current = id;
    const seen = new Set<string>();
    while (!seen.has(current)) {
        seen.add(current);
        const data = nodes.get(current);
        const parent = (data?.parent ?? data?.metaGroup) as string | undefined;
        if (!parent || !nodes.has(parent)) break;
        current = parent;
    }
    return current;
}

function transposeCenters(layout: GraphLayout): GraphLayout {
    const next = cloneLayout(layout);
    next.forEach((entry, id) => {
        const cx = entry.bbox.x + entry.bbox.w / 2;
        const cy = entry.bbox.y + entry.bbox.h / 2;
        next.set(id, {
            ...entry,
            bbox: {
                x: cy - entry.bbox.w / 2,
                y: cx - entry.bbox.h / 2,
                w: entry.bbox.w,
                h: entry.bbox.h,
            },
        });
    });
    return next;
}

type RankCluster = {
    ids: string[];
    minX: number;
    maxX: number;
};

/** Group roots that sit on (approximately) the same LR rank. */
function clusterByRank(ids: string[], layout: GraphLayout): RankCluster[] {
    const sorted = [...ids]
        .filter((id) => layout.has(id))
        .sort((a, b) => {
            const ea = layout.get(a)!;
            const eb = layout.get(b)!;
            return ea.bbox.x - eb.bbox.x || ea.bbox.y - eb.bbox.y || a.localeCompare(b);
        });
    if (!sorted.length) return [];

    const clusters: RankCluster[] = [];
    let current: RankCluster | null = null;
    sorted.forEach((id) => {
        const entry = layout.get(id)!;
        // Same-rank roots share nearly the same left edge. Compare against the
        // cluster origin (minX), not maxX — otherwise the next occupied rank
        // (only RANK_SEP away from the previous right edge) gets merged in and
        // empty-rank compression never runs between adjacent steps.
        if (!current || entry.bbox.x - current.minX > RANK_CLUSTER_EPS) {
            current = {
                ids: [id],
                minX: entry.bbox.x,
                maxX: entry.bbox.x + entry.bbox.w,
            };
            clusters.push(current);
            return;
        }
        current.ids.push(id);
        current.minX = Math.min(current.minX, entry.bbox.x);
        current.maxX = Math.max(current.maxX, entry.bbox.x + entry.bbox.w);
    });
    return clusters;
}

/**
 * Build the full graph once, pack label groups left-to-right, and compress only
 * empty ranks inside each label. Occupied ranks keep their internal order and
 * relative geometry; Y is untouched.
 */
export function buildLabelColumnLayout(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Iterable<Cytoscape.EdgeDataDefinition>,
    expanded: Set<string>,
    orientation: "LR" | "TB",
    labelKey: string,
    preferredLabelOrder: string[] = [],
    pipelineOrders: Map<string, string[]> = new Map(),
): GraphLayout {
    const edgeList = Array.from(edges);
    const base = buildCollapsedLayout(nodes, edgeList, expanded, "LR", pipelineOrders);
    const primary = primaryLabels(nodes, edgeList, labelKey);

    const roots = Array.from(base.keys()).filter((id) => rootId(id, nodes) === id);
    const rootsByLabel = new Map<string, string[]>();
    roots.forEach((id) => {
        const label = primary.get(id) ?? "__unlabeled__";
        const bucket = rootsByLabel.get(label) ?? [];
        bucket.push(id);
        rootsByLabel.set(label, bucket);
    });

    const firstX = (label: string): number =>
        Math.min(
            ...((rootsByLabel.get(label) ?? []).map((id) => base.get(id)?.bbox.x ?? 0)),
        );
    const preferredIndex = new Map(preferredLabelOrder.map((label, index) => [label, index]));
    const labels = Array.from(rootsByLabel.keys()).sort((a, b) => {
        if (a === "__unlabeled__") return 1;
        if (b === "__unlabeled__") return -1;
        const ai = preferredIndex.get(a);
        const bi = preferredIndex.get(b);
        if (ai != null || bi != null) {
            return (ai ?? Number.MAX_SAFE_INTEGER) - (bi ?? Number.MAX_SAFE_INTEGER);
        }
        return firstX(a) - firstX(b) || a.localeCompare(b);
    });

    const rootDelta = new Map<string, number>();
    let cursorX = 0;
    labels.forEach((label) => {
        const clusters = clusterByRank(rootsByLabel.get(label) ?? [], base);
        if (!clusters.length) return;

        let rankCursor = cursorX;
        clusters.forEach((cluster, index) => {
            const delta = rankCursor - cluster.minX;
            cluster.ids.forEach((id) => rootDelta.set(id, delta));
            const width = cluster.maxX - cluster.minX;
            rankCursor += width + (index < clusters.length - 1 ? RANK_GAP : 0);
        });
        cursorX = rankCursor + COLUMN_GAP;
    });

    const packed = cloneLayout(base);
    packed.forEach((entry, id) => {
        const delta = rootDelta.get(rootId(id, nodes)) ?? 0;
        packed.set(id, {
            ...entry,
            bbox: { ...entry.bbox, x: entry.bbox.x + delta },
        });
    });

    return orientation === "TB" ? transposeCenters(packed) : packed;
}
