import type {
    LabelGraphEdge,
    LabelGraphPayload,
    LabelInterleaving,
    LabelSharedRelation,
    StageEdge,
    StageItem,
} from "../../../types/ops";

export type { StageEdge, StageItem };

export type LabelGraphMode = "overview" | "compact";

export type LayoutNodeKind = "node" | "container" | "interleaved-group";

export type LayoutNode = {
    id: string;
    kind: LayoutNodeKind;
    x: number;
    y: number;
    width: number;
    height: number;
    nodeId: string;
    label?: string;
    status?: string;
    childIds?: string[];
    interleavedLabelIds?: string[];
};

export type LayoutEdgeKind = "order" | "exact-order" | "secondary";

export type LayoutEdge = {
    id: string;
    source: string;
    target: string;
    kind: LayoutEdgeKind;
    visibleByDefault: boolean;
    showWhenSelected?: string[];
    replacesEdgeId?: string;
    x1: number;
    y1: number;
    x2: number;
    y2: number;
};

export type EdgeHighlightLevel = "focused" | "context" | "muted";

export type SharedBracket = {
    id: string;
    a: string;
    b: string;
    x: number;
    y: number;
    width: number;
    label: string;
    visibleByDefault: boolean;
};

export type LabelGraphLayout = {
    width: number;
    height: number;
    nodes: LayoutNode[];
    orderEdges: LayoutEdge[];
    exactEdges: LayoutEdge[];
    sharedBrackets: SharedBracket[];
    interleavings: LabelInterleaving[];
};

const LAYOUT = {
    overview: {
        nodeWidth: 132,
        nodeHeight: 72,
        childHeight: 64,
        containerPadding: 20,
        containerInnerTop: 48,
        containerBottomPad: 18,
        gapX: 52,
        canvasPad: 24,
        interleavedHeight: 120,
        interleavedLaneGap: 8,
    },
    compact: {
        nodeWidth: 112,
        nodeHeight: 56,
        childHeight: 48,
        containerPadding: 14,
        containerInnerTop: 40,
        containerBottomPad: 14,
        gapX: 36,
        canvasPad: 16,
        interleavedHeight: 96,
        interleavedLaneGap: 6,
    },
} as const;

const DEFAULT_STAGE_CONTAINMENT_RULES: { parent: string; children: string[] }[] = [
    { parent: "train", children: ["train-prepare", "inference", "count-metrics"] },
];

function stepNamesFromStage(stage: StageItem): string[] {
    return (stage.steps as { name?: string }[])
        .map((s) => s.name)
        .filter((n): n is string => Boolean(n));
}


function sortByOrderMin(ids: string[], orderMap: Map<string, number>): string[] {
    return [...ids].sort((a, b) => {
        const ia = orderMap.get(a) ?? Number.MAX_SAFE_INTEGER;
        const ib = orderMap.get(b) ?? Number.MAX_SAFE_INTEGER;
        if (ia !== ib) return ia - ib;
        return a.localeCompare(b);
    });
}

function edgePath(x1: number, y1: number, x2: number, y2: number): string {
    const midX = (x1 + x2) / 2;
    return `M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`;
}

type Bounds = { x: number; y: number; w: number; h: number };

function buildFallbackPayload(stages: StageItem[], stageEdges?: StageEdge[]): LabelGraphPayload {
    const stageOrder = stages.map((s) => s.stage);
    const orderMap = new Map(stageOrder.map((s, i) => [s, i]));

    const nodes = stages.map((stage, index) => ({
        id: stage.stage,
        label: stage.stage,
        status: stage.status,
        kind: "label" as "label" | "container" | "interleaved-group",
        step_ids: stepNamesFromStage(stage),
        step_count: stage.steps.length,
        parent_id: null as string | null,
        children_ids: [] as string[],
        order_min: index,
        order_max: index,
        segments: [
            {
                label_id: stage.stage,
                start_order: index,
                end_order: index,
                step_ids: stepNamesFromStage(stage),
            },
        ],
    }));

    const nodeById = new Map(nodes.map((n) => [n.id, n]));

    for (const rule of DEFAULT_STAGE_CONTAINMENT_RULES) {
        const parent = nodeById.get(rule.parent);
        if (!parent) continue;
        for (const childId of rule.children) {
            const child = nodeById.get(childId);
            if (!child) continue;
            parent.kind = "container";
            parent.children_ids = [...(parent.children_ids ?? []), childId];
            child.parent_id = rule.parent;
        }
    }

    const childrenSet = new Set(
        nodes.filter((n) => n.parent_id).map((n) => n.id),
    );
    const topLevel = sortByOrderMin(
        nodes.filter((n) => !childrenSet.has(n.id)).map((n) => n.id),
        orderMap,
    );

    const edges: LabelGraphEdge[] = [];
    for (let i = 0; i < topLevel.length - 1; i += 1) {
        edges.push({
            id: `${topLevel[i]}->${topLevel[i + 1]}`,
            source: topLevel[i],
            target: topLevel[i + 1],
            kind: "order",
            visible_by_default: true,
        });
    }

    for (const node of nodes) {
        if (node.kind !== "container" || !node.children_ids?.length) continue;
        const children = sortByOrderMin(node.children_ids, orderMap);
        for (let i = 0; i < children.length - 1; i += 1) {
            edges.push({
                id: `${children[i]}->${children[i + 1]}`,
                source: children[i],
                target: children[i + 1],
                kind: "order",
                visible_by_default: true,
            });
        }
        const parentIdx = topLevel.indexOf(node.id);
        if (parentIdx > 0) {
            const prev = topLevel[parentIdx - 1];
            const firstChild = children[0];
            edges.push({
                id: `${prev}->${firstChild}`,
                source: prev,
                target: firstChild,
                kind: "exact-order",
                visible_by_default: false,
                show_when_selected: [firstChild],
                replaces_edge_id: `${prev}->${node.id}`,
                source_scope: "node",
                target_scope: "child",
            });
        }
        if (parentIdx >= 0 && parentIdx < topLevel.length - 1) {
            const next = topLevel[parentIdx + 1];
            const lastChild = children[children.length - 1];
            edges.push({
                id: `${lastChild}->${next}`,
                source: lastChild,
                target: next,
                kind: "exact-order",
                visible_by_default: false,
                show_when_selected: [lastChild, next],
                replaces_edge_id: `${node.id}->${next}`,
                source_scope: "child",
                target_scope: "node",
            });
        }
    }

    const shared_relations: LabelSharedRelation[] = [];
    for (let i = 0; i < stages.length; i += 1) {
        for (let j = i + 1; j < stages.length; j += 1) {
            const a = stages[i].stage;
            const b = stages[j].stage;
            const setA = new Set(stepNamesFromStage(stages[i]));
            const shared = stepNamesFromStage(stages[j]).filter((n) => setA.has(n));
            if (shared.length === 0) continue;
            const isContainment =
                nodeById.get(a)?.children_ids?.includes(b) ||
                nodeById.get(b)?.children_ids?.includes(a);
            if (isContainment) continue;
            shared_relations.push({
                id: `shared-${a}-${b}`,
                a,
                b,
                shared_step_ids: shared,
                shared_count: shared.length,
                visible_by_default: false,
            });
        }
    }

    if (stageEdges?.length) {
        for (const e of stageEdges) {
            if (edges.some((edge) => edge.source === e.from && edge.target === e.to)) continue;
            edges.push({
                id: `secondary-${e.from}-${e.to}`,
                source: e.from,
                target: e.to,
                kind: "secondary",
                visible_by_default: false,
            });
        }
    }

    const containments = DEFAULT_STAGE_CONTAINMENT_RULES.flatMap((rule) =>
        rule.children
            .filter((child) => nodeById.has(child) && nodeById.has(rule.parent))
            .map((child) => ({
                parent: rule.parent,
                child,
                kind: "semantic" as const,
            })),
    );

    return {
        label_key: "stage",
        nodes,
        edges,
        containments,
        shared_relations,
        interleavings: [],
    };
}

export function resolveLabelGraph(detail: {
    stages: StageItem[];
    stage_edges?: StageEdge[];
    label_graph?: LabelGraphPayload;
}): LabelGraphPayload {
    return detail.label_graph ?? buildFallbackPayload(detail.stages, detail.stage_edges);
}

export function layoutLabelGraph(
    payload: LabelGraphPayload,
    mode: LabelGraphMode = "overview",
): LabelGraphLayout {
    const cfg = LAYOUT[mode];
    const nodeById = new Map(payload.nodes.map((n) => [n.id, n]));
    const orderMap = new Map(payload.nodes.map((n) => [n.id, n.order_min]));

    const childrenSet = new Set(
        payload.containments.map((c) => c.child),
    );
    const interleavedIds = new Set(payload.interleavings.map((i) => i.id));

    const topLevel = sortByOrderMin(
        payload.nodes.filter((n) => !childrenSet.has(n.id) && !n.parent_id).map((n) => n.id),
        orderMap,
    );

    const sequenceUnits: string[] = [];
    const seenInterleaved = new Set<string>();
    for (const id of topLevel) {
        const node = nodeById.get(id);
        if (!node) continue;
        if (node.kind === "interleaved-group") {
            if (!seenInterleaved.has(id)) {
                sequenceUnits.push(id);
                seenInterleaved.add(id);
            }
            continue;
        }
        const inter = payload.interleavings.find((i) => i.labels.includes(id));
        if (inter && !seenInterleaved.has(inter.id)) {
            sequenceUnits.push(inter.id);
            seenInterleaved.add(inter.id);
        } else if (!inter) {
            sequenceUnits.push(id);
        }
    }

    for (const inter of payload.interleavings) {
        if (!sequenceUnits.includes(inter.id)) {
            sequenceUnits.push(inter.id);
            sequenceUnits.sort((a, b) => (orderMap.get(a) ?? 0) - (orderMap.get(b) ?? 0));
        }
    }

    const containerHeightFor = (childCount: number): number =>
        childCount > 0
            ? cfg.containerInnerTop + cfg.childHeight + cfg.containerBottomPad
            : cfg.nodeHeight;

    const rowHeight = sequenceUnits.reduce<number>((acc, id) => {
        const node = nodeById.get(id);
        if (!node) return acc;
        if (node.kind === "interleaved-group") return Math.max(acc, cfg.interleavedHeight);
        if (node.kind === "container") {
            return Math.max(acc, containerHeightFor(node.children_ids?.length ?? 0));
        }
        return Math.max(acc, cfg.nodeHeight);
    }, cfg.nodeHeight);

    const layoutNodes: LayoutNode[] = [];
    const bounds = new Map<string, Bounds>();
    let cursorX = cfg.canvasPad;
    const rowY = cfg.canvasPad;

    for (const unitId of sequenceUnits) {
        const node = nodeById.get(unitId);
        if (!node) continue;

        if (node.kind === "interleaved-group") {
            const labels = node.children_ids ?? node.segments.map((s) => s.label_id);
            const uniqueLabels = Array.from(new Set(labels)).slice(0, 2);
            const laneW = cfg.nodeWidth;
            const innerW =
                uniqueLabels.length * laneW +
                Math.max(0, uniqueLabels.length - 1) * cfg.interleavedLaneGap;
            const containerW = innerW + cfg.containerPadding * 2;
            const containerH = cfg.interleavedHeight;
            const containerY = rowY + (rowHeight - containerH) / 2;

            layoutNodes.push({
                id: `interleaved-${unitId}`,
                kind: "interleaved-group",
                x: cursorX,
                y: containerY,
                width: containerW,
                height: containerH,
                nodeId: unitId,
                label: node.label,
                status: node.status,
                interleavedLabelIds: uniqueLabels,
            });
            bounds.set(unitId, { x: cursorX, y: containerY, w: containerW, h: containerH });

            let laneX = cursorX + cfg.containerPadding;
            const laneY = containerY + cfg.containerInnerTop;
            for (const labelId of uniqueLabels) {
                const member = nodeById.get(labelId);
                layoutNodes.push({
                    id: labelId,
                    kind: "node",
                    x: laneX,
                    y: laneY,
                    width: laneW,
                    height: cfg.childHeight,
                    nodeId: labelId,
                    label: member?.label ?? labelId,
                    status: member?.status,
                });
                bounds.set(labelId, { x: laneX, y: laneY, w: laneW, h: cfg.childHeight });
                laneX += laneW + cfg.interleavedLaneGap;
            }
            cursorX += containerW + cfg.gapX;
            continue;
        }

        if (node.kind === "container" && (node.children_ids?.length ?? 0) > 0) {
            const children = sortByOrderMin(node.children_ids ?? [], orderMap);
            const childW = cfg.nodeWidth;
            const innerW =
                children.length * childW + Math.max(0, children.length - 1) * cfg.gapX;
            const containerW = innerW + cfg.containerPadding * 2;
            const containerH = containerHeightFor(children.length);
            const containerY = rowY + (rowHeight - containerH) / 2;

            layoutNodes.push({
                id: `container-${unitId}`,
                kind: "container",
                x: cursorX,
                y: containerY,
                width: containerW,
                height: containerH,
                nodeId: unitId,
                label: node.label,
                status: node.status,
                childIds: children,
            });
            bounds.set(unitId, { x: cursorX, y: containerY, w: containerW, h: containerH });

            let childX = cursorX + cfg.containerPadding;
            const childY = containerY + cfg.containerInnerTop;
            for (const childId of children) {
                const child = nodeById.get(childId);
                layoutNodes.push({
                    id: childId,
                    kind: "node",
                    x: childX,
                    y: childY,
                    width: childW,
                    height: cfg.childHeight,
                    nodeId: childId,
                    label: child?.label ?? childId,
                    status: child?.status,
                });
                bounds.set(childId, {
                    x: childX,
                    y: childY,
                    w: childW,
                    h: cfg.childHeight,
                });
                childX += childW + cfg.gapX;
            }
            cursorX += containerW + cfg.gapX;
        } else if (!interleavedIds.has(unitId) && !node.parent_id) {
            const nodeY = rowY + (rowHeight - cfg.nodeHeight) / 2;
            layoutNodes.push({
                id: unitId,
                kind: "node",
                x: cursorX,
                y: nodeY,
                width: cfg.nodeWidth,
                height: cfg.nodeHeight,
                nodeId: unitId,
                label: node.label,
                status: node.status,
            });
            bounds.set(unitId, {
                x: cursorX,
                y: nodeY,
                w: cfg.nodeWidth,
                h: cfg.nodeHeight,
            });
            cursorX += cfg.nodeWidth + cfg.gapX;
        }
    }

    const resolveAnchor = (nodeId: string, side: "left" | "right"): { x: number; y: number } | null => {
        const b = bounds.get(nodeId);
        if (!b) {
            const node = nodeById.get(nodeId);
            if (node?.parent_id) {
                const parentBounds = bounds.get(node.parent_id);
                if (parentBounds) {
                    return {
                        x: side === "right" ? parentBounds.x + parentBounds.w : parentBounds.x,
                        y: parentBounds.y + parentBounds.h / 2,
                    };
                }
            }
            return null;
        }
        return {
            x: side === "right" ? b.x + b.w : b.x,
            y: b.y + b.h / 2,
        };
    };

    const buildLayoutEdge = (edge: LabelGraphEdge): LayoutEdge | null => {
        const from = resolveAnchor(edge.source, "right");
        const to = resolveAnchor(edge.target, "left");
        if (!from || !to) return null;
        return {
            id: edge.id,
            source: edge.source,
            target: edge.target,
            kind: edge.kind,
            visibleByDefault: edge.visible_by_default,
            showWhenSelected: edge.show_when_selected,
            replacesEdgeId: edge.replaces_edge_id,
            x1: from.x,
            y1: from.y,
            x2: to.x,
            y2: to.y,
        };
    };

    const orderEdges: LayoutEdge[] = [];
    const exactEdges: LayoutEdge[] = [];

    for (const edge of payload.edges) {
        const le = buildLayoutEdge(edge);
        if (!le) continue;
        if (edge.kind === "exact-order") {
            exactEdges.push(le);
        } else if (edge.kind === "order") {
            orderEdges.push(le);
        }
    }

    const sharedBrackets: SharedBracket[] = [];
    for (const rel of payload.shared_relations) {
        const bA = bounds.get(rel.a);
        const bB = bounds.get(rel.b);
        if (!bA || !bB) continue;
        const left = bA.x < bB.x ? bA : bB;
        const right = bA.x < bB.x ? bB : bA;
        const y = Math.max(left.y + left.h, right.y + right.h) + 18;
        sharedBrackets.push({
            id: rel.id,
            a: rel.a,
            b: rel.b,
            x: left.x + left.w / 2,
            y,
            width: right.x + right.w / 2 - (left.x + left.w / 2),
            label: rel.shared_count > 0 ? `shared · ${rel.shared_count}` : "shared",
            visibleByDefault: rel.visible_by_default,
        });
    }

    const allBounds = Array.from(bounds.values());
    const maxX = Math.max(...allBounds.map((b) => b.x + b.w), cfg.canvasPad);
    const maxY = Math.max(
        ...allBounds.map((b) => b.y + b.h),
        ...sharedBrackets.map((b) => b.y + 20),
        cfg.canvasPad,
    );

    return {
        width: maxX + cfg.canvasPad,
        height: maxY + cfg.canvasPad,
        nodes: layoutNodes,
        orderEdges,
        exactEdges,
        sharedBrackets,
        interleavings: payload.interleavings,
    };
}

export function nodeToTopLevel(payload: LabelGraphPayload): Map<string, string> {
    const map = new Map<string, string>();
    for (const node of payload.nodes) {
        if (node.parent_id) {
            map.set(node.id, node.parent_id);
        } else {
            map.set(node.id, node.id);
        }
    }
    for (const inter of payload.interleavings) {
        for (const label of inter.labels) {
            map.set(label, inter.id);
        }
        map.set(inter.id, inter.id);
    }
    return map;
}

export function isEdgeVisible(
    edge: LayoutEdge,
    selectedLabel: string | null | undefined,
    hoveredNodeId: string | null | undefined,
): boolean {
    if (edge.visibleByDefault) return true;
    if (!selectedLabel && !hoveredNodeId) return false;
    if (edge.showWhenSelected?.length) {
        const direct = [selectedLabel, hoveredNodeId].filter(Boolean) as string[];
        return edge.showWhenSelected.some((id) => direct.includes(id));
    }
    const focus = [selectedLabel, hoveredNodeId].filter(Boolean) as string[];
    return focus.some((id) => id === edge.source || id === edge.target);
}

export function isSharedVisible(
    bracket: SharedBracket,
    selectedLabel: string | null | undefined,
    hoveredNodeId: string | null | undefined,
): boolean {
    if (bracket.visibleByDefault) return true;
    if (!selectedLabel && !hoveredNodeId) return false;
    const focus = [selectedLabel, hoveredNodeId].filter(Boolean) as string[];
    return focus.some((id) => id === bracket.a || id === bracket.b);
}

export function getEdgeHighlightLevel(
    edge: LayoutEdge,
    selectedLabel: string | null | undefined,
    activeIds: Set<string>,
): EdgeHighlightLevel {
    if (!selectedLabel && activeIds.size === 0) return "context";

    if (edge.source === selectedLabel || edge.target === selectedLabel) {
        return "focused";
    }

    if (
        edge.kind === "exact-order" &&
        selectedLabel &&
        edge.showWhenSelected?.includes(selectedLabel)
    ) {
        return "focused";
    }

    if (activeIds.has(edge.source) && activeIds.has(edge.target)) {
        return "context";
    }

    if (selectedLabel || activeIds.size > 0) {
        return "muted";
    }

    return "context";
}

export { edgePath, buildFallbackPayload as buildFallbackLabelGraphFromStages };
