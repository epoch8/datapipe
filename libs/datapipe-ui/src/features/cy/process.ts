import { GraphData, MetaNode, TransformNode } from "../../types";
import Cytoscape from "cytoscape";

type TableOrderSource = "consumer" | "producer";
type PipelineNode = MetaNode | TransformNode;

function tableOrderKey(
    baseOrderKey: string | undefined,
    role: "in" | "out",
    index: number,
): string | undefined {
    if (!baseOrderKey) return undefined;
    return `${baseOrderKey}.${role}.${String(index).padStart(4, "0")}`;
}

function shouldReplaceTableOrder(
    existing: Cytoscape.NodeDataDefinition | undefined,
    nextOrderKey: string | undefined,
    source: TableOrderSource,
): boolean {
    if (!nextOrderKey) return false;

    const currentOrderKey = existing?.pipelineOrderKey as string | undefined;
    const currentSource = existing?.tableOrderSource as TableOrderSource | undefined;

    if (!currentOrderKey) return true;

    // Output tables should stay near the transform/meta-step that produced them.
    // A later consumer must not pull a produced table sideways across the graph.
    if (source === "producer" && currentSource !== "producer") return true;
    if (source === "consumer" && currentSource === "producer") return false;

    // Among producers, or among source-only consumer anchors, keep the earliest
    // pipeline occurrence for deterministic stable ordering.
    return nextOrderKey.localeCompare(currentOrderKey) < 0;
}

/** Prefer marking an existing hop as sequential; otherwise add a synthetic dashed hop. */
function ensureSequentialEdge(
    edges: Set<Cytoscape.EdgeDataDefinition>,
    source: string,
    target: string,
    extra: Partial<Cytoscape.EdgeDataDefinition> = {},
): void {
    let found: Cytoscape.EdgeDataDefinition | null = null;
    for (const edge of Array.from(edges)) {
        if (edge.source === source && edge.target === target) {
            found = edge;
            break;
        }
    }
    if (found) {
        if (found.sequential) return;
        edges.delete(found);
        edges.add({ ...found, sequential: true, ...extra });
        return;
    }
    edges.add({ source, target, sequential: true, synthetic: true, ...extra });
}

function isStepCardType(type: unknown): boolean {
    return type === "transform" || type === "group";
}

function padOrderIndex(index: number): string {
    return String(index).padStart(4, "0");
}

function metaNameCounts(pipeline: PipelineNode[]): Map<string, number> {
    const counts = new Map<string, number>();
    pipeline.forEach((pipe) => {
        if (pipe.type !== "meta") return;
        counts.set(pipe.name, (counts.get(pipe.name) ?? 0) + 1);
    });
    return counts;
}

/**
 * Graph node id for a meta step. Duplicate display names (e.g. two
 * Inference_DetectionModel) get a stable `__orderKey` suffix so they do not
 * overwrite each other in the node map / layout.
 */
function metaGraphId(
    name: string,
    orderKey: string,
    counts: Map<string, number>,
): string {
    return (counts.get(name) ?? 0) > 1 ? `${name}__${orderKey}` : name;
}

function parseMetaGraphId(groupId: string): { name: string; orderKey: string | null } {
    const sep = groupId.lastIndexOf("__");
    if (sep <= 0) return { name: groupId, orderKey: null };
    const maybeKey = groupId.slice(sep + 2);
    if (/^\d{4}(?:\.\d{4})*$/.test(maybeKey)) {
        return { name: groupId.slice(0, sep), orderKey: maybeKey };
    }
    return { name: groupId, orderKey: null };
}

/**
 * Dashed next-step hops between consecutive transform/group cards in a pipeline list.
 * Skips expanded metas (their children get their own sequential chain).
 */
function addSequentialStepEdges(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
    pipeline: PipelineNode[],
    expandedGroups: Set<string>,
    parentOrderKey: string | undefined,
    extra: Partial<Cytoscape.EdgeDataDefinition> = {},
): void {
    const counts = metaNameCounts(pipeline);
    const steps: string[] = [];
    pipeline.forEach((pipe, index) => {
        const orderKey = parentOrderKey
            ? `${parentOrderKey}.${padOrderIndex(index)}`
            : padOrderIndex(index);
        const stepId =
            pipe.type === "meta" ? metaGraphId(pipe.name, orderKey, counts) : pipe.name;
        if (pipe.type === "meta" && expandedGroups.has(stepId)) return;
        const type = nodes.get(stepId)?.type;
        if (!isStepCardType(type)) return;
        steps.push(stepId);
    });

    for (let index = 0; index < steps.length - 1; index += 1) {
        ensureSequentialEdge(edges, steps[index], steps[index + 1], extra);
    }
}

function ensureTable(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    data: GraphData,
    tableName: string,
    metaGroup?: string,
    pipelineIndex?: number,
    pipelineOrderKey?: string,
    tableOrderSource: TableOrderSource = "consumer",
) {
    const properties = data.catalog[tableName];
    if (!properties) return;

    const tableData = nodes.get(tableName);
    const orderPatch =
        shouldReplaceTableOrder(tableData, pipelineOrderKey, tableOrderSource)
            ? {
                  pipelineIndex,
                  pipelineOrderKey,
                  tableOrderSource,
              }
            : {};

    nodes.set(tableName, {
        ...tableData,
        ...properties,
        type: "table",
        name: tableName,
        ...orderPatch,
        ...(metaGroup ? { metaGroup } : {}),
    });
}

function addTransformNode(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
    data: GraphData,
    pipe: TransformNode,
    metaGroup?: string,
    pipelineIndex?: number,
    pipelineOrderKey?: string,
) {
    const nodeName = pipe.name;
    nodes.set(nodeName, {
        ...pipe,
        type: "transform",
        name: nodeName,
        transform_primary_keys:
            pipe.transform_primary_keys ??
            pipe.tpk ??
            pipe.indexes ??
            pipe.primary_keys ??
            [],
        ...(metaGroup ? { metaGroup } : {}),
        ...(pipelineIndex != null ? { pipelineIndex } : {}),
        ...(pipelineOrderKey ? { pipelineOrderKey } : {}),
    });

    (pipe.inputs || []).forEach((input: string, inputIndex: number) => {
        ensureTable(
            nodes,
            data,
            input,
            metaGroup,
            pipelineIndex,
            tableOrderKey(pipelineOrderKey, "in", inputIndex),
            "consumer",
        );
        edges.add({ source: input, target: nodeName });
    });
    (pipe.outputs || []).forEach((output: string, outputIndex: number) => {
        ensureTable(
            nodes,
            data,
            output,
            metaGroup,
            pipelineIndex,
            tableOrderKey(pipelineOrderKey, "out", outputIndex),
            "producer",
        );
        edges.add({ source: nodeName, target: output });
    });
}

function addCollapsedMeta(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
    data: GraphData,
    pipe: MetaNode,
    metaId: string,
    pipelineIndex?: number,
    pipelineOrderKey?: string,
) {
    const childCount = pipe.graph?.pipeline?.length ?? 0;
    nodes.set(metaId, {
        id: metaId,
        type: "group",
        name: pipe.name,
        transform_type: pipe.transform_type || pipe.name,
        labels: pipe.labels,
        collapsed: true,
        child_count: childCount,
        inputs: pipe.inputs || [],
        outputs: pipe.outputs || [],
        transform_primary_keys:
            pipe.transform_primary_keys ??
            pipe.tpk ??
            [],
        ...(pipelineIndex != null ? { pipelineIndex } : {}),
        ...(pipelineOrderKey ? { pipelineOrderKey } : {}),
    });

    (pipe.inputs || []).forEach((input: string, inputIndex: number) => {
        ensureTable(
            nodes,
            data,
            input,
            undefined,
            pipelineIndex,
            tableOrderKey(pipelineOrderKey, "in", inputIndex),
            "consumer",
        );
        edges.add({ source: input, target: metaId });
    });
    (pipe.outputs || []).forEach((output: string, outputIndex: number) => {
        ensureTable(
            nodes,
            data,
            output,
            undefined,
            pipelineIndex,
            tableOrderKey(pipelineOrderKey, "out", outputIndex),
            "producer",
        );
        edges.add({ source: metaId, target: output });
    });
}

function processMetaGraph(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
    graph: GraphData,
    expandedGroups: Set<string>,
    metaGroup: string,
    parentOrderKey: string,
) {
    const counts = metaNameCounts(graph.pipeline);
    graph.pipeline.forEach((child, index) => {
        const orderKey = `${parentOrderKey}.${padOrderIndex(index)}`;
        if (child.type === "meta") {
            const childId = metaGraphId(child.name, orderKey, counts);
            if (expandedGroups.has(childId)) {
                processMetaGraph(nodes, edges, child.graph, expandedGroups, childId, orderKey);
                child.graph.pipeline.forEach((nested, nestedIndex) => {
                    if (nested.type !== "meta") {
                        const nestedKey = `${orderKey}.${padOrderIndex(nestedIndex)}`;
                        addTransformNode(
                            nodes,
                            edges,
                            child.graph,
                            nested,
                            childId,
                            nestedIndex,
                            nestedKey,
                        );
                    }
                });
            } else {
                addCollapsedMeta(nodes, edges, child.graph, child, childId, index, orderKey);
            }
            return;
        }
        addTransformNode(nodes, edges, graph, child, metaGroup, index, orderKey);
    });
}

function processData(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
    data: GraphData,
    expandedGroups: Set<string>,
) {
    const counts = metaNameCounts(data.pipeline);
    data.pipeline.forEach((pipe, index) => {
        const orderKey = padOrderIndex(index);
        if (pipe.type !== "meta") {
            addTransformNode(nodes, edges, data, pipe, undefined, index, orderKey);
            return;
        }

        const metaId = metaGraphId(pipe.name, orderKey, counts);
        if (expandedGroups.has(metaId)) {
            processMetaGraph(nodes, edges, pipe.graph, expandedGroups, metaId, orderKey);
        } else {
            addCollapsedMeta(nodes, edges, data, pipe, metaId, index, orderKey);
        }
    });
}

function pruneDisconnectedTables(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
) {
    const connected = new Set<string>();
    edges.forEach((edge) => {
        if (edge.source) connected.add(edge.source as string);
        if (edge.target) connected.add(edge.target as string);
    });

    Array.from(nodes.entries()).forEach(([nodeId, nodeData]) => {
        if (nodeData.type === "table" && !connected.has(nodeId)) {
            nodes.delete(nodeId);
        }
    });
}

/**
 * Mark expanded meta subgraph members via metaGroup; the blue frame is a flat background node.
 * Declared meta inputs/outputs stay outside the frame (boundary tables).
 */
function findMetaNode(
    pipeline: PipelineNode[],
    groupId: string,
    parentOrderKey = "",
): MetaNode | undefined {
    const { name, orderKey } = parseMetaGraphId(groupId);
    for (let index = 0; index < pipeline.length; index += 1) {
        const pipe = pipeline[index];
        if (pipe.type !== "meta") continue;
        const key = parentOrderKey
            ? `${parentOrderKey}.${padOrderIndex(index)}`
            : padOrderIndex(index);
        if (orderKey) {
            if (pipe.name === name && key === orderKey) return pipe;
        } else if (pipe.name === name) {
            return pipe;
        }
        const nested = findMetaNode(pipe.graph.pipeline, groupId, key);
        if (nested) return nested;
    }
    return undefined;
}

function assignCompoundParents(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
    expandedGroups: Set<string>,
    data: GraphData,
) {
    Array.from(expandedGroups).forEach((group) => {
        const memberIds = new Set<string>();
        nodes.forEach((nodeData, id) => {
            if (nodeData.metaGroup === group) memberIds.add(id);
        });
        if (!memberIds.size) return;

        const metaPipe = findMetaNode(data.pipeline, group);
        const metaInputs = new Set(metaPipe?.inputs ?? []);
        const metaOutputs = new Set(metaPipe?.outputs ?? []);

        const boundaryNodes = new Set<string>();
        edges.forEach((edge) => {
            const source = edge.source as string;
            const target = edge.target as string;
            const sourceIn = memberIds.has(source);
            const targetIn = memberIds.has(target);
            if (sourceIn !== targetIn) {
                if (sourceIn) boundaryNodes.add(source);
                if (targetIn) boundaryNodes.add(target);
            }
        });

        // Declared meta inputs must stay outside the blue frame (same as outputs).
        // Edge-crossing alone misses them: input→first_transform is both-in-member.
        metaInputs.forEach((inputTable) => {
            if (!memberIds.has(inputTable)) return;
            boundaryNodes.add(inputTable);
        });

        metaOutputs.forEach((outputTable) => {
            if (!memberIds.has(outputTable)) return;
            if (boundaryNodes.has(outputTable)) return;
            const producedByMember = Array.from(edges).some(
                (edge) => edge.target === outputTable && memberIds.has(edge.source as string),
            );
            const consumedByMember = Array.from(edges).some(
                (edge) => edge.source === outputTable && memberIds.has(edge.target as string),
            );
            if (producedByMember && consumedByMember) return;
            boundaryNodes.add(outputTable);
        });

        let nested = 0;
        memberIds.forEach((id) => {
            const nodeData = nodes.get(id);
            if (!nodeData) return;
            if (nodeData.type === "table" && boundaryNodes.has(id)) {
                const { metaGroup, ...rest } = nodeData;
                nodes.set(id, rest);
                return;
            }
            // Keep subgraph members as top-level nodes; the blue frame is visual-only (no compound parent).
            nested += 1;
        });

        if (nested > 0) {
            const prev = nodes.get(group);
            const metaPipe = findMetaNode(data.pipeline, group);
            const displayName = metaPipe?.name ?? parseMetaGraphId(group).name;
            nodes.set(group, {
                ...prev,
                id: group,
                type: "group-expanded",
                name: displayName,
                child_count: nested,
                frameLabel: `${displayName} · ${nested} step${nested === 1 ? "" : "s"}`,
                // Expanded metas skip addCollapsedMeta, so labels/TPK must be
                // copied from the pipeline meta or column packing loses them.
                labels: prev?.labels ?? metaPipe?.labels,
                transform_type:
                    prev?.transform_type ?? metaPipe?.transform_type ?? metaPipe?.name ?? displayName,
                transform_primary_keys:
                    prev?.transform_primary_keys ??
                    metaPipe?.transform_primary_keys ??
                    metaPipe?.tpk ??
                    [],
                inputs: prev?.inputs ?? metaPipe?.inputs ?? [],
                outputs: prev?.outputs ?? metaPipe?.outputs ?? [],
            });
        }
    });
}

function addSequentialMetaEdges(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
    data: GraphData,
    expandedGroups: Set<string>,
) {
    // Top-level collapsed graph: consecutive transform/group cards.
    addSequentialStepEdges(nodes, edges, data.pipeline, expandedGroups, undefined);

    expandedGroups.forEach((groupId) => {
        const meta = findMetaNode(data.pipeline, groupId);
        if (!meta) return;
        const parentKey =
            parseMetaGraphId(groupId).orderKey ??
            (nodes.get(groupId)?.pipelineOrderKey as string | undefined);
        addSequentialStepEdges(nodes, edges, meta.graph.pipeline, expandedGroups, parentKey, {
            internalMeta: groupId,
        });
    });
}

function markInternalMetaEdges(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
): Set<Cytoscape.EdgeDataDefinition> {
    const marked = new Set<Cytoscape.EdgeDataDefinition>();
    edges.forEach((edge) => {
        const sourceMeta = nodes.get(edge.source as string)?.metaGroup;
        const targetMeta = nodes.get(edge.target as string)?.metaGroup;
        if (sourceMeta && sourceMeta === targetMeta) {
            marked.add({ ...edge, internalMeta: sourceMeta });
            return;
        }
        marked.add(edge);
    });
    return marked;
}

function reprocessData(data: GraphData, expandedGroups: Set<string> = new Set()) {
    const nodes = new Map<string, Cytoscape.NodeDataDefinition>();
    const edges = new Set<Cytoscape.EdgeDataDefinition>();
    processData(nodes, edges, data, expandedGroups);
    pruneDisconnectedTables(nodes, edges);
    assignCompoundParents(nodes, edges, expandedGroups, data);
    addSequentialMetaEdges(nodes, edges, data, expandedGroups);
    const markedEdges = markInternalMetaEdges(nodes, edges);
    return { nodes, edges: markedEdges };
}

export { reprocessData };
