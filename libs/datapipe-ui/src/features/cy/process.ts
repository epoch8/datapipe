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
    _source: TableOrderSource,
): boolean {
    if (!nextOrderKey) return false;

    const currentOrderKey = existing?.pipelineOrderKey as string | undefined;
    if (!currentOrderKey) return true;

    // First use wins: keep the earliest pipelineOrderKey so a table that is an
    // early input (then later rewritten as an output) stays next to its first
    // consumer — e.g. image__ground_truth before get_images_without_ground_truth,
    // not next to a late parse_annotations producer.
    return nextOrderKey.localeCompare(currentOrderKey) < 0;
}

/** Always add a dedicated chronology hop — never restyle an existing data edge. */
function ensureSequentialEdge(
    edges: Set<Cytoscape.EdgeDataDefinition>,
    source: string,
    target: string,
    extra: Partial<Cytoscape.EdgeDataDefinition> = {},
): void {
    for (const edge of Array.from(edges)) {
        if (edge.source === source && edge.target === target && edge.sequential) {
            return;
        }
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

/** Resolve chronology for a meta id that has no `__orderKey` suffix (unique names). */
function findMetaOrderKey(
    pipeline: PipelineNode[],
    groupId: string,
    parentOrderKey = "",
): string | null {
    const { name, orderKey: parsed } = parseMetaGraphId(groupId);
    for (let index = 0; index < pipeline.length; index += 1) {
        const pipe = pipeline[index];
        if (pipe.type !== "meta") continue;
        const key = parentOrderKey
            ? `${parentOrderKey}.${padOrderIndex(index)}`
            : padOrderIndex(index);
        if (parsed) {
            if (pipe.name === name && key === parsed) return key;
        } else if (pipe.name === name) {
            return key;
        }
        const nested = findMetaOrderKey(pipe.graph.pipeline, groupId, key);
        if (nested) return nested;
    }
    return null;
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
                nodes.set(childId, {
                    id: childId,
                    type: "group-expanded",
                    name: child.name,
                    transform_type: child.transform_type || child.name,
                    labels: child.labels,
                    child_count: child.graph?.pipeline?.length ?? 0,
                    inputs: child.inputs || [],
                    outputs: child.outputs || [],
                    transform_primary_keys:
                        child.transform_primary_keys ?? child.tpk ?? [],
                    metaGroup,
                    pipelineIndex: index,
                    pipelineOrderKey: orderKey,
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
            // Seed the blue-frame node with chronology. Expanded path skips
            // addCollapsedMeta, and without pipelineOrderKey the frame ranks as
            // "unkeyed leftover" at the end of the snake (e.g. LabelStudioUploadTasks).
            nodes.set(metaId, {
                id: metaId,
                type: "group-expanded",
                name: pipe.name,
                transform_type: pipe.transform_type || pipe.name,
                labels: pipe.labels,
                child_count: pipe.graph?.pipeline?.length ?? 0,
                inputs: pipe.inputs || [],
                outputs: pipe.outputs || [],
                transform_primary_keys:
                    pipe.transform_primary_keys ?? pipe.tpk ?? [],
                pipelineIndex: index,
                pipelineOrderKey: orderKey,
            });
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
            const orderKey =
                (prev?.pipelineOrderKey as string | undefined) ||
                parseMetaGraphId(group).orderKey ||
                findMetaOrderKey(data.pipeline, group) ||
                undefined;
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
                ...(orderKey ? { pipelineOrderKey: orderKey } : {}),
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
