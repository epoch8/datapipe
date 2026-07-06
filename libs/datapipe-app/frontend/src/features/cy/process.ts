import { GraphData, MetaNode, TransformNode } from "../../types";
import Cytoscape from "cytoscape";

function hasTransformPath(
    edges: Set<Cytoscape.EdgeDataDefinition>,
    source: string,
    target: string,
): boolean {
    for (const edge of Array.from(edges)) {
        if (edge.source === source && edge.target === target) return true;
    }
    for (const edge of Array.from(edges)) {
        if (edge.source !== source) continue;
        const mid = edge.target as string;
        for (const hop of Array.from(edges)) {
            if (hop.source === mid && hop.target === target) return true;
        }
    }
    return false;
}

function ensureTable(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    data: GraphData,
    tableName: string,
    metaGroup?: string,
) {
    const properties = data.catalog[tableName];
    if (!properties) return;

    const tableData = nodes.get(tableName);
    nodes.set(tableName, {
        ...tableData,
        ...properties,
        type: "table",
        name: tableName,
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

    (pipe.inputs || []).forEach((input: string) => {
        ensureTable(nodes, data, input, metaGroup);
        edges.add({ source: input, target: nodeName });
    });
    (pipe.outputs || []).forEach((output: string) => {
        ensureTable(nodes, data, output, metaGroup);
        edges.add({ source: nodeName, target: output });
    });
}

function addCollapsedMeta(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
    data: GraphData,
    pipe: MetaNode,
    pipelineIndex?: number,
    pipelineOrderKey?: string,
) {
    const childCount = pipe.graph?.pipeline?.length ?? 0;
    nodes.set(pipe.name, {
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

    (pipe.inputs || []).forEach((input: string) => {
        ensureTable(nodes, data, input);
        edges.add({ source: input, target: pipe.name });
    });
    (pipe.outputs || []).forEach((output: string) => {
        ensureTable(nodes, data, output);
        edges.add({ source: pipe.name, target: output });
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
    graph.pipeline.forEach((child, index) => {
        const orderKey = `${parentOrderKey}.${String(index).padStart(4, "0")}`;
        if (child.type === "meta") {
            if (expandedGroups.has(child.name)) {
                processMetaGraph(nodes, edges, child.graph, expandedGroups, child.name, orderKey);
                child.graph.pipeline.forEach((nested, nestedIndex) => {
                    if (nested.type !== "meta") {
                        const nestedKey = `${orderKey}.${String(nestedIndex).padStart(4, "0")}`;
                        addTransformNode(nodes, edges, child.graph, nested, child.name, nestedIndex, nestedKey);
                    }
                });
            } else {
                addCollapsedMeta(nodes, edges, child.graph, child, index, orderKey);
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
    data.pipeline.forEach((pipe, index) => {
        const orderKey = String(index).padStart(4, "0");
        if (pipe.type !== "meta") {
            addTransformNode(nodes, edges, data, pipe, undefined, index, orderKey);
            return;
        }

        if (expandedGroups.has(pipe.name)) {
            processMetaGraph(nodes, edges, pipe.graph, expandedGroups, pipe.name, orderKey);
        } else {
            addCollapsedMeta(nodes, edges, data, pipe, index, orderKey);
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
 */
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

        const metaPipe = data.pipeline.find(
            (pipe) => pipe.type === "meta" && pipe.name === group,
        );
        const metaOutputs = new Set(metaPipe?.type === "meta" ? metaPipe.outputs ?? [] : []);

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
            nodes.set(group, {
                id: group,
                type: "group-expanded",
                name: group,
                child_count: nested,
                frameLabel: `${group} · ${nested} step${nested === 1 ? "" : "s"}`,
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
    expandedGroups.forEach((groupId) => {
        const meta = data.pipeline.find(
            (pipe): pipe is MetaNode => pipe.type === "meta" && pipe.name === groupId,
        );
        if (!meta) return;

        const transforms = meta.graph.pipeline
            .filter((step) => step.type !== "meta")
            .map((step) => step.name)
            .filter((name) => nodes.get(name)?.type === "transform");

        for (let index = 0; index < transforms.length - 1; index += 1) {
            const source = transforms[index];
            const target = transforms[index + 1];
            if (hasTransformPath(edges, source, target)) continue;
            edges.add({ source, target, internalMeta: groupId, sequential: true });
        }
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
