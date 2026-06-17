import { omit } from "lodash";
import { GraphData, MetaNode, TransformNode } from "../../types";
import Cytoscape from "cytoscape";

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
) {
    const nodeName = pipe.name;
    nodes.set(nodeName, {
        ...omit(pipe, ["inputs", "outputs"]),
        type: "transform",
        ...(metaGroup ? { metaGroup } : {}),
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
) {
    const childCount = pipe.graph?.pipeline?.length ?? 0;
    nodes.set(pipe.name, {
        type: "group",
        name: pipe.name,
        transform_type: pipe.transform_type || pipe.name,
        labels: pipe.labels,
        collapsed: true,
        child_count: childCount,
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
) {
    for (const child of graph.pipeline) {
        if (child.type === "meta") {
            if (expandedGroups.has(child.name)) {
                processMetaGraph(nodes, edges, child.graph, expandedGroups, child.name);
                for (const nested of child.graph.pipeline) {
                    if (nested.type !== "meta") {
                        addTransformNode(nodes, edges, child.graph, nested, child.name);
                    }
                }
            } else {
                addCollapsedMeta(nodes, edges, child.graph, child);
            }
            continue;
        }
        addTransformNode(nodes, edges, graph, child, metaGroup);
    }
}

function processData(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
    data: GraphData,
    expandedGroups: Set<string>,
) {
    for (const pipe of data.pipeline) {
        if (pipe.type !== "meta") {
            addTransformNode(nodes, edges, data, pipe);
            continue;
        }

        if (expandedGroups.has(pipe.name)) {
            processMetaGraph(nodes, edges, pipe.graph, expandedGroups, pipe.name);
        } else {
            addCollapsedMeta(nodes, edges, data, pipe);
        }
    }
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

function reprocessData(data: GraphData, expandedGroups: Set<string> = new Set()) {
    const nodes = new Map<string, Cytoscape.NodeDataDefinition>();
    const edges = new Set<Cytoscape.EdgeDataDefinition>();
    processData(nodes, edges, data, expandedGroups);
    pruneDisconnectedTables(nodes, edges);
    return { nodes, edges };
}

export { reprocessData };
