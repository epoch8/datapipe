import { omit } from "lodash";
import { GraphData } from "../../types";
import Cytoscape from "cytoscape";

function setCatalog(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    data: GraphData,
    grouped?: string,
) {
    for (const [table, properties] of Object.entries(data.catalog)) {
        const tableData = nodes.get(table);
        properties.type = "table";
        nodes.set(table, {
            ...tableData,
            ...properties,
        });

        if (grouped) {
            nodes.set(table, {
                type: "group",
                ...tableData,
                parent: grouped,
            });
        }
    }
}

function processData(
    nodes: Map<string, Cytoscape.NodeDataDefinition>,
    edges: Set<Cytoscape.EdgeDataDefinition>,
    data: GraphData,
    grouped?: string,
) {
    if (grouped) {
        nodes.set(grouped, {
            selectable: false,
        });
    }

    setCatalog(nodes, data, grouped);

    for (const pipe of data.pipeline) {
        const nodeName = pipe.name;
        if (pipe.type !== "meta") {
            nodes.set(nodeName, {
                ...omit(pipe, ["inputs", "outputs"]),
            });

            if (grouped) {
                nodes.set(nodeName, {
                    ...nodes.get(nodeName),
                    parent: grouped,
                });
            }

            (pipe.inputs || []).forEach((input: string) => {
                edges.add({ source: input, target: nodeName });
            });

            (pipe.outputs || []).forEach((output: string) => {
                edges.add({ source: nodeName, target: output });
            });
        } else {
            processData(nodes, edges, pipe.graph, pipe.name);
        }
    }
}

function reprocessData(data: GraphData) {
    const nodes = new Map<string, Cytoscape.NodeDataDefinition>();
    const edges = new Set<Cytoscape.EdgeDataDefinition>();
    processData(nodes, edges, data);
    return {
        nodes,
        edges,
    };
}

export { reprocessData };
