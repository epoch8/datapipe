import Cytoscape from "cytoscape";
import { GraphData, TableColumn } from "../../types";
import { reprocessData } from "./process";

export function graphNodesByIdFromGraph(
    graph: GraphData,
    expandedGroups: Set<string> = new Set(),
): Map<string, Cytoscape.NodeDataDefinition> {
    const { nodes } = reprocessData(graph, expandedGroups);
    return nodes;
}

export function nodeDataFromTable(
    name: string,
    indexes: string[],
    storeClass: string,
    size?: number | null,
    schema: TableColumn[] = [],
): Cytoscape.NodeDataDefinition {
    return {
        id: name,
        name,
        type: "table",
        indexes,
        store_class: storeClass,
        size: size ?? null,
        schema,
    };
}

export function nodeDataFromTransform(
    step: {
        name: string;
        transform_type?: string;
        inputs: string[];
        outputs: string[];
        labels?: string[][];
        indexes?: string[];
        has_transform_meta?: boolean;
        total_idx_count?: number;
        changed_idx_count?: number;
    },
): Cytoscape.NodeDataDefinition {
    return {
        id: step.name,
        name: step.name,
        type: "transform",
        transform_type: step.transform_type,
        inputs: step.inputs,
        outputs: step.outputs,
        labels: step.labels,
        indexes: step.indexes,
        transform_primary_keys: step.indexes,
        has_transform_meta: step.has_transform_meta ?? false,
        total_idx_count: step.total_idx_count,
        changed_idx_count: step.changed_idx_count,
    };
}

export function nodeDataFromMeta(
    meta: {
        name: string;
        transform_type?: string;
        inputs?: string[];
        outputs?: string[];
        labels?: string[][];
        graph?: { pipeline: { name: string }[] };
    },
): Cytoscape.NodeDataDefinition {
    return {
        id: meta.name,
        name: meta.name,
        type: "group",
        transform_type: meta.transform_type || meta.name,
        child_count: meta.graph?.pipeline?.length ?? 0,
        inputs: meta.inputs ?? [],
        outputs: meta.outputs ?? [],
        labels: meta.labels,
    };
}
