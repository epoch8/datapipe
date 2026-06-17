import { useEffect, useState } from "react";
import type { GraphData } from "../types";

const GRAPH_URL =
    (process.env["REACT_APP_GET_GRAPH_URL"] as string) || "/api/v1alpha2/graph";

export function usePipelineGraph() {
    const [graph, setGraph] = useState<GraphData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        let cancelled = false;
        setLoading(true);
        fetch(GRAPH_URL)
            .then((r) => {
                if (!r.ok) throw new Error(`Graph ${r.status}`);
                return r.json();
            })
            .then((data: GraphData) => {
                if (!cancelled) {
                    setGraph(data);
                    setError(null);
                }
            })
            .catch((e) => {
                if (!cancelled) setError(String(e));
            })
            .finally(() => {
                if (!cancelled) setLoading(false);
            });
        return () => {
            cancelled = true;
        };
    }, []);

    return { graph, loading, error };
}

export function findTableInGraph(graph: GraphData, name: string) {
    const entry = graph.catalog[name];
    if (!entry) return null;
    return {
        name: entry.id ?? name,
        indexes: entry.indexes ?? [],
        size: entry.size ?? 0,
        store_class: entry.store_class ?? "",
    };
}

export function findTransformInGraph(graph: GraphData, name: string) {
    for (const node of graph.pipeline) {
        if (node.type === "transform" && node.name === name) {
            return node;
        }
        if (node.type === "meta" && node.graph) {
            const inner = node.graph.pipeline.find(
                (s) => s.type === "transform" && s.name === name,
            );
            if (inner && inner.type === "transform") return inner;
        }
    }
    return null;
}

export function findMetaStepInGraph(graph: GraphData, name: string) {
    return graph.pipeline.find((n) => n.type === "meta" && n.name === name) ?? null;
}
