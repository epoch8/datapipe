import { useCallback, useEffect, useState } from "react";
import type { GraphData } from "../types";
import { fetchGraph } from "../api/graph";

export function usePipelineGraph(stage?: string | null) {
    const [graph, setGraph] = useState<GraphData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<unknown>(null);
    const [refreshToken, setRefreshToken] = useState(0);

    const refresh = useCallback(() => {
        setRefreshToken((token) => token + 1);
    }, []);

    useEffect(() => {
        let cancelled = false;
        setLoading(true);
        fetchGraph(stage)
            .then((data) => {
                if (!cancelled) {
                    setGraph(data as GraphData);
                    setError(null);
                }
            })
            .catch((e) => {
                if (!cancelled) setError(e);
            })
            .finally(() => {
                if (!cancelled) setLoading(false);
            });
        return () => {
            cancelled = true;
        };
    }, [stage, refreshToken]);

    return { graph, loading, error, refresh };
}

export function findTableInGraph(graph: GraphData, name: string) {
    const entry = graph.catalog[name];
    if (!entry) return null;
    return {
        name: entry.id ?? name,
        indexes: entry.indexes ?? [],
        size: entry.size ?? null,
        store_class: entry.store_class ?? "",
        schema: entry.schema ?? [],
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
