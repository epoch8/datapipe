import Cytoscape from "cytoscape";
import { GraphData } from "../../types";
import { applyViewport, captureViewport, runGraphLayout, type GraphViewport } from "./compound";
import { reprocessData } from "./process";
import { layoutCompactSubgraphAtAnchor } from "./subgraphLayout";

export type CyElement = Cytoscape.ElementDefinition;

export type SyncMode = "fit" | "preserve" | "local";

export type SyncOptions = {
    mode: SyncMode;
    rankDir?: "TB" | "LR";
    viewport?: GraphViewport | null;
    anchorGroup?: string | null;
};

function buildElements(data: GraphData, expanded: Set<string>): CyElement[] {
    const { nodes, edges } = reprocessData(data, expanded);
    const elements: CyElement[] = Array.from(nodes.entries()).map(([nodeId, options]) => ({
        selectable: true,
        data: {
            id: nodeId,
            label: options.name || nodeId,
            ...options,
        },
    }));
    edges.forEach((edge) => {
        elements.push({
            grabbable: false,
            data: edge,
        });
    });
    return elements;
}

function edgeKey(source: string, target: string): string {
    return `${source}->${target}`;
}

function snapshotPositions(cy: Cytoscape.Core): Map<string, Cytoscape.Position> {
    const positions = new Map<string, Cytoscape.Position>();
    cy.nodes().forEach((node) => {
        positions.set(node.id(), { ...node.position() });
    });
    return positions;
}

function restorePositions(cy: Cytoscape.Core, positions: Map<string, Cytoscape.Position>) {
    positions.forEach((pos, id) => {
        const node = cy.getElementById(id);
        if (node.nonempty()) {
            node.position(pos);
        }
    });
}

function centroid(positions: Cytoscape.Position[]): Cytoscape.Position {
    if (!positions.length) return { x: 0, y: 0 };
    const sum = positions.reduce(
        (acc, p) => ({ x: acc.x + p.x, y: acc.y + p.y }),
        { x: 0, y: 0 },
    );
    return { x: sum.x / positions.length, y: sum.y / positions.length };
}

function applyElementDiff(cy: Cytoscape.Core, target: CyElement[]) {
    const targetNodes = target.filter((el) => el.data.id);
    const targetEdges = target.filter((el) => el.data.source && el.data.target);
    const targetNodeIds = new Set(targetNodes.map((el) => el.data.id as string));
    const targetEdgeKeys = new Set(
        targetEdges.map((el) => edgeKey(el.data.source as string, el.data.target as string)),
    );

    cy.batch(() => {
        cy.nodes().forEach((node) => {
            if (!targetNodeIds.has(node.id())) {
                node.remove();
            }
        });
        cy.edges().forEach((edge) => {
            const key = edgeKey(edge.source().id(), edge.target().id());
            if (!targetEdgeKeys.has(key)) {
                edge.remove();
            }
        });

        targetNodes.forEach((el) => {
            const id = el.data.id as string;
            const existing = cy.getElementById(id);
            if (existing.nonempty()) {
                existing.data(el.data);
            } else {
                cy.add(el);
            }
        });

        targetEdges.forEach((el) => {
            const key = edgeKey(el.data.source as string, el.data.target as string);
            const found = cy.edges().filter(
                (edge) => edgeKey(edge.source().id(), edge.target().id()) === key,
            );
            if (found.empty()) {
                cy.add(el);
            }
        });
    });
}

export function syncCyGraph(
    cy: Cytoscape.Core,
    data: GraphData,
    expanded: Set<string>,
    options: SyncOptions,
) {
    const rankDir = options.rankDir ?? "TB";
    const target = buildElements(data, expanded);
    const targetNodeIds = new Set(
        target.filter((el) => el.data.id).map((el) => el.data.id as string),
    );
    const currentNodeIds = new Set(cy.nodes().map((node) => node.id()));
    const sameStructure =
        targetNodeIds.size === currentNodeIds.size &&
        Array.from(targetNodeIds).every((id) => currentNodeIds.has(id));

    const savedPositions = snapshotPositions(cy);
    const viewport = options.viewport ?? captureViewport(cy);

    if (options.mode === "preserve" && sameStructure) {
        applyElementDiff(cy, target);
        restorePositions(cy, savedPositions);
        applyViewport(cy, viewport);
        return;
    }

    if (options.mode === "fit" || cy.nodes().empty()) {
        applyElementDiff(cy, target);
        runGraphLayout(cy, rankDir, { mode: "fit" });
        return;
    }

    const anchorGroup = options.anchorGroup;
    if (options.mode === "local" && anchorGroup) {
        applyViewport(cy, viewport);

        const expanding = expanded.has(anchorGroup);
        let anchorPos = expanding ? savedPositions.get(anchorGroup) : undefined;
        if (!anchorPos && !expanding) {
            const childPositions: Cytoscape.Position[] = [];
            cy.nodes()
                .filter((node) => node.data("metaGroup") === anchorGroup)
                .forEach((node) => {
                    childPositions.push(node.position());
                });
            anchorPos = centroid(childPositions);
        }

        const stablePositions = new Map(savedPositions);
        if (expanding) {
            stablePositions.delete(anchorGroup);
        }

        applyElementDiff(cy, target);

        restorePositions(cy, stablePositions);

        if (anchorPos) {
            const anchor = anchorPos;
            if (expanding) {
                const newNodes = cy.nodes().filter((node) => node.data("metaGroup") === anchorGroup);
                layoutCompactSubgraphAtAnchor(newNodes, anchor);
            } else {
                const groupNode = cy.getElementById(anchorGroup);
                if (groupNode.nonempty()) {
                    groupNode.position(anchor);
                }
            }
        }

        return;
    }

    if (options.mode === "preserve") {
        applyElementDiff(cy, target);
        restorePositions(cy, savedPositions);
        applyViewport(cy, viewport);
        return;
    }

    applyElementDiff(cy, target);
    runGraphLayout(cy, rankDir, { mode: "preserve", viewport });
}

export { buildElements };
