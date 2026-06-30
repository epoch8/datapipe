import Cytoscape from "cytoscape";
import { GraphData } from "../../types";
import { reprocessData } from "./process";

export type CyElement = Cytoscape.ElementDefinition;

export type SyncMode = "fit" | "preserve";

export type SyncOptions = {
    mode: SyncMode;
    rankDir?: "TB" | "LR";
    anchorGroup?: string | null;
    expanding?: boolean;
};

function buildElements(data: GraphData, expanded: Set<string>): CyElement[] {
    const { nodes, edges } = reprocessData(data, expanded);
    const elements: CyElement[] = Array.from(nodes.entries())
        // Compound parents must be added before their children.
        .sort(([, a], [, b]) => {
            const aParent = a.type === "group-expanded" ? 0 : 1;
            const bParent = b.type === "group-expanded" ? 0 : 1;
            return aParent - bParent;
        })
        .map(([nodeId, options]) => ({
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
                const node = existing as unknown as Cytoscape.NodeSingular;
                const nextParent = (el.data.parent as string) ?? null;
                const currentParent = node.isChild() ? node.parent().first().id() : null;
                node.data(el.data);
                if (nextParent !== currentParent) {
                    node.move({ parent: nextParent });
                }
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

function runDagre(cy: Cytoscape.Core, rankDir: "TB" | "LR", animate: boolean, fit: boolean) {
    const layout = cy.layout({
        name: "dagre",
        rankDir,
        ranker: "network-simplex",
        nodeDimensionsIncludeLabels: true,
        nodeSep: 44,
        rankSep: 70,
        edgeSep: 16,
        fit,
        padding: 60,
        animate,
        animationDuration: 350,
        animationEasing: "ease-out-cubic",
    } as unknown as Cytoscape.LayoutOptions);
    layout.run();
}

/**
 * Re-derive the graph for the current expand/collapse state and lay it out compactly with
 * dagre. Collapsed metas stay as large clickable rectangles; expanding one reflows the graph
 * with a short animation and keeps the toggled group in view, so the overview stays compact
 * and readable instead of reserving the full sub-step area for every collapsed step.
 */
export function syncCyGraph(
    cy: Cytoscape.Core,
    data: GraphData,
    expanded: Set<string>,
    options: SyncOptions,
) {
    const rankDir = options.rankDir ?? "TB";
    const target = buildElements(data, expanded);
    applyElementDiff(cy, target);

    if (options.mode === "fit" || cy.nodes().empty()) {
        runDagre(cy, rankDir, false, true);
        const fitZoom = cy.zoom();
        cy.minZoom(Math.min(0.05, fitZoom * 0.3));
        cy.maxZoom(Math.max(2.5, fitZoom * 6));

        // A whole pipeline of large nodes can't be read at fit-to-screen. If fitting drops
        // the zoom below a legible level, start at a readable zoom anchored to the top of the
        // pipeline instead; the user pans to explore the rest.
        const READABLE_ZOOM = 0.4;
        if (fitZoom < READABLE_ZOOM) {
            const bb = cy.elements().boundingBox();
            cy.zoom(READABLE_ZOOM);
            cy.pan({
                x: cy.width() / 2 - READABLE_ZOOM * (bb.x1 + bb.w / 2),
                y: 90 - READABLE_ZOOM * bb.y1,
            });
        }
        return;
    }

    runDagre(cy, rankDir, true, false);

    // Keep the toggled group comfortably in view after the reflow.
    const anchor = options.anchorGroup ? cy.getElementById(options.anchorGroup) : null;
    if (anchor && anchor.nonempty()) {
        const focus = options.expanding ? anchor.closedNeighborhood() : anchor;
        cy.animate(
            { center: { eles: focus } },
            { duration: 350, easing: "ease-out-cubic" },
        );
    }
}

export { buildElements };
