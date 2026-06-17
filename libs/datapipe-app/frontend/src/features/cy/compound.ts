import Cytoscape from "cytoscape";
import dagre from "cytoscape-dagre";

export type GraphViewport = {
    zoom: number;
    pan: { x: number; y: number };
};

export type LayoutMode = "fit" | "preserve" | "local";

export type LayoutOptions = {
    mode?: LayoutMode;
    viewport?: GraphViewport | null;
    animationDuration?: number;
};

export function captureViewport(cy: Cytoscape.Core): GraphViewport {
    return {
        zoom: cy.zoom(),
        pan: { ...cy.pan() },
    };
}

export function applyViewport(cy: Cytoscape.Core, viewport: GraphViewport) {
    cy.zoom(viewport.zoom);
    cy.pan(viewport.pan);
}

export function runGraphLayout(
    cy: Cytoscape.Core,
    rankDir: "TB" | "LR",
    options: LayoutOptions = {},
) {
    const mode = options.mode ?? "fit";

    cy.nodes().style("display", "element");

    const layout = cy.layout({
        name: "dagre",
        rankDir,
        ranker: "network-simplex",
        nodeDimensionsIncludeLabels: true,
        spacingFactor: 1.1,
        nodeSep: 24,
        rankSep: 48,
        edgeSep: 12,
        animate: false,
    } as dagre.DagreLayoutOptions);

    layout.on("layoutstop", () => {
        if (mode === "fit") {
            cy.fit(cy.elements(), 48);
            const fitZoom = cy.zoom();
            cy.minZoom(Math.min(0.08, fitZoom * 0.35));
            cy.maxZoom(Math.max(2.5, fitZoom * 6));
        } else if (options.viewport) {
            applyViewport(cy, options.viewport);
        }
    });
    layout.run();
}
