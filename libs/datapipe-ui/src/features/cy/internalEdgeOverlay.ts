import Cytoscape from "cytoscape";
import { edgeColors } from "./graphColors";

const overlayInitStore = new WeakMap<Cytoscape.Core, true>();

/** Match cytoscape `arrow-scale: 1.08` triangle; userSpaceOnUse so stroke width doesn't inflate it. */
const ARROW_MARKER = {
    viewBox: "0 0 10 10",
    refX: "9",
    refY: "5",
    markerWidth: "7",
    markerHeight: "7",
    path: "M 0 0 L 10 5 L 0 10 z",
} as const;

const ARROW_MARKERS: Array<{ id: string; fill: string }> = [
    { id: "cy-internal-edge-arrow", fill: edgeColors.default },
    { id: "cy-internal-edge-arrow-related", fill: edgeColors.related },
    { id: "cy-internal-edge-arrow-focused", fill: edgeColors.active },
    { id: "cy-internal-edge-arrow-failed", fill: edgeColors.error },
];

function modelToRendered(cy: Cytoscape.Core, x: number, y: number): { x: number; y: number } {
    const pan = cy.pan();
    const zoom = cy.zoom();
    return { x: x * zoom + pan.x, y: y * zoom + pan.y };
}

function taxiPathFromPoints(points: number[]): string {
    if (points.length < 4) return "";
    const segments: string[] = [];
    for (let i = 0; i + 1 < points.length; i += 2) {
        const cmd = i === 0 ? "M" : "L";
        segments.push(`${cmd} ${points[i]} ${points[i + 1]}`);
    }
    return segments.join(" ");
}

function edgePathD(cy: Cytoscape.Core, edge: Cytoscape.EdgeSingular): string {
    const scratch = (edge as unknown as { _private?: { rscratch?: { allpts?: number[] } } })._private
        ?.rscratch;
    const allpts = scratch?.allpts;
    if (allpts && allpts.length >= 4) {
        const pairs: number[] = [];
        for (let i = 0; i < allpts.length; i += 2) {
            const p = modelToRendered(cy, allpts[i], allpts[i + 1]);
            pairs.push(p.x, p.y);
        }
        const path = taxiPathFromPoints(pairs);
        if (path) return path;
    }

    const source = edge.source().renderedPosition();
    const target = edge.target().renderedPosition();
    const midY = (source.y + target.y) / 2;
    return `M ${source.x} ${source.y} L ${source.x} ${midY} L ${target.x} ${midY} L ${target.x} ${target.y}`;
}

function edgeStroke(edge: Cytoscape.EdgeSingular): string {
    if (edge.hasClass("failed")) return edgeColors.error;
    if (edge.hasClass("related")) return edgeColors.related;
    if (edge.hasClass("focused")) return edgeColors.active;
    return edgeColors.default;
}

function edgeArrowMarker(edge: Cytoscape.EdgeSingular): string {
    if (edge.hasClass("failed")) return "url(#cy-internal-edge-arrow-failed)";
    if (edge.hasClass("related")) return "url(#cy-internal-edge-arrow-related)";
    if (edge.hasClass("focused")) return "url(#cy-internal-edge-arrow-focused)";
    return "url(#cy-internal-edge-arrow)";
}

function edgeOpacity(edge: Cytoscape.EdgeSingular): number {
    if (edge.hasClass("muted")) return 0.12;
    if (edge.hasClass("related") || edge.hasClass("focused") || edge.hasClass("failed")) return 1;
    return 0.78;
}

function edgeWidth(edge: Cytoscape.EdgeSingular): number {
    if (edge.hasClass("related") || edge.hasClass("focused") || edge.hasClass("failed")) return 3.2;
    return 2.15;
}

function ensureOverlayRoot(cy: Cytoscape.Core): {
    svg: SVGSVGElement;
    defs: SVGDefsElement;
} | null {
    const container = cy.container();
    if (!container) return null;

    const host = container.firstElementChild as HTMLElement | null;
    if (!host) return null;

    let layer = host.querySelector(".cy-internal-edges-layer") as HTMLDivElement | null;
    if (!layer) {
        layer = document.createElement("div");
        layer.className = "cy-internal-edges-layer";
        host.appendChild(layer);
    }

    let svg = layer.querySelector("svg.cy-internal-edges-svg") as SVGSVGElement | null;
    if (!svg) {
        svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
        svg.classList.add("cy-internal-edges-svg");
        layer.appendChild(svg);
    }

    let defs = svg.querySelector("defs") as SVGDefsElement | null;
    if (!defs) {
        defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
        svg.insertBefore(defs, svg.firstChild);
    }

    ARROW_MARKERS.forEach(({ id, fill }) => {
        let marker = defs!.querySelector(`#${id}`) as SVGMarkerElement | null;
        if (!marker) {
            marker = document.createElementNS("http://www.w3.org/2000/svg", "marker");
            marker.setAttribute("id", id);
            defs!.appendChild(marker);
            const arrowPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
            marker.appendChild(arrowPath);
        }
        marker.setAttribute("viewBox", ARROW_MARKER.viewBox);
        marker.setAttribute("refX", ARROW_MARKER.refX);
        marker.setAttribute("refY", ARROW_MARKER.refY);
        marker.setAttribute("markerWidth", ARROW_MARKER.markerWidth);
        marker.setAttribute("markerHeight", ARROW_MARKER.markerHeight);
        marker.setAttribute("markerUnits", "userSpaceOnUse");
        marker.setAttribute("orient", "auto-start-reverse");
        const arrowPath = marker.querySelector("path");
        if (arrowPath) {
            arrowPath.setAttribute("d", ARROW_MARKER.path);
            arrowPath.setAttribute("fill", fill);
        }
    });

    return { svg, defs };
}

function syncInternalEdgeOverlay(cy: Cytoscape.Core): void {
    if (cy.destroyed()) return;
    const root = ensureOverlayRoot(cy);
    if (!root) return;

    const { svg } = root;
    const container = cy.container()!;
    svg.setAttribute("width", String(container.clientWidth));
    svg.setAttribute("height", String(container.clientHeight));
    svg.setAttribute("viewBox", `0 0 ${container.clientWidth} ${container.clientHeight}`);

    while (svg.childNodes.length > 1) {
        svg.removeChild(svg.lastChild!);
    }

    cy.edges("[internalMeta]").forEach((edgeEle) => {
        const edge = edgeEle as Cytoscape.EdgeSingular;
        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        path.setAttribute("d", edgePathD(cy, edge));
        path.setAttribute("fill", "none");
        path.setAttribute("stroke", edgeStroke(edge));
        path.setAttribute("stroke-width", String(edgeWidth(edge)));
        path.setAttribute("opacity", String(edgeOpacity(edge)));
        path.setAttribute("marker-end", edgeArrowMarker(edge));
        path.setAttribute("stroke-linecap", "round");
        path.setAttribute("stroke-linejoin", "round");
        svg.appendChild(path);
    });
}

export function initInternalEdgeOverlay(cy: Cytoscape.Core): void {
    if (overlayInitStore.has(cy)) return;
    overlayInitStore.set(cy, true);

    const update = () => syncInternalEdgeOverlay(cy);
    cy.on("render pan zoom position add remove data", update);
    update();
}

export function refreshInternalEdgeOverlay(cy: Cytoscape.Core): void {
    if (cy.destroyed() || !overlayInitStore.has(cy)) return;
    syncInternalEdgeOverlay(cy);
}
