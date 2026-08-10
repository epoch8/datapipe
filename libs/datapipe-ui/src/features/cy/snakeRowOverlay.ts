import Cytoscape from "cytoscape";
import {
    computeSnakeRowGuides,
    GraphLayout,
    SnakeRowGuide,
} from "./incrementalLayout";

const layerStore = new WeakMap<Cytoscape.Core, HTMLDivElement>();
const initStore = new WeakMap<Cytoscape.Core, true>();
const enabledStore = new WeakMap<Cytoscape.Core, boolean>();
const layoutStore = new WeakMap<Cytoscape.Core, GraphLayout>();

const GUIDE_STROKE = "rgba(91, 168, 160, 0.45)";
const GUIDE_WIDTH = 2.2;

function ensureLayer(cy: Cytoscape.Core): { layer: HTMLDivElement; svg: SVGSVGElement } | null {
    const container = cy.container();
    if (!container) return null;
    const host = container.firstElementChild as HTMLElement | null;
    if (!host) return null;

    let layer = layerStore.get(cy) ?? null;
    if (!layer || !host.contains(layer)) {
        layer = host.querySelector(".cy-snake-rows-layer") as HTMLDivElement | null;
        if (!layer) {
            layer = document.createElement("div");
            layer.className = "cy-snake-rows-layer";
            host.appendChild(layer);
        }
        layerStore.set(cy, layer);
    }

    let svg = layer.querySelector("svg.cy-snake-rows-svg") as SVGSVGElement | null;
    if (!svg) {
        svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
        svg.classList.add("cy-snake-rows-svg");
        layer.appendChild(svg);
    }
    return { layer, svg };
}

function updateCamera(cy: Cytoscape.Core, layer: HTMLDivElement): void {
    const pan = cy.pan();
    const zoom = Math.max(cy.zoom(), 1e-6);
    layer.style.transform = `translate(${pan.x}px,${pan.y}px) scale(${zoom})`;
    layer.style.transformOrigin = "top left";
}

function pointsToPath(points: Array<{ x: number; y: number }>): string {
    if (!points.length) return "";
    return points
        .map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)} ${p.y.toFixed(1)}`)
        .join(" ");
}

/** Selection focus hides most nodes — keep the direction ribbon out of the way. */
function hasActiveFocus(cy: Cytoscape.Core): boolean {
    return cy.nodes(".focus-hidden").length > 0 || cy.nodes(".label-hidden").length > 0;
}

function syncGuides(cy: Cytoscape.Core): void {
    const root = ensureLayer(cy);
    if (!root) return;
    const { layer, svg } = root;
    updateCamera(cy, layer);

    while (svg.firstChild) svg.removeChild(svg.firstChild);

    if (!enabledStore.get(cy)) return;
    // Overview cue only — filled bands over empty focus looked like "stripes".
    if (hasActiveFocus(cy)) return;
    const layout = layoutStore.get(cy);
    if (!layout) return;

    const guides = computeSnakeRowGuides(layout);
    if (guides.length < 1) return;

    const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
    const marker = document.createElementNS("http://www.w3.org/2000/svg", "marker");
    marker.setAttribute("id", "cy-snake-arrow");
    marker.setAttribute("viewBox", "0 0 10 8");
    marker.setAttribute("refX", "9");
    marker.setAttribute("refY", "4");
    marker.setAttribute("markerWidth", "7");
    marker.setAttribute("markerHeight", "6");
    marker.setAttribute("orient", "auto");
    marker.setAttribute("markerUnits", "userSpaceOnUse");
    const arrow = document.createElementNS("http://www.w3.org/2000/svg", "path");
    arrow.setAttribute("d", "M0 0 L10 4 L0 8 Z");
    arrow.setAttribute("fill", GUIDE_STROKE);
    marker.appendChild(arrow);
    defs.appendChild(marker);
    svg.appendChild(defs);

    const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("class", "cy-snake-rows");

    guides.forEach((guide: SnakeRowGuide, index) => {
        if (guide.points.length < 2) return;

        const line = document.createElementNS("http://www.w3.org/2000/svg", "path");
        line.setAttribute("d", pointsToPath(guide.points));
        line.setAttribute("fill", "none");
        line.setAttribute("stroke", GUIDE_STROKE);
        line.setAttribute("stroke-width", String(GUIDE_WIDTH));
        line.setAttribute("stroke-linecap", "round");
        line.setAttribute("stroke-linejoin", "round");
        line.setAttribute("marker-end", "url(#cy-snake-arrow)");
        g.appendChild(line);

        const next = guides[index + 1];
        if (!next || next.points.length < 1) return;
        const from = guide.points[guide.points.length - 1];
        const to = next.points[0];
        const midY = (from.y + to.y) / 2;
        const turn = document.createElementNS("http://www.w3.org/2000/svg", "path");
        turn.setAttribute(
            "d",
            `M${from.x.toFixed(1)} ${from.y.toFixed(1)} L${from.x.toFixed(1)} ${midY.toFixed(1)} L${to.x.toFixed(1)} ${midY.toFixed(1)} L${to.x.toFixed(1)} ${to.y.toFixed(1)}`,
        );
        turn.setAttribute("fill", "none");
        turn.setAttribute("stroke", GUIDE_STROKE);
        turn.setAttribute("stroke-width", String(GUIDE_WIDTH * 0.85));
        turn.setAttribute("stroke-linecap", "round");
        turn.setAttribute("stroke-linejoin", "round");
        turn.setAttribute("stroke-dasharray", "8 7");
        g.appendChild(turn);
    });

    svg.appendChild(g);
}

export function setSnakeRowOverlayLayout(
    cy: Cytoscape.Core,
    layout: GraphLayout | null,
    enabled: boolean,
): void {
    enabledStore.set(cy, enabled);
    if (layout) layoutStore.set(cy, layout);
    else layoutStore.delete(cy);
    if (!cy.destroyed()) syncGuides(cy);
}

export function refreshSnakeRowOverlay(cy: Cytoscape.Core): void {
    if (cy.destroyed()) return;
    syncGuides(cy);
}

export function initSnakeRowOverlay(cy: Cytoscape.Core): void {
    if (initStore.has(cy)) return;
    initStore.set(cy, true);

    let cameraFrame = 0;
    const updateCameraOnly = () => {
        cancelAnimationFrame(cameraFrame);
        cameraFrame = requestAnimationFrame(() => {
            if (cy.destroyed()) return;
            const layer = layerStore.get(cy);
            if (layer) updateCamera(cy, layer);
        });
    };

    cy.on("pan zoom resize", updateCameraOnly);
    cy.one("render", () => syncGuides(cy));
    cy.one("destroy", () => {
        initStore.delete(cy);
        layerStore.delete(cy);
        enabledStore.delete(cy);
        layoutStore.delete(cy);
    });
}
