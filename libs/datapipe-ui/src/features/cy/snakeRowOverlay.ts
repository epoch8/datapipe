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

/** Soft single-tone zigzag cue — light green, no hard dark edge. */
const GUIDE_STROKE = "rgba(46, 160, 67, 0.28)";

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

/** Open polyline through row midlines + vertical drops through end-node columns. */
export function computeSnakeSpine(rows: SnakeRowGuide[]): Array<{ x: number; y: number }> {
    if (!rows.length) return [];
    const pts: Array<{ x: number; y: number }> = [];
    rows.forEach((row, i) => {
        const midY = (row.y1 + row.y2) / 2;
        const goRight = i % 2 === 0;
        const leftMid = (row.leftX1 + row.leftX2) / 2;
        const rightMid = (row.rightX1 + row.rightX2) / 2;
        if (goRight) {
            pts.push({ x: leftMid, y: midY });
            pts.push({ x: rightMid, y: midY });
        } else {
            pts.push({ x: rightMid, y: midY });
            pts.push({ x: leftMid, y: midY });
        }
        if (i >= rows.length - 1) return;
        const next = rows[i + 1];
        const nextMid = (next.y1 + next.y2) / 2;
        // Drop through the rightmost / leftmost node column (not past the row pad).
        const turnX = goRight
            ? Math.max(rightMid, (next.rightX1 + next.rightX2) / 2)
            : Math.min(leftMid, (next.leftX1 + next.leftX2) / 2);
        const last = pts[pts.length - 1];
        if (Math.abs(last.x - turnX) > 0.5) {
            pts.push({ x: turnX, y: midY });
        }
        pts.push({ x: turnX, y: nextMid });
    });
    return pts;
}

function pointsToOpenPath(points: Array<{ x: number; y: number }>): string {
    if (points.length < 2) return "";
    return points
        .map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)} ${p.y.toFixed(1)}`)
        .join(" ");
}

function spineStrokeWidth(rows: SnakeRowGuide[]): number {
    if (!rows.length) return 64;
    const heights = rows.map((r) => r.y2 - r.y1).sort((a, b) => a - b);
    const median = heights[Math.floor(heights.length / 2)] ?? 120;
    // Keep it a readable tube, not a full row highlight band.
    return Math.max(48, Math.min(120, median * 0.28));
}

function syncGuides(cy: Cytoscape.Core): void {
    const root = ensureLayer(cy);
    if (!root) return;
    const { layer, svg } = root;
    updateCamera(cy, layer);

    while (svg.firstChild) svg.removeChild(svg.firstChild);

    if (!enabledStore.get(cy)) return;
    const layout = layoutStore.get(cy);
    if (!layout) return;

    const rows = computeSnakeRowGuides(layout);
    if (rows.length < 1) return;
    const spine = computeSnakeSpine(rows);
    if (spine.length < 2) return;

    const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("class", "cy-snake-boundary");

    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", pointsToOpenPath(spine));
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", GUIDE_STROKE);
    path.setAttribute("stroke-width", String(spineStrokeWidth(rows)));
    path.setAttribute("stroke-linejoin", "round");
    path.setAttribute("stroke-linecap", "round");
    g.appendChild(path);

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
