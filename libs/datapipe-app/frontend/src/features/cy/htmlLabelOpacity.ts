import Cytoscape from "cytoscape";
import { ANIMATION_MS } from "./animationConstants";

function easeInOutCubic(t: number): number {
    return t < 0.5 ? 4 * t * t * t : 1 - (-2 * t + 2) ** 3 / 2;
}

const opacityStore = new WeakMap<Cytoscape.Core, Map<string, number>>();
const syncInitStore = new WeakMap<Cytoscape.Core, true>();
const activeRafStore = new WeakMap<Cytoscape.Core, Set<number>>();

function getOpacityStore(cy: Cytoscape.Core): Map<string, number> {
    let store = opacityStore.get(cy);
    if (!store) {
        store = new Map();
        opacityStore.set(cy, store);
    }
    return store;
}

function trackRaf(cy: Cytoscape.Core, id: number): void {
    let set = activeRafStore.get(cy);
    if (!set) {
        set = new Set();
        activeRafStore.set(cy, set);
    }
    set.add(id);
}

export function initHtmlLabelOpacitySync(cy: Cytoscape.Core): void {
    if (syncInitStore.has(cy)) return;
    syncInitStore.set(cy, true);
    getOpacityStore(cy);

    cy.on("render", () => {
        if (cy.destroyed()) return;
        const store = opacityStore.get(cy);
        cy.nodes().forEach((node) => {
            if (!nodeUsesHtmlLabel(node as Cytoscape.NodeSingular)) return;
            const nodeId = node.id();
            const opacity =
                store?.get(nodeId) ??
                (node.data("htmlLabelOpacity") as number | undefined);
            if (typeof opacity !== "number") return;
            const labelEl = getNodeHtmlLabelEl(cy, nodeId);
            if (labelEl) {
                labelEl.style.opacity = String(opacity);
            }
        });
    });
}

export function stopHtmlOpacityAnimations(cy: Cytoscape.Core): void {
    const rafs = activeRafStore.get(cy);
    if (rafs) {
        rafs.forEach((id) => cancelAnimationFrame(id));
        rafs.clear();
    }
}

export function getNodeHtmlLabelEl(cy: Cytoscape.Core, nodeId: string): HTMLElement | null {
    const container = cy.container();
    if (!container) return null;
    return container.querySelector(`[data-cy-node-id="${CSS.escape(nodeId)}"]`);
}

/** Transform/table/group labels are HTML; group-expanded uses native cytoscape drawing. */
export function nodeUsesHtmlLabel(node: Cytoscape.NodeSingular): boolean {
    const type = node.data("type") as string;
    return type === "transform" || type === "table" || type === "group";
}

export function ensureGroupExpandedVisible(node: Cytoscape.NodeSingular): void {
    if (node.data("type") !== "group-expanded") return;
    node.removeStyle("opacity");
    node.style("opacity", 1);
}

export function setNodeVisualOpacity(
    cy: Cytoscape.Core,
    node: Cytoscape.NodeSingular,
    opacity: number,
): void {
    getOpacityStore(cy).set(node.id(), opacity);
    node.data("htmlLabelOpacity", opacity);

    if (nodeUsesHtmlLabel(node)) {
        const labelEl = getNodeHtmlLabelEl(cy, node.id());
        if (labelEl) {
            labelEl.style.opacity = String(opacity);
        }
        // Canvas node is transparent for html-labeled types; keep it invisible.
        node.style("opacity", 0);
        return;
    }
    node.style("opacity", opacity);
}

export function animateNodeVisualOpacity(
    cy: Cytoscape.Core,
    nodeId: string,
    fromOpacity: number,
    toOpacity: number,
    duration = ANIMATION_MS,
    onComplete?: () => void,
): void {
    const node = cy.getElementById(nodeId);
    if (node.empty()) {
        onComplete?.();
        return;
    }

    const nodeEl = node as Cytoscape.NodeSingular;
    const usesHtml = nodeUsesHtmlLabel(nodeEl);
    const start = performance.now();

    const tick = (now: number) => {
        const t = Math.min(1, (now - start) / duration);
        const eased = easeInOutCubic(t);
        const opacity = fromOpacity + (toOpacity - fromOpacity) * eased;

        if (usesHtml) {
            const labelEl = getNodeHtmlLabelEl(cy, nodeId);
            if (labelEl) {
                labelEl.style.opacity = String(opacity);
            }
            getOpacityStore(cy).set(nodeId, opacity);
            nodeEl.data("htmlLabelOpacity", opacity);
            nodeEl.style("opacity", 0);
        } else {
            nodeEl.style("opacity", opacity);
            getOpacityStore(cy).set(nodeId, opacity);
        }

        if (t < 1) {
            const rafId = requestAnimationFrame(tick);
            trackRaf(cy, rafId);
        } else {
            setNodeVisualOpacity(cy, nodeEl, toOpacity);
            onComplete?.();
        }
    };

    const rafId = requestAnimationFrame(tick);
    trackRaf(cy, rafId);
}
