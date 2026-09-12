import Cytoscape from "cytoscape";
import { ANIMATION_MS } from "./animationConstants";

function easeInOutCubic(t: number): number {
    return t < 0.5 ? 4 * t * t * t : 1 - (-2 * t + 2) ** 3 / 2;
}

const opacityStore = new WeakMap<Cytoscape.Core, Map<string, number>>();
const syncInitStore = new WeakMap<Cytoscape.Core, true>();
/** Current opacity RAF per node — never accumulate (stale ids cancel recycled rAFs). */
const activeRafStore = new WeakMap<Cytoscape.Core, Map<string, number>>();
const pendingOpacityStore = new WeakMap<
    Cytoscape.Core,
    Map<string, { toOpacity: number }>
>();

function getOpacityStore(cy: Cytoscape.Core): Map<string, number> {
    let store = opacityStore.get(cy);
    if (!store) {
        store = new Map();
        opacityStore.set(cy, store);
    }
    return store;
}

function getPendingOpacities(cy: Cytoscape.Core): Map<string, { toOpacity: number }> {
    let map = pendingOpacityStore.get(cy);
    if (!map) {
        map = new Map();
        pendingOpacityStore.set(cy, map);
    }
    return map;
}

function getActiveRafs(cy: Cytoscape.Core): Map<string, number> {
    let map = activeRafStore.get(cy);
    if (!map) {
        map = new Map();
        activeRafStore.set(cy, map);
    }
    return map;
}

function trackRaf(cy: Cytoscape.Core, nodeId: string, id: number): void {
    getActiveRafs(cy).set(nodeId, id);
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
            // Label-focus hide owns opacity while active; don't fight it on pan/zoom.
            if (labelEl && !labelEl.classList.contains("is-label-hidden")) {
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
    // Jump unfinished fades to their target (mirrors cy.stop jumpToEnd). Do not
    // invoke per-animation onComplete — a newer sync owns the transition now.
    const pending = pendingOpacityStore.get(cy);
    if (!pending?.size) return;
    pending.forEach(({ toOpacity }, nodeId) => {
        const node = cy.getElementById(nodeId);
        if (!node.empty()) {
            setNodeVisualOpacity(cy, node as Cytoscape.NodeSingular, toOpacity);
        }
    });
    pending.clear();
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
    delay = 0,
): void {
    const node = cy.getElementById(nodeId);
    if (node.empty()) {
        onComplete?.();
        return;
    }

    const nodeEl = node as Cytoscape.NodeSingular;
    const usesHtml = nodeUsesHtmlLabel(nodeEl);
    const startAt = performance.now() + delay;
    const pending = getPendingOpacities(cy);
    pending.set(nodeId, { toOpacity });

    const tick = (now: number) => {
        // Interrupted / superseded by stopHtmlOpacityAnimations.
        if (!pending.has(nodeId)) return;

        if (now < startAt) {
            trackRaf(cy, nodeId, requestAnimationFrame(tick));
            return;
        }

        const t = Math.min(1, (now - startAt) / duration);
        const eased = easeInOutCubic(t);
        const opacity = fromOpacity + (toOpacity - fromOpacity) * eased;

        if (usesHtml) {
            const labelEl = getNodeHtmlLabelEl(cy, nodeId);
            if (labelEl) {
                labelEl.style.opacity = String(opacity);
            }
            getOpacityStore(cy).set(nodeId, opacity);
            // Avoid node.data() every frame — it fires cy `data` handlers and
            // can stall the parallel box-morph RAF under load.
            nodeEl.style("opacity", 0);
        } else {
            nodeEl.style("opacity", opacity);
            getOpacityStore(cy).set(nodeId, opacity);
        }

        if (t < 1) {
            trackRaf(cy, nodeId, requestAnimationFrame(tick));
        } else {
            pending.delete(nodeId);
            getActiveRafs(cy).delete(nodeId);
            setNodeVisualOpacity(cy, nodeEl, toOpacity);
            onComplete?.();
        }
    };

    trackRaf(cy, nodeId, requestAnimationFrame(tick));
}
