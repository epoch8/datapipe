import Cytoscape from "cytoscape";
import { getNodeHtmlLabelEl, nodeUsesHtmlLabel } from "./htmlLabelOpacity";
import { refreshInternalEdgeOverlay } from "./internalEdgeOverlay";

export type FocusPaint = {
    selectedIds: Set<string>;
    highlightedIds: Set<string>;
    edgeRelatedIds: Set<string>;
};

const selectedIdsByCy = new WeakMap<Cytoscape.Core, Set<string>>();
const focusPaintStore = new WeakMap<Cytoscape.Core, FocusPaint | null>();
const labelObserverStore = new WeakMap<Cytoscape.Core, MutationObserver>();

function selectionStore(cy: Cytoscape.Core): Set<string> {
    let set = selectedIdsByCy.get(cy);
    if (!set) {
        set = new Set();
        selectedIdsByCy.set(cy, set);
    }
    return set;
}

function setsEqual(a: Set<string>, b: Set<string>): boolean {
    return a.size === b.size && Array.from(a).every((id) => b.has(id));
}

function setHtmlFocusClasses(
    labelEl: HTMLElement,
    selected: boolean,
    related: boolean,
    dimmed: boolean,
): void {
    labelEl.classList.toggle("is-focused", selected);
    labelEl.classList.toggle("is-selected", selected);
    labelEl.classList.toggle("is-related", related);
    labelEl.classList.toggle("is-dimmed", dimmed);
}

function paintHtmlLabels(cy: Cytoscape.Core, paint: FocusPaint | null): void {
    cy.nodes().forEach((node) => {
        if (!nodeUsesHtmlLabel(node as Cytoscape.NodeSingular)) return;
        const labelEl = getNodeHtmlLabelEl(cy, node.id());
        if (!labelEl) return;

        if (!paint) {
            setHtmlFocusClasses(labelEl, false, false, false);
            return;
        }

        const id = node.id();
        const selected = paint.selectedIds.has(id);
        const related = paint.highlightedIds.has(id) && !selected;
        const dimmed = !paint.highlightedIds.has(id);
        setHtmlFocusClasses(labelEl, selected, related, dimmed);
    });
}

function paintNativeNodesAndEdges(cy: Cytoscape.Core, paint: FocusPaint | null): void {
    cy.batch(() => {
        cy.edges().forEach((edge) => {
            const related = paint?.edgeRelatedIds.has(edge.id()) ?? false;
            const muted = paint != null && !related;
            edge.toggleClass("related", related);
            edge.toggleClass("muted", muted);
            edge.removeClass("focused");
        });

        cy.nodes().forEach((node) => {
            if (nodeUsesHtmlLabel(node as Cytoscape.NodeSingular)) return;
            const id = node.id();
            const focused = paint?.selectedIds.has(id) ?? false;
            const related = paint != null && !focused && paint.highlightedIds.has(id);
            const dimmed = paint != null && !focused && !related;
            node.toggleClass("focused", focused);
            node.toggleClass("related", related);
            node.toggleClass("dimmed", dimmed);
        });
    });
}

function computeFocusPaint(cy: Cytoscape.Core): FocusPaint | null {
    const selected = getSelectedNodes(cy);
    if (selected.empty()) return null;

    if (selected.length === 1) {
        const node = selected.first() as Cytoscape.NodeSingular;
        const connectedEdges = node.connectedEdges();
        const highlightedNodes = connectedEdges.connectedNodes().union(node);
        return {
            selectedIds: new Set([node.id()]),
            highlightedIds: new Set(highlightedNodes.map((n) => n.id())),
            edgeRelatedIds: new Set(connectedEdges.map((e) => e.id())),
        };
    }

    let highlightedEdges = cy.collection() as Cytoscape.EdgeCollection;
    let highlightedNodes = selected;
    selected.forEach((node) => {
        const connectedEdges = node.connectedEdges();
        highlightedEdges = highlightedEdges.union(connectedEdges) as Cytoscape.EdgeCollection;
        highlightedNodes = highlightedNodes.union(connectedEdges.connectedNodes());
    });

    return {
        selectedIds: new Set(selected.map((n) => n.id())),
        highlightedIds: new Set(highlightedNodes.map((n) => n.id())),
        edgeRelatedIds: new Set(highlightedEdges.map((e) => e.id())),
    };
}

function commitFocusPaint(cy: Cytoscape.Core, paint: FocusPaint | null): void {
    const prev = focusPaintStore.get(cy);
    if (
        paint &&
        prev &&
        setsEqual(prev.selectedIds, paint.selectedIds) &&
        setsEqual(prev.highlightedIds, paint.highlightedIds) &&
        setsEqual(prev.edgeRelatedIds, paint.edgeRelatedIds)
    ) {
        paintHtmlLabels(cy, paint);
        return;
    }

    focusPaintStore.set(cy, paint);
    paintNativeNodesAndEdges(cy, paint);
    paintHtmlLabels(cy, paint);
    refreshInternalEdgeOverlay(cy);
}

// --- Selection API ---

export function getSelectedNodeIds(cy: Cytoscape.Core): string[] {
    return Array.from(selectionStore(cy));
}

export function isNodeSelected(cy: Cytoscape.Core, nodeId: string): boolean {
    return selectionStore(cy).has(nodeId);
}

export function getSelectedNodes(cy: Cytoscape.Core): Cytoscape.NodeCollection {
    let collection = cy.collection() as Cytoscape.NodeCollection;
    selectionStore(cy).forEach((id) => {
        const node = cy.getElementById(id);
        if (!node.empty()) {
            collection = collection.union(node) as Cytoscape.NodeCollection;
        }
    });
    return collection;
}

export function setSelectedNodeIds(cy: Cytoscape.Core, nodeIds: string[]): void {
    const next = new Set(
        nodeIds.filter((id) => {
            const node = cy.getElementById(id);
            return !node.empty();
        }),
    );
    selectedIdsByCy.set(cy, next);
    commitFocusPaint(cy, computeFocusPaint(cy));
}

export function clearSelectedNodeIds(cy: Cytoscape.Core): void {
    selectedIdsByCy.set(cy, new Set());
    commitFocusPaint(cy, null);
}

export function toggleSelectedNodeId(cy: Cytoscape.Core, nodeId: string): boolean {
    const set = selectionStore(cy);
    if (set.has(nodeId)) {
        set.delete(nodeId);
        commitFocusPaint(cy, computeFocusPaint(cy));
        return false;
    }
    set.add(nodeId);
    commitFocusPaint(cy, computeFocusPaint(cy));
    return true;
}

// --- Focus / visual sync ---

export function applyGraphVisualState(cy: Cytoscape.Core): void {
    commitFocusPaint(cy, computeFocusPaint(cy));
}

export function clearGraphFocus(cy: Cytoscape.Core): void {
    commitFocusPaint(cy, null);
}

export function syncHtmlLabelInteractionState(cy: Cytoscape.Core): void {
    paintHtmlLabels(cy, focusPaintStore.get(cy) ?? null);
}

export function initHtmlLabelInteractionStateSync(cy: Cytoscape.Core): void {
    if (labelObserverStore.has(cy)) return;
    const container = cy.container();
    if (!container) return;

    const observer = new MutationObserver((mutations) => {
        if (!mutations.some((mutation) => mutation.addedNodes.length > 0)) return;
        paintHtmlLabels(cy, focusPaintStore.get(cy) ?? null);
    });
    observer.observe(container, { childList: true, subtree: true });
    labelObserverStore.set(cy, observer);
    cy.one("destroy", () => {
        observer.disconnect();
        labelObserverStore.delete(cy);
    });
}

export function applyFailedEdgeStyles(
    cy: Cytoscape.Core,
    runStatusByStep?: Map<string, string>,
): void {
    if (!runStatusByStep?.size) {
        cy.edges().removeClass("failed");
        refreshInternalEdgeOverlay(cy);
        return;
    }

    cy.batch(() => {
        cy.edges().forEach((edge) => {
            const target = edge.target();
            const name = target.data("name") as string | undefined;
            const status = name ? runStatusByStep.get(name) : undefined;
            const failed = status === "failed" || status === "error";
            edge.toggleClass("failed", failed);
        });
    });
    refreshInternalEdgeOverlay(cy);
}

/** @deprecated Use applyGraphVisualState */
export function focusSelection(cy: Cytoscape.Core): void {
    applyGraphVisualState(cy);
}

/** @deprecated Use clearGraphFocus */
export function clearFocus(cy: Cytoscape.Core): void {
    clearGraphFocus(cy);
}

/** @deprecated Use applyGraphVisualState after setSelectedNodeIds */
export function focusNode(cy: Cytoscape.Core, node: Cytoscape.NodeSingular): void {
    setSelectedNodeIds(cy, [node.id()]);
}
