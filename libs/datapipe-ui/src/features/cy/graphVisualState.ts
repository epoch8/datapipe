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
const labelFocusStore = new WeakMap<
    Cytoscape.Core,
    { labelKey: string; labelValue: string } | null
>();
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
            labelEl.classList.remove("is-focus-hidden");
            return;
        }

        const id = node.id();
        const selected = paint.selectedIds.has(id);
        const related = paint.highlightedIds.has(id) && !selected;
        const hidden = !paint.highlightedIds.has(id);
        setHtmlFocusClasses(labelEl, selected, related, false);
        labelEl.classList.toggle("is-focus-hidden", hidden);
        if (hidden) {
            labelEl.style.opacity = "0";
        } else if (labelEl.style.opacity === "0" && !labelEl.classList.contains("is-label-hidden")) {
            const stored = node.data("htmlLabelOpacity") as number | undefined;
            labelEl.style.opacity = String(typeof stored === "number" ? stored : 1);
        }
    });
}

function nodeHasLabel(
    node: Cytoscape.NodeSingular,
    labelKey: string,
    labelValue: string,
): boolean {
    const labels = (node.data("labels") as string[][] | undefined) ?? [];
    return labels.some(
        (label) => label.length >= 2 && label[0] === labelKey && label[1] === labelValue,
    );
}

function setHtmlLabelHidden(cy: Cytoscape.Core, nodeId: string, hidden: boolean): void {
    const labelEl = getNodeHtmlLabelEl(cy, nodeId);
    if (!labelEl) return;
    labelEl.classList.toggle("is-label-hidden", hidden);
    // Templates and opacity sync write inline opacity; keep that in sync too so
    // focus hide/show stays correct even if CSS specificity changes later.
    if (hidden || labelEl.classList.contains("is-focus-hidden")) {
        labelEl.style.opacity = "0";
    } else if (!labelEl.style.opacity || labelEl.style.opacity === "0") {
        const node = cy.getElementById(nodeId);
        const stored = node.nonempty()
            ? (node.data("htmlLabelOpacity") as number | undefined)
            : undefined;
        labelEl.style.opacity = String(typeof stored === "number" ? stored : 1);
    }
}

function paintLabelFocus(
    cy: Cytoscape.Core,
    focus: { labelKey: string; labelValue: string } | null,
): void {
    if (!focus) {
        cy.elements().removeClass("label-hidden");
        cy.nodes().forEach((node) => {
            setHtmlLabelHidden(cy, node.id(), false);
        });
        return;
    }

    let visibleNodes = cy.collection() as Cytoscape.NodeCollection;
    cy.nodes().forEach((node) => {
        if (nodeHasLabel(node as Cytoscape.NodeSingular, focus.labelKey, focus.labelValue)) {
            visibleNodes = visibleNodes.union(node) as Cytoscape.NodeCollection;
        }
    });

    // Keep the selected transforms/groups and their boundary tables. This mirrors
    // the prototype: the focused subgraph stays readable without changing layout.
    let expandedVisible = visibleNodes;
    visibleNodes.forEach((node) => {
        expandedVisible = expandedVisible.union(node.connectedEdges().connectedNodes());
        if (node.isParent()) {
            expandedVisible = expandedVisible.union(node.descendants());
        }
        if (node.isChild()) {
            expandedVisible = expandedVisible.union(node.parent());
        }
    });

    const visibleIds = new Set(expandedVisible.map((node) => node.id()));
    cy.batch(() => {
        cy.nodes().forEach((node) => {
            const hidden = !visibleIds.has(node.id());
            node.toggleClass("label-hidden", hidden);
            setHtmlLabelHidden(cy, node.id(), hidden);
        });
        cy.edges().forEach((edge) => {
            edge.toggleClass(
                "label-hidden",
                !visibleIds.has(edge.source().id()) || !visibleIds.has(edge.target().id()),
            );
        });
    });
    refreshInternalEdgeOverlay(cy);
}

function paintNativeNodesAndEdges(cy: Cytoscape.Core, paint: FocusPaint | null): void {
    cy.batch(() => {
        cy.edges().forEach((edge) => {
            const related = paint?.edgeRelatedIds.has(edge.id()) ?? false;
            edge.toggleClass("related", related);
            edge.toggleClass("muted", false);
            edge.toggleClass("focus-hidden", paint != null && !related);
            edge.removeClass("focused");
        });

        cy.nodes().forEach((node) => {
            const id = node.id();
            const focused = paint?.selectedIds.has(id) ?? false;
            const related = paint != null && !focused && paint.highlightedIds.has(id);
            const hidden = paint != null && !paint.highlightedIds.has(id);
            if (!nodeUsesHtmlLabel(node as Cytoscape.NodeSingular)) {
                node.toggleClass("focused", focused);
                node.toggleClass("related", related);
                node.toggleClass("dimmed", false);
            }
            node.toggleClass("focus-hidden", hidden);
        });
    });
}

function expandHighlightWithAncestors(
    nodes: Cytoscape.NodeCollection,
): Cytoscape.NodeCollection {
    let expanded = nodes;
    nodes.forEach((node) => {
        if (node.isChild()) {
            expanded = expanded.union(node.ancestors()) as Cytoscape.NodeCollection;
        }
    });
    return expanded;
}

function computeFocusPaint(cy: Cytoscape.Core): FocusPaint | null {
    const selected = getSelectedNodes(cy);
    if (selected.empty()) return null;

    if (selected.length === 1) {
        const node = selected.first() as Cytoscape.NodeSingular;
        const connectedEdges = node.connectedEdges();
        const highlightedNodes = expandHighlightWithAncestors(
            connectedEdges.connectedNodes().union(node) as Cytoscape.NodeCollection,
        );
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
    highlightedNodes = expandHighlightWithAncestors(highlightedNodes);

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
    // Selection hide must not leave label-filter hide stuck wrong after clear.
    paintLabelFocus(cy, labelFocusStore.get(cy) ?? null);
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

export function setGraphLabelFocus(
    cy: Cytoscape.Core,
    labelKey: string,
    labelValue: string | null | undefined,
): void {
    const focus = labelValue ? { labelKey, labelValue } : null;
    labelFocusStore.set(cy, focus);
    paintLabelFocus(cy, focus);
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
        paintLabelFocus(cy, labelFocusStore.get(cy) ?? null);
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
