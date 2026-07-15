import Cytoscape from "cytoscape";
import { getNodeHtmlLabelEl, nodeUsesHtmlLabel } from "./htmlLabelOpacity";
import { refreshInternalEdgeOverlay } from "./internalEdgeOverlay";

const FOCUS_CLASSES = "focused related muted dimmed";

/** Stable fingerprint of the current selection for idempotent focus. */
let lastFocusKey = "";

function selectionFocusKey(selected: Cytoscape.NodeCollection): string {
    return selected
        .map((n) => n.id())
        .sort()
        .join("\0");
}

export function syncHtmlLabelInteractionState(cy: Cytoscape.Core): void {
    cy.nodes().forEach((node) => {
        if (!nodeUsesHtmlLabel(node as Cytoscape.NodeSingular)) return;

        const n = node as Cytoscape.NodeSingular;
        const selected = n.selected();
        const related = n.hasClass("related");
        // Selected stays blue; related neighbors are orange (not blue-focused).
        const focused = (n.hasClass("focused") || selected) && !related;
        const dimmed = n.hasClass("dimmed");

        // Persist interaction state on node data so it is re-emitted by the
        // html-label template whenever the plugin rebuilds the label DOM (which
        // happens on any style change, e.g. selection border / dim opacity).
        // Without this, classes applied directly below are wiped on rebuild.
        if (Boolean(n.data("uiFocused")) !== focused) n.data("uiFocused", focused);
        if (Boolean(n.data("uiSelected")) !== selected) n.data("uiSelected", selected);
        if (Boolean(n.data("uiRelated")) !== related) n.data("uiRelated", related);
        if (Boolean(n.data("uiDimmed")) !== dimmed) n.data("uiDimmed", dimmed);

        // Immediate feedback on the current DOM element (before any rebuild).
        const labelEl = getNodeHtmlLabelEl(cy, node.id());
        if (labelEl) {
            labelEl.classList.toggle("is-focused", focused);
            labelEl.classList.toggle("is-selected", selected);
            labelEl.classList.toggle("is-related", related);
            labelEl.classList.toggle("is-dimmed", dimmed);
        }
    });
}

function applyNeighborhoodFocus(
    cy: Cytoscape.Core,
    selected: Cytoscape.NodeCollection,
    highlightedEdges: Cytoscape.EdgeCollection,
    highlightedNodes: Cytoscape.NodeCollection,
): void {
    const key = selectionFocusKey(selected);
    // Skip churn when focus already matches this selection (label rebuilds +
    // mouseover noise otherwise flash related/muted/orange edges).
    if (key && key === lastFocusKey && selected.first()?.hasClass("focused")) return;
    lastFocusKey = key;

    // Update classes + ui* data inside one batch so html-label rebuilds (fired
    // when styles flush) already see the correct template flags — otherwise
    // labels briefly render with stale focused/related/dimmed and flash.
    cy.batch(() => {
        cy.elements().removeClass(FOCUS_CLASSES);

        selected.addClass("focused");
        const relatedNodes = highlightedNodes.difference(selected);
        relatedNodes.addClass("related");
        highlightedEdges.addClass("related");
        cy.nodes().not(highlightedNodes).addClass("dimmed");
        cy.edges().not(highlightedEdges).addClass("muted");

        syncHtmlLabelInteractionState(cy);
    });

    refreshInternalEdgeOverlay(cy);
}

export function focusNode(cy: Cytoscape.Core, node: Cytoscape.NodeSingular): void {
    const connectedEdges = node.connectedEdges();
    const neighborNodes = connectedEdges.connectedNodes();
    applyNeighborhoodFocus(cy, node, connectedEdges, neighborNodes.union(node));
}

export function focusSelection(cy: Cytoscape.Core): void {
    const selected = cy.nodes(":selected");
    if (selected.empty()) {
        clearFocus(cy);
        return;
    }
    if (selected.length === 1) {
        focusNode(cy, selected.first() as Cytoscape.NodeSingular);
        return;
    }

    // OR: union of each selected node's neighborhood (not intersection of paths).
    let highlightedEdges = cy.collection() as Cytoscape.EdgeCollection;
    let highlightedNodes = selected;

    selected.forEach((node) => {
        const connectedEdges = node.connectedEdges();
        highlightedEdges = highlightedEdges.union(connectedEdges) as Cytoscape.EdgeCollection;
        highlightedNodes = highlightedNodes.union(connectedEdges.connectedNodes());
    });

    applyNeighborhoodFocus(cy, selected, highlightedEdges, highlightedNodes);
}

export function clearFocus(cy: Cytoscape.Core): void {
    lastFocusKey = "";
    cy.batch(() => {
        cy.elements().removeClass(FOCUS_CLASSES);
        syncHtmlLabelInteractionState(cy);
    });
    refreshInternalEdgeOverlay(cy);
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

    cy.edges().forEach((edge) => {
        const target = edge.target();
        const name = target.data("name") as string | undefined;
        const status = name ? runStatusByStep.get(name) : undefined;
        const failed = status === "failed" || status === "error";
        if (failed) {
            edge.addClass("failed");
        } else {
            edge.removeClass("failed");
        }
    });
    refreshInternalEdgeOverlay(cy);
}
