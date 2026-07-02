import Cytoscape from "cytoscape";
import { getNodeHtmlLabelEl, nodeUsesHtmlLabel } from "./htmlLabelOpacity";
import { refreshInternalEdgeOverlay } from "./internalEdgeOverlay";

export function syncHtmlLabelInteractionState(cy: Cytoscape.Core): void {
    cy.nodes().forEach((node) => {
        if (!nodeUsesHtmlLabel(node as Cytoscape.NodeSingular)) return;

        const n = node as Cytoscape.NodeSingular;
        const selected = n.selected();
        const focused = n.hasClass("focused") || selected;
        const dimmed = n.hasClass("dimmed");

        // Persist interaction state on node data so it is re-emitted by the
        // html-label template whenever the plugin rebuilds the label DOM (which
        // happens on any style change, e.g. selection border / dim opacity).
        // Without this, classes applied directly below are wiped on rebuild.
        if (Boolean(n.data("uiFocused")) !== focused) n.data("uiFocused", focused);
        if (Boolean(n.data("uiSelected")) !== selected) n.data("uiSelected", selected);
        if (Boolean(n.data("uiDimmed")) !== dimmed) n.data("uiDimmed", dimmed);

        // Immediate feedback on the current DOM element (before any rebuild).
        const labelEl = getNodeHtmlLabelEl(cy, node.id());
        if (labelEl) {
            labelEl.classList.toggle("is-focused", focused);
            labelEl.classList.toggle("is-selected", selected);
            labelEl.classList.toggle("is-dimmed", dimmed);
        }
    });
}

export function focusNode(cy: Cytoscape.Core, node: Cytoscape.NodeSingular): void {
    const connectedEdges = node.connectedEdges();
    const neighborNodes = connectedEdges.connectedNodes();

    cy.elements().removeClass("focused muted dimmed");

    node.addClass("focused");
    connectedEdges.addClass("focused");
    cy.edges().not(connectedEdges).addClass("muted");
    cy.nodes().not(neighborNodes.union(node)).addClass("dimmed");

    syncHtmlLabelInteractionState(cy);
    refreshInternalEdgeOverlay(cy);
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
    let highlightedEdges = cy.collection();
    let highlightedNodes = selected;

    selected.forEach((node) => {
        const connectedEdges = node.connectedEdges();
        highlightedEdges = highlightedEdges.union(connectedEdges);
        highlightedNodes = highlightedNodes.union(connectedEdges.connectedNodes());
    });

    cy.elements().removeClass("focused muted dimmed");

    selected.addClass("focused");
    highlightedEdges.addClass("focused");
    cy.nodes().not(highlightedNodes).addClass("dimmed");
    cy.edges().not(highlightedEdges).addClass("muted");

    syncHtmlLabelInteractionState(cy);
    refreshInternalEdgeOverlay(cy);
}

export function clearFocus(cy: Cytoscape.Core): void {
    cy.elements().removeClass("focused muted dimmed");
    syncHtmlLabelInteractionState(cy);
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
