import Cytoscape from "cytoscape";
import { getNodeHtmlLabelEl, nodeUsesHtmlLabel } from "./htmlLabelOpacity";

export function syncHtmlLabelInteractionState(cy: Cytoscape.Core): void {
    cy.nodes().forEach((node) => {
        if (!nodeUsesHtmlLabel(node as Cytoscape.NodeSingular)) return;
        const labelEl = getNodeHtmlLabelEl(cy, node.id());
        if (!labelEl) return;

        const n = node as Cytoscape.NodeSingular;
        const focused = n.hasClass("focused") || n.selected();
        labelEl.classList.toggle("is-focused", focused);
        labelEl.classList.toggle("is-selected", n.selected());
        labelEl.classList.toggle("is-dimmed", n.hasClass("dimmed"));
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
}

export function clearFocus(cy: Cytoscape.Core): void {
    cy.elements().removeClass("focused muted dimmed");
    syncHtmlLabelInteractionState(cy);
}

export function applyFailedEdgeStyles(
    cy: Cytoscape.Core,
    runStatusByStep?: Map<string, string>,
): void {
    if (!runStatusByStep?.size) {
        cy.edges().removeClass("failed");
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
}
