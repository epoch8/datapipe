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

/** Shortest directed path (following edge direction) between two nodes. */
export function findDirectedPathEdges(
    cy: Cytoscape.Core,
    sourceId: string,
    targetId: string,
): Cytoscape.EdgeCollection | null {
    if (sourceId === targetId) return null;

    type QueueItem = { nodeId: string; edges: Cytoscape.EdgeSingular[] };
    const queue: QueueItem[] = [{ nodeId: sourceId, edges: [] }];
    const visited = new Set<string>([sourceId]);

    while (queue.length) {
        const { nodeId, edges } = queue.shift()!;
        if (nodeId === targetId) {
            return edges.length ? cy.collection(edges) : null;
        }

        const node = cy.getElementById(nodeId);
        if (node.empty()) continue;

        node.outgoers("edge").forEach((edge) => {
            const nextId = edge.target().id();
            if (visited.has(nextId)) return;
            visited.add(nextId);
            queue.push({
                nodeId: nextId,
                edges: [...edges, edge as Cytoscape.EdgeSingular],
            });
        });
    }
    return null;
}

function collectPathsBetweenSelected(
    cy: Cytoscape.Core,
    selected: Cytoscape.NodeCollection,
): { pathEdges: Cytoscape.EdgeCollection; pathNodes: Cytoscape.NodeCollection } {
    let pathEdges = cy.collection();
    let pathNodes = cy.collection();

    selected.forEach((source) => {
        selected.forEach((target) => {
            if (source.id() === target.id()) return;
            const edges = findDirectedPathEdges(cy, source.id(), target.id());
            if (!edges || edges.empty()) return;
            pathEdges = pathEdges.union(edges);
            pathNodes = pathNodes.union(edges.connectedNodes());
        });
    });

    return { pathEdges, pathNodes };
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

    const { pathEdges, pathNodes } = collectPathsBetweenSelected(cy, selected);
    const highlightedNodes = selected.union(pathNodes);
    const highlightedEdges = pathEdges;

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
