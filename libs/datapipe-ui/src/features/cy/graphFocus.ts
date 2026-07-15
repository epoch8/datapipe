import Cytoscape from "cytoscape";
import { getNodeHtmlLabelEl, nodeUsesHtmlLabel } from "./htmlLabelOpacity";
import { getSelectedNodes } from "./graphSelection";
import { refreshInternalEdgeOverlay } from "./internalEdgeOverlay";

type HtmlFocusPaint = {
    selectedIds: Set<string>;
    highlightedIds: Set<string>;
    edgeRelatedIds: Set<string>;
};

const htmlFocusPaintStore = new WeakMap<Cytoscape.Core, HtmlFocusPaint | null>();
let lastFocusKey = "";

function selectionFocusKey(selectedIds: Set<string>): string {
    return Array.from(selectedIds).sort().join("\0");
}

function freezeViewport(cy: Cytoscape.Core): () => void {
    const pan = { x: cy.pan().x, y: cy.pan().y };
    const zoom = cy.zoom();
    const restore = () => {
        if (cy.destroyed()) return;
        const cur = cy.pan();
        if (cy.zoom() !== zoom || cur.x !== pan.x || cur.y !== pan.y) {
            cy.viewport({ zoom, pan });
        }
    };
    restore();
    return restore;
}

function paintHtmlLabelFocus(cy: Cytoscape.Core, paint: HtmlFocusPaint | null): void {
    cy.nodes().forEach((node) => {
        if (!nodeUsesHtmlLabel(node as Cytoscape.NodeSingular)) return;
        const labelEl = getNodeHtmlLabelEl(cy, node.id());
        if (!labelEl) return;

        if (!paint) {
            labelEl.classList.remove("is-focused", "is-selected", "is-related", "is-dimmed");
            return;
        }

        const id = node.id();
        const selected = paint.selectedIds.has(id);
        const related = paint.highlightedIds.has(id) && !selected;
        const dimmed = !paint.highlightedIds.has(id);

        labelEl.classList.toggle("is-focused", selected);
        labelEl.classList.toggle("is-selected", selected);
        labelEl.classList.toggle("is-related", related);
        labelEl.classList.toggle("is-dimmed", dimmed);
    });
}

/**
 * Edge + native (non-HTML) node chrome only.
 * Never touch HTML-labeled node classes/data — that rebuilds labels.
 */
function applyCyEdgeFocus(cy: Cytoscape.Core, paint: HtmlFocusPaint | null): void {
    cy.batch(() => {
        cy.edges().removeClass("related muted focused");
        cy.nodes().forEach((n) => {
            if (nodeUsesHtmlLabel(n as Cytoscape.NodeSingular)) return;
            n.removeClass("focused related dimmed");
        });

        if (!paint) return;

        cy.edges().forEach((edge) => {
            if (paint.edgeRelatedIds.has(edge.id())) {
                edge.addClass("related");
            } else {
                edge.addClass("muted");
            }
        });

        cy.nodes().forEach((n) => {
            if (nodeUsesHtmlLabel(n as Cytoscape.NodeSingular)) return;
            const id = n.id();
            if (paint.selectedIds.has(id)) n.addClass("focused");
            else if (paint.highlightedIds.has(id)) n.addClass("related");
            else n.addClass("dimmed");
        });
    });
}

function applyNeighborhoodFocus(
    cy: Cytoscape.Core,
    selected: Cytoscape.NodeCollection,
    highlightedEdges: Cytoscape.EdgeCollection,
    highlightedNodes: Cytoscape.NodeCollection,
): void {
    const selectedIds = new Set(selected.map((n) => n.id()));
    const highlightedIds = new Set(highlightedNodes.map((n) => n.id()));
    const edgeRelatedIds = new Set(highlightedEdges.map((e) => e.id()));
    const key = selectionFocusKey(selectedIds);

    const prev = htmlFocusPaintStore.get(cy);
    if (
        key &&
        key === lastFocusKey &&
        prev &&
        prev.selectedIds.size === selectedIds.size &&
        Array.from(selectedIds).every((id) => prev.selectedIds.has(id)) &&
        prev.highlightedIds.size === highlightedIds.size &&
        Array.from(highlightedIds).every((id) => prev.highlightedIds.has(id))
    ) {
        return;
    }
    lastFocusKey = key;

    const paint: HtmlFocusPaint = { selectedIds, highlightedIds, edgeRelatedIds };
    htmlFocusPaintStore.set(cy, paint);

    const restoreViewport = freezeViewport(cy);

    applyCyEdgeFocus(cy, paint);
    paintHtmlLabelFocus(cy, paint);
    refreshInternalEdgeOverlay(cy);

    restoreViewport();
    requestAnimationFrame(() => {
        restoreViewport();
        paintHtmlLabelFocus(cy, htmlFocusPaintStore.get(cy) ?? null);
    });
}

export function syncHtmlLabelInteractionState(cy: Cytoscape.Core): void {
    paintHtmlLabelFocus(cy, htmlFocusPaintStore.get(cy) ?? null);
}

export function focusNode(cy: Cytoscape.Core, node: Cytoscape.NodeSingular): void {
    const connectedEdges = node.connectedEdges();
    const neighborNodes = connectedEdges.connectedNodes();
    applyNeighborhoodFocus(cy, node, connectedEdges, neighborNodes.union(node));
}

export function focusSelection(cy: Cytoscape.Core): void {
    const selected = getSelectedNodes(cy);
    if (selected.empty()) {
        clearFocus(cy);
        return;
    }
    if (selected.length === 1) {
        focusNode(cy, selected.first() as Cytoscape.NodeSingular);
        return;
    }

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
    htmlFocusPaintStore.set(cy, null);
    const restoreViewport = freezeViewport(cy);
    applyCyEdgeFocus(cy, null);
    paintHtmlLabelFocus(cy, null);
    refreshInternalEdgeOverlay(cy);
    restoreViewport();
    requestAnimationFrame(restoreViewport);
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
