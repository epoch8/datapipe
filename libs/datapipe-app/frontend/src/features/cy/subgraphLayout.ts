import Cytoscape from "cytoscape";
import {
    SUBGRAPH_GAP,
    SUBGRAPH_STEP_HEIGHT,
    SUBGRAPH_STEP_WIDTH,
    SUBGRAPH_TABLE_HEIGHT,
    SUBGRAPH_TABLE_WIDTH,
} from "./graphNodeLayout";

function nodeCellSize(node: Cytoscape.NodeSingular): { w: number; h: number } {
    if (node.data("type") === "table") {
        return { w: SUBGRAPH_TABLE_WIDTH, h: SUBGRAPH_TABLE_HEIGHT };
    }
    return { w: SUBGRAPH_STEP_WIDTH, h: SUBGRAPH_STEP_HEIGHT };
}

function orderSubgraphNodes(nodes: Cytoscape.NodeCollection): Cytoscape.NodeSingular[] {
    const list = nodes.toArray() as Cytoscape.NodeSingular[];
    return list.sort((a, b) => {
        const typeA = a.data("type");
        const typeB = b.data("type");
        if (typeA !== typeB) {
            return typeA === "transform" ? -1 : 1;
        }
        return String(a.data("name")).localeCompare(String(b.data("name")));
    });
}

/** Place expanded sub-steps in a compact grid at anchor; does not move other graph nodes. */
export function layoutCompactSubgraphAtAnchor(
    nodes: Cytoscape.NodeCollection,
    anchor: Cytoscape.Position,
) {
    const ordered = orderSubgraphNodes(nodes);
    const count = ordered.length;
    if (!count) return;

    const cols = Math.min(count, Math.max(2, Math.ceil(Math.sqrt(count * 1.15))));
    const rows = Math.ceil(count / cols);

    let cellW = SUBGRAPH_STEP_WIDTH + SUBGRAPH_GAP;
    let cellH = SUBGRAPH_STEP_HEIGHT + SUBGRAPH_GAP;
    ordered.forEach((node) => {
        const { w, h } = nodeCellSize(node);
        cellW = Math.max(cellW, w + SUBGRAPH_GAP);
        cellH = Math.max(cellH, h + SUBGRAPH_GAP);
    });

    const gridW = cols * cellW - SUBGRAPH_GAP;
    const gridH = rows * cellH - SUBGRAPH_GAP;
    const originX = anchor.x - gridW / 2;
    const originY = anchor.y - gridH / 2;

    ordered.forEach((node, index) => {
        const col = index % cols;
        const row = Math.floor(index / cols);
        const { w, h } = nodeCellSize(node);
        const target = {
            x: originX + col * cellW + w / 2,
            y: originY + row * cellH + h / 2,
        };
        const current = node.position();
        if (Math.hypot(current.x - target.x, current.y - target.y) < 2) {
            node.position(target);
            return;
        }
        node.animate({
            position: target,
            duration: 220,
            easing: "ease-out-cubic",
        });
    });
}
