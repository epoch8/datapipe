import Cytoscape from "cytoscape";

/**
 * App-owned selection — avoids Cytoscape `node.select()` / `:selected` styles.
 * Those fire style events that make cytoscape-node-html-label wipe + recreate
 * every label DOM (camera "jump" + blue/orange flicker).
 */
const selectedIdsByCy = new WeakMap<Cytoscape.Core, Set<string>>();

function storeFor(cy: Cytoscape.Core): Set<string> {
    let set = selectedIdsByCy.get(cy);
    if (!set) {
        set = new Set();
        selectedIdsByCy.set(cy, set);
    }
    return set;
}

export function getSelectedNodeIds(cy: Cytoscape.Core): string[] {
    return Array.from(storeFor(cy));
}

export function isNodeSelected(cy: Cytoscape.Core, nodeId: string): boolean {
    return storeFor(cy).has(nodeId);
}

export function getSelectedNodes(cy: Cytoscape.Core): Cytoscape.NodeCollection {
    let collection = cy.collection() as Cytoscape.NodeCollection;
    storeFor(cy).forEach((id) => {
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
}

export function clearSelectedNodeIds(cy: Cytoscape.Core): void {
    selectedIdsByCy.set(cy, new Set());
}

export function toggleSelectedNodeId(cy: Cytoscape.Core, nodeId: string): boolean {
    const set = storeFor(cy);
    if (set.has(nodeId)) {
        set.delete(nodeId);
        return false;
    }
    set.add(nodeId);
    return true;
}
