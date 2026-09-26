import Cytoscape from "cytoscape";
import { getSelectedNodeIds } from "./graphSelection";
import type { InspectorState } from "./NodeInspectorPanel";

export type GraphSessionState = {
    graphUrl: string;
    expandedGroups: string[];
    selectedNodeIds: string[];
    inspectorNodeId: string | null;
    zoom: number;
    pan: { x: number; y: number };
    userInteracted: boolean;
};

const STORAGE_PREFIX = "dp.graphSession.";

function storageKey(graphUrl: string): string {
    return `${STORAGE_PREFIX}${graphUrl}`;
}

export function loadGraphSessionState(graphUrl: string): GraphSessionState | null {
    try {
        const raw = sessionStorage.getItem(storageKey(graphUrl));
        if (!raw) return null;
        const parsed = JSON.parse(raw) as GraphSessionState;
        if (parsed.graphUrl !== graphUrl) return null;
        return parsed;
    } catch {
        return null;
    }
}

export function saveGraphSessionState(state: GraphSessionState, _stageFilter?: string | null): void {
    try {
        sessionStorage.setItem(storageKey(state.graphUrl), JSON.stringify(state));
    } catch {
        /* quota / private mode */
    }
}

export function captureGraphSessionState(
    graphUrl: string,
    expandedGroups: Set<string>,
    inspector: InspectorState,
    cy: Cytoscape.Core | undefined,
    userInteracted: boolean,
): GraphSessionState {
    if (!cy || cy.destroyed()) {
        const selected = inspector ? [inspector.nodeId] : [];
        return {
            graphUrl,
            expandedGroups: Array.from(expandedGroups),
            selectedNodeIds: selected,
            inspectorNodeId: inspector?.nodeId ?? null,
            zoom: 1,
            pan: { x: 0, y: 0 },
            userInteracted,
        };
    }

    const pan = cy.pan();
    const selected = getSelectedNodeIds(cy);

    return {
        graphUrl,
        expandedGroups: Array.from(expandedGroups),
        selectedNodeIds:
            selected.length > 0 ? selected : inspector ? [inspector.nodeId] : [],
        inspectorNodeId: inspector?.nodeId ?? null,
        zoom: cy.zoom(),
        pan: { x: pan.x, y: pan.y },
        userInteracted,
    };
}
