import React from "react";
import Cytoscape from "cytoscape";
import { GraphNodeDetailBody } from "./GraphNodeDetailContent";

export type InspectorState = {
    nodeId: string;
    data: Cytoscape.NodeDataDefinition;
} | null;

type NodeInspectorPanelProps = {
    inspector: InspectorState;
    graphNodesById: Map<string, Cytoscape.NodeDataDefinition>;
    runStatusByStep?: Map<string, string>;
    width: number;
    dragging?: boolean;
    onHandleMouseDown: (event: React.MouseEvent) => void;
    onClose: () => void;
    onNavigateToNode?: (nodeId: string) => void;
    onOpenDetails?: () => void;
};

function InspectorEmptyState() {
    return (
        <div className="node-inspector-empty">
            <div className="node-inspector-empty-title">No node selected</div>
            <div className="node-inspector-empty-hint">
                Click a table, transform or group in the graph to see its details here.
            </div>
        </div>
    );
}

export function NodeInspectorPanel({
    inspector,
    graphNodesById,
    runStatusByStep,
    width,
    dragging,
    onHandleMouseDown,
    onClose,
    onNavigateToNode,
    onOpenDetails,
}: NodeInspectorPanelProps) {
    return (
        <aside
            className={`node-inspector-panel${dragging ? " is-resizing" : ""}`}
            style={{ width, flex: `0 0 ${width}px` }}
        >
            <div
                className="dp-resize-handle dp-resize-handle-left"
                role="separator"
                aria-orientation="vertical"
                onMouseDown={onHandleMouseDown}
            />
            <div className="node-inspector-panel-content">
                {!inspector ? (
                    <InspectorEmptyState />
                ) : (
                    <GraphNodeDetailBody
                        node={inspector.data}
                        graphNodesById={graphNodesById}
                        runStatus={runStatusByStep?.get(String(inspector.data.name))}
                        onClose={onClose}
                        onOpenDetails={onOpenDetails}
                        onNavigateToNode={onNavigateToNode}
                        showTableData
                        showMetaTable
                    />
                )}
            </div>
        </aside>
    );
}
