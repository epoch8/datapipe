import React from "react";
import Cytoscape from "cytoscape";
import { getTransformPrimaryKeys } from "./nodeKeyChips";

type InspectorSectionProps = {
    title: string;
    count?: string;
    children: React.ReactNode;
};

function InspectorSection({ title, count, children }: InspectorSectionProps) {
    return (
        <section className="inspector-section">
            <div className="inspector-section-title">
                <span>{title}</span>
                {count ? <span className="inspector-section-count">{count}</span> : null}
            </div>
            {children}
        </section>
    );
}

function KeyChipList({ kind, keys }: { kind: "pk" | "tpk"; keys: string[] }) {
    if (!keys.length) {
        return <span style={{ color: "#667085", fontSize: 12 }}>No keys</span>;
    }
    return (
        <div className="inspector-chip-list">
            {keys.map((key) => (
                <span key={key} className={`node-key-chip ${kind}`}>
                    {key}
                </span>
            ))}
        </div>
    );
}

function nodeKeysForInspector(
    node: Cytoscape.NodeDataDefinition | undefined,
    kind: "pk" | "tpk",
): string[] {
    if (!node) return [];
    if (kind === "pk") return (node.indexes as string[]) ?? [];
    return getTransformPrimaryKeys(node);
}

function isInspectableGraphNode(node: Cytoscape.NodeDataDefinition | undefined): boolean {
    if (!node) return false;
    const type = node.type as string;
    return type === "table" || type === "transform" || type === "group";
}

function InspectorNodeLink({
    nodeId,
    node,
    onNavigate,
}: {
    nodeId: string;
    node?: Cytoscape.NodeDataDefinition;
    onNavigate?: (nodeId: string) => void;
}) {
    const label = node?.name ? String(node.name) : nodeId;
    if (!onNavigate || !isInspectableGraphNode(node)) {
        return <>{label}</>;
    }
    return (
        <button
            type="button"
            className="inspector-node-link"
            title={`Show ${label} in graph`}
            onClick={() => onNavigate(nodeId)}
        >
            {label}
        </button>
    );
}

function InspectorHeader({
    kicker,
    title,
    icon,
    onClose,
}: {
    kicker: string;
    title: string;
    icon?: React.ReactNode;
    onClose: () => void;
}) {
    return (
        <div className="node-inspector-panel-header">
            <div className="inspector-header">
                {icon}
                <div>
                    <div className="inspector-kicker">{kicker}</div>
                    <div className="inspector-title">{title}</div>
                </div>
            </div>
            <button
                type="button"
                className="node-inspector-close"
                aria-label="Clear selection"
                title="Clear selection"
                onClick={onClose}
            >
                ×
            </button>
        </div>
    );
}

function TableInspectorContent({
    node,
    onClose,
}: {
    node: Cytoscape.NodeDataDefinition;
    onClose: () => void;
}) {
    const primaryKeys = (node.indexes as string[]) ?? [];
    const storeClass = node.store_class ? String(node.store_class) : "TableStoreDB";

    return (
        <>
            <InspectorHeader kicker="Table" title={String(node.name)} onClose={onClose} />
            <div className="node-inspector-panel-body">
                <InspectorSection title="Summary">
                    <dl className="inspector-kv">
                        <dt>Type</dt>
                        <dd>{storeClass}</dd>
                        <dt>Size</dt>
                        <dd>{node.size != null ? String(node.size) : "—"}</dd>
                        {node.metaGroup ? (
                            <>
                                <dt>Group</dt>
                                <dd>{String(node.metaGroup)}</dd>
                            </>
                        ) : null}
                    </dl>
                </InspectorSection>

                <InspectorSection title="Primary keys (PK)" count={`${primaryKeys.length} keys`}>
                    <KeyChipList kind="pk" keys={primaryKeys} />
                </InspectorSection>
            </div>
        </>
    );
}

function TransformInspectorContent({
    node,
    graphNodesById,
    runStatus,
    onClose,
    onNavigateToNode,
}: {
    node: Cytoscape.NodeDataDefinition;
    graphNodesById: Map<string, Cytoscape.NodeDataDefinition>;
    runStatus?: string;
    onClose: () => void;
    onNavigateToNode?: (nodeId: string) => void;
}) {
    const tpk = getTransformPrimaryKeys(node);
    const inputs = ((node.inputs as string[]) ?? []).map((id) => ({
        id,
        node: graphNodesById.get(id),
    }));
    const outputs = ((node.outputs as string[]) ?? []).map((id) => ({
        id,
        node: graphNodesById.get(id),
    }));
    const labels = (node.labels as string[][] | undefined) ?? [];

    return (
        <>
            <InspectorHeader
                kicker="Transform Step"
                title={String(node.name)}
                icon={<span className="node-transform-f-icon">f</span>}
                onClose={onClose}
            />
            <div className="node-inspector-panel-body">
                <InspectorSection title="Summary">
                    <dl className="inspector-kv">
                        <dt>Type</dt>
                        <dd>{node.transform_type ? String(node.transform_type) : "TransformStep"}</dd>
                        <dt>ID</dt>
                        <dd>{String(node.name)}</dd>
                        {node.metaGroup ? (
                            <>
                                <dt>Group</dt>
                                <dd>{String(node.metaGroup)}</dd>
                            </>
                        ) : null}
                        {runStatus ? (
                            <>
                                <dt>Status</dt>
                                <dd>{runStatus}</dd>
                            </>
                        ) : null}
                    </dl>
                </InspectorSection>

                <InspectorSection title="Transform Primary Keys (TPK)" count={`${tpk.length} keys`}>
                    <KeyChipList kind="tpk" keys={tpk} />
                </InspectorSection>

                <InspectorSection
                    title="Inputs"
                    count={`${inputs.length} input${inputs.length === 1 ? "" : "s"}`}
                >
                    <table className="inspector-io-table">
                        <thead>
                            <tr>
                                <th>Source</th>
                                <th>Type</th>
                                <th>Keys</th>
                            </tr>
                        </thead>
                        <tbody>
                            {inputs.map(({ id, node: input }) => (
                                <tr key={id}>
                                    <td>
                                        <InspectorNodeLink
                                            nodeId={id}
                                            node={input}
                                            onNavigate={onNavigateToNode}
                                        />
                                    </td>
                                    <td>{input?.type === "table" ? "Table" : input?.type ?? "—"}</td>
                                    <td>
                                        <KeyChipList
                                            kind={input?.type === "table" ? "pk" : "tpk"}
                                            keys={nodeKeysForInspector(
                                                input,
                                                input?.type === "table" ? "pk" : "tpk",
                                            )}
                                        />
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </InspectorSection>

                <InspectorSection
                    title="Outputs"
                    count={`${outputs.length} output${outputs.length === 1 ? "" : "s"}`}
                >
                    <table className="inspector-io-table">
                        <thead>
                            <tr>
                                <th>Destination</th>
                                <th>Type</th>
                                <th>Keys</th>
                            </tr>
                        </thead>
                        <tbody>
                            {outputs.map(({ id, node: output }) => (
                                <tr key={id}>
                                    <td>
                                        <InspectorNodeLink
                                            nodeId={id}
                                            node={output}
                                            onNavigate={onNavigateToNode}
                                        />
                                    </td>
                                    <td>{output?.type === "table" ? "Table" : output?.type ?? "—"}</td>
                                    <td>
                                        <KeyChipList
                                            kind={output?.type === "table" ? "pk" : "tpk"}
                                            keys={nodeKeysForInspector(
                                                output,
                                                output?.type === "table" ? "pk" : "tpk",
                                            )}
                                        />
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </InspectorSection>

                {labels.length > 0 && (
                    <InspectorSection title="Metadata">
                        <dl className="inspector-kv">
                            {labels.map(([k, v]) => (
                                <React.Fragment key={`${k}-${v}`}>
                                    <dt>{k}</dt>
                                    <dd>{v}</dd>
                                </React.Fragment>
                            ))}
                        </dl>
                    </InspectorSection>
                )}
            </div>
        </>
    );
}

function GroupInspectorContent({
    node,
    graphNodesById,
    onClose,
    onNavigateToNode,
}: {
    node: Cytoscape.NodeDataDefinition;
    graphNodesById: Map<string, Cytoscape.NodeDataDefinition>;
    onClose: () => void;
    onNavigateToNode?: (nodeId: string) => void;
}) {
    const childCount = (node.child_count as number) ?? 0;
    const tpk = getTransformPrimaryKeys(node);
    const inputs = (node.inputs as string[]) ?? [];
    const outputs = (node.outputs as string[]) ?? [];

    return (
        <>
            <InspectorHeader kicker="Group Step" title={String(node.name)} onClose={onClose} />
            <div className="node-inspector-panel-body">
                <InspectorSection title="Summary">
                    <dl className="inspector-kv">
                        <dt>Steps</dt>
                        <dd>{childCount}</dd>
                        <dt>Type</dt>
                        <dd>{node.transform_type ? String(node.transform_type) : "Group"}</dd>
                    </dl>
                </InspectorSection>

                {tpk.length > 0 && (
                    <InspectorSection title="Transform Primary Keys (TPK)" count={`${tpk.length} keys`}>
                        <KeyChipList kind="tpk" keys={tpk} />
                    </InspectorSection>
                )}

                {(inputs.length > 0 || outputs.length > 0) && (
                    <InspectorSection title="Connections">
                        <dl className="inspector-kv">
                            {inputs.length > 0 && (
                                <>
                                    <dt>Inputs</dt>
                                    <dd className="inspector-link-list">
                                        {inputs.map((id, i) => (
                                            <React.Fragment key={id}>
                                                {i > 0 ? ", " : null}
                                                <InspectorNodeLink
                                                    nodeId={id}
                                                    node={graphNodesById.get(id)}
                                                    onNavigate={onNavigateToNode}
                                                />
                                            </React.Fragment>
                                        ))}
                                    </dd>
                                </>
                            )}
                            {outputs.length > 0 && (
                                <>
                                    <dt>Outputs</dt>
                                    <dd className="inspector-link-list">
                                        {outputs.map((id, i) => (
                                            <React.Fragment key={id}>
                                                {i > 0 ? ", " : null}
                                                <InspectorNodeLink
                                                    nodeId={id}
                                                    node={graphNodesById.get(id)}
                                                    onNavigate={onNavigateToNode}
                                                />
                                            </React.Fragment>
                                        ))}
                                    </dd>
                                </>
                            )}
                        </dl>
                    </InspectorSection>
                )}
            </div>
        </>
    );
}

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
};

function InspectorContent({
    inspector,
    graphNodesById,
    runStatusByStep,
    onClose,
    onNavigateToNode,
}: Pick<
    NodeInspectorPanelProps,
    "inspector" | "graphNodesById" | "runStatusByStep" | "onClose" | "onNavigateToNode"
>) {
    if (!inspector) return <InspectorEmptyState />;

    const node = inspector.data;
    const type = node.type as string;

    if (type === "table") {
        return <TableInspectorContent node={node} onClose={onClose} />;
    }
    if (type === "transform") {
        return (
            <TransformInspectorContent
                node={node}
                graphNodesById={graphNodesById}
                runStatus={runStatusByStep?.get(String(node.name))}
                onClose={onClose}
                onNavigateToNode={onNavigateToNode}
            />
        );
    }
    if (type === "group") {
        return (
            <GroupInspectorContent
                node={node}
                graphNodesById={graphNodesById}
                onClose={onClose}
                onNavigateToNode={onNavigateToNode}
            />
        );
    }
    return <InspectorEmptyState />;
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
                <InspectorContent
                    inspector={inspector}
                    graphNodesById={graphNodesById}
                    runStatusByStep={runStatusByStep}
                    onClose={onClose}
                    onNavigateToNode={onNavigateToNode}
                />
            </div>
        </aside>
    );
}
