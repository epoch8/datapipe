import React from "react";
import { Button } from "antd";
import { Link } from "react-router-dom";
import Cytoscape from "cytoscape";
import { fetchTableSize, fetchTransformMetaSize } from "../../api/graph";
import { TableDataPanel } from "../ops/components/TableDataPanel";
import { TableSizeControl } from "../ops/shared/TableSizeControl";
import { formatNodeLabels, getTransformPrimaryKeys } from "./nodeKeyChips";
import type { PipeTable, TableColumn } from "../../types";

export type GraphNodeData = Record<string, unknown>;

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

function KeyChipList({ kind, keys }: { kind: "pk" | "tpk" | "label"; keys: string[] }) {
    if (!keys.length) {
        return <span style={{ color: "#667085", fontSize: 12 }}>No {kind === "label" ? "labels" : "keys"}</span>;
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

function TableSchemaSection({
    schema,
    primaryKeys,
}: {
    schema: TableColumn[];
    primaryKeys: string[];
}) {
    if (!schema.length) {
        return null;
    }

    const pkSet = new Set(primaryKeys);

    return (
        <InspectorSection title="Columns" count={`${schema.length} columns`}>
            <table className="inspector-io-table inspector-schema-table">
                <thead>
                    <tr>
                        <th>Column</th>
                        <th>Type</th>
                    </tr>
                </thead>
                <tbody>
                    {schema.map((column) => {
                        const isPk = pkSet.has(column.name);
                        return (
                            <tr key={column.name} className={isPk ? "inspector-schema-pk-row" : undefined}>
                                <td>
                                    <span className="inspector-schema-column-name">
                                        {column.name}
                                        {isPk ? <span className="node-key-chip pk">PK</span> : null}
                                    </span>
                                </td>
                                <td>
                                    <code className="inspector-schema-type">{column.type}</code>
                                </td>
                            </tr>
                        );
                    })}
                </tbody>
            </table>
        </InspectorSection>
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

function IoNodeLink({
    nodeId,
    node,
    pipelineId,
    onNavigate,
}: {
    nodeId: string;
    node?: Cytoscape.NodeDataDefinition;
    pipelineId?: string;
    onNavigate?: (nodeId: string) => void;
}) {
    const label = node?.name ? String(node.name) : nodeId;
    if (onNavigate && isInspectableGraphNode(node)) {
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
    if (pipelineId && node?.type === "table") {
        return (
            <Link to={`/pipelines/${encodeURIComponent(pipelineId)}/tables/${encodeURIComponent(nodeId)}`}>
                {label}
            </Link>
        );
    }
    if (pipelineId && node?.type === "transform") {
        return (
            <Link to={`/pipelines/${encodeURIComponent(pipelineId)}/transforms/${encodeURIComponent(nodeId)}`}>
                {label}
            </Link>
        );
    }
    return <>{label}</>;
}

export function GraphNodeDetailHeader({
    kicker,
    title,
    icon,
    onClose,
    onOpenDetails,
}: {
    kicker: string;
    title: string;
    icon?: React.ReactNode;
    onClose?: () => void;
    onOpenDetails?: () => void;
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
            <div className="inspector-header-actions">
                {onOpenDetails ? (
                    <Button size="small" type="primary" onClick={onOpenDetails}>
                        Open details
                    </Button>
                ) : null}
                {onClose ? (
                    <button
                        type="button"
                        className="node-inspector-close"
                        aria-label="Clear selection"
                        title="Clear selection"
                        onClick={onClose}
                    >
                        ×
                    </button>
                ) : null}
            </div>
        </div>
    );
}

function TableDataSection({
    table,
    knownRowCount,
    pipelineId,
    initialColumnFilter,
}: {
    table: PipeTable;
    knownRowCount: number | null;
    pipelineId?: string;
    initialColumnFilter?: { column: string; value: string };
}) {
    return (
        <InspectorSection title="Table data">
            <TableDataPanel
                table={table}
                knownRowCount={knownRowCount}
                pipelineId={pipelineId}
                initialColumnFilter={initialColumnFilter}
            />
        </InspectorSection>
    );
}

export function TableNodeDetailBody({
    node,
    onClose,
    onOpenDetails,
    showTableData = true,
    showHeader = true,
    pipelineId,
    initialColumnFilter,
}: {
    node: GraphNodeData;
    pipelineId?: string;
    onClose?: () => void;
    onOpenDetails?: () => void;
    showTableData?: boolean;
    showHeader?: boolean;
    initialColumnFilter?: { column: string; value: string };
}) {
    const primaryKeys = (node.indexes as string[]) ?? [];
    const storeClass = node.store_class ? String(node.store_class) : "TableStoreDB";
    const tableName = String(node.name);
    const schema = ((node.schema as TableColumn[] | undefined) ?? []).filter(
        (column) => column?.name && column?.type,
    );
    const [tableSize, setTableSize] = React.useState<number | null>(
        node.size != null ? Number(node.size) : null,
    );
    const [countingSize, setCountingSize] = React.useState(false);
    const [sizeError, setSizeError] = React.useState<string | null>(null);

    const countSize = () => {
        setCountingSize(true);
        setSizeError(null);
        fetchTableSize(tableName)
            .then(setTableSize)
            .catch((e) => setSizeError(String(e)))
            .finally(() => setCountingSize(false));
    };

    const pipeTable: PipeTable = {
        id: tableName,
        indexes: primaryKeys,
        size: tableSize,
        store_class: storeClass,
        schema,
        type: "table",
    };

    return (
        <>
            {showHeader ? (
                <GraphNodeDetailHeader
                    kicker="Table"
                    title={tableName}
                    onClose={onClose}
                    onOpenDetails={onOpenDetails}
                />
            ) : null}
            <div className="node-inspector-panel-body">
                <InspectorSection title="Summary">
                    <dl className="inspector-kv">
                        <dt>Type</dt>
                        <dd>{storeClass}</dd>
                        <dt>Size</dt>
                        <dd>
                            <TableSizeControl
                                size={tableSize}
                                loading={countingSize}
                                onCount={countSize}
                            />
                        </dd>
                        {sizeError ? (
                            <>
                                <dt />
                                <dd style={{ color: "#ff4d4f" }}>{sizeError}</dd>
                            </>
                        ) : null}
                        {node.metaGroup ? (
                            <>
                                <dt>Group</dt>
                                <dd>{String(node.metaGroup)}</dd>
                            </>
                        ) : null}
                    </dl>
                </InspectorSection>

                <TableSchemaSection schema={schema} primaryKeys={primaryKeys} />

                <InspectorSection title="Primary keys (PK)" count={`${primaryKeys.length} keys`}>
                    <KeyChipList kind="pk" keys={primaryKeys} />
                </InspectorSection>

                {showTableData && (
                    <TableDataSection
                        table={pipeTable}
                        knownRowCount={tableSize}
                        pipelineId={pipelineId}
                        initialColumnFilter={initialColumnFilter}
                    />
                )}
            </div>
        </>
    );
}

export function TransformNodeDetailBody({
    node,
    graphNodesById,
    runStatus,
    pipelineId,
    onClose,
    onOpenDetails,
    onNavigateToNode,
    showMetaTable = true,
    showHeader = true,
}: {
    node: GraphNodeData;
    graphNodesById?: Map<string, Cytoscape.NodeDataDefinition>;
    runStatus?: string;
    pipelineId?: string;
    onClose?: () => void;
    onOpenDetails?: () => void;
    onNavigateToNode?: (nodeId: string) => void;
    showMetaTable?: boolean;
    showHeader?: boolean;
}) {
    const transformName = String(node.name);
    const tpk = getTransformPrimaryKeys(node as Cytoscape.NodeDataDefinition);
    const inputs = ((node.inputs as string[]) ?? []).map((id) => ({
        id,
        node: graphNodesById?.get(id),
    }));
    const outputs = ((node.outputs as string[]) ?? []).map((id) => ({
        id,
        node: graphNodesById?.get(id),
    }));
    const labels = formatNodeLabels((node.labels as string[][]) ?? []);
    const hasTransformMeta =
        node.has_transform_meta === true ||
        node.total_idx_count != null ||
        node.changed_idx_count != null;

    const [metaSize, setMetaSize] = React.useState<number | null>(null);
    const [countingMeta, setCountingMeta] = React.useState(false);
    const [metaSizeError, setMetaSizeError] = React.useState<string | null>(null);

    const countMetaSize = () => {
        setCountingMeta(true);
        setMetaSizeError(null);
        fetchTransformMetaSize(transformName)
            .then(setMetaSize)
            .catch((e) => setMetaSizeError(String(e)))
            .finally(() => setCountingMeta(false));
    };

    const metaTable: PipeTable = {
        id: transformName,
        indexes: (node.indexes as string[]) ?? [],
        size: metaSize,
        store_class: node.transform_type ? String(node.transform_type) : "transform",
        type: "transform",
    };

    return (
        <>
            {showHeader ? (
                <GraphNodeDetailHeader
                    kicker="Transform Step"
                    title={transformName}
                    icon={<span className="node-transform-f-icon">f</span>}
                    onClose={onClose}
                    onOpenDetails={onOpenDetails}
                />
            ) : null}
            <div className="node-inspector-panel-body">
                <InspectorSection title="Summary">
                    <dl className="inspector-kv">
                        <dt>Type</dt>
                        <dd>{node.transform_type ? String(node.transform_type) : "TransformStep"}</dd>
                        <dt>ID</dt>
                        <dd>{transformName}</dd>
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

                {labels.length > 0 && (
                    <InspectorSection title="Labels" count={`${labels.length} labels`}>
                        <KeyChipList kind="label" keys={labels} />
                    </InspectorSection>
                )}

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
                                        <IoNodeLink
                                            nodeId={id}
                                            node={input}
                                            pipelineId={pipelineId}
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
                                        <IoNodeLink
                                            nodeId={id}
                                            node={output}
                                            pipelineId={pipelineId}
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

                {showMetaTable && hasTransformMeta && (
                    <InspectorSection title="Transform meta table">
                        <div style={{ marginBottom: 12 }}>
                            <TableSizeControl
                                size={metaSize}
                                loading={countingMeta}
                                onCount={countMetaSize}
                            />
                            {metaSizeError ? (
                                <div style={{ color: "#ff4d4f", marginTop: 8 }}>{metaSizeError}</div>
                            ) : null}
                        </div>
                        <TableDataPanel table={metaTable} knownRowCount={metaSize} hideRunStep />
                    </InspectorSection>
                )}
            </div>
        </>
    );
}

export function GroupNodeDetailBody({
    node,
    graphNodesById,
    pipelineId,
    onClose,
    onOpenDetails,
    onNavigateToNode,
    subSteps,
    showHeader = true,
}: {
    node: GraphNodeData;
    graphNodesById?: Map<string, Cytoscape.NodeDataDefinition>;
    pipelineId?: string;
    onClose?: () => void;
    onOpenDetails?: () => void;
    onNavigateToNode?: (nodeId: string) => void;
    subSteps?: { name: string; type: string; transform_type?: string }[];
    showHeader?: boolean;
}) {
    const childCount = (node.child_count as number) ?? subSteps?.length ?? 0;
    const tpk = getTransformPrimaryKeys(node as Cytoscape.NodeDataDefinition);
    const inputs = (node.inputs as string[]) ?? [];
    const outputs = (node.outputs as string[]) ?? [];
    const labels = formatNodeLabels((node.labels as string[][]) ?? []);

    return (
        <>
            {showHeader ? (
                <GraphNodeDetailHeader
                    kicker="Group Step"
                    title={String(node.name)}
                    onClose={onClose}
                    onOpenDetails={onOpenDetails}
                />
            ) : null}
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

                {labels.length > 0 && (
                    <InspectorSection title="Labels" count={`${labels.length} labels`}>
                        <KeyChipList kind="label" keys={labels} />
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
                                                <IoNodeLink
                                                    nodeId={id}
                                                    node={graphNodesById?.get(id)}
                                                    pipelineId={pipelineId}
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
                                                <IoNodeLink
                                                    nodeId={id}
                                                    node={graphNodesById?.get(id)}
                                                    pipelineId={pipelineId}
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

                {subSteps && subSteps.length > 0 && (
                    <InspectorSection title="Sub-steps" count={`${subSteps.length} steps`}>
                        <ul className="inspector-substep-list">
                            {subSteps.map((item) => (
                                <li key={item.name}>
                                    {item.type === "transform" && pipelineId ? (
                                        <Link
                                            to={`/pipelines/${encodeURIComponent(pipelineId)}/transforms/${encodeURIComponent(item.name)}`}
                                        >
                                            {item.name}
                                        </Link>
                                    ) : (
                                        <span>{item.name}</span>
                                    )}
                                    {item.transform_type ? (
                                        <span className="node-key-chip label" style={{ marginLeft: 8 }}>
                                            {item.transform_type}
                                        </span>
                                    ) : null}
                                </li>
                            ))}
                        </ul>
                    </InspectorSection>
                )}
            </div>
        </>
    );
}

export function GraphNodeDetailBody({
    node,
    graphNodesById,
    runStatus,
    pipelineId,
    onClose,
    onOpenDetails,
    onNavigateToNode,
    subSteps,
    showTableData = true,
    showMetaTable = true,
    showHeader = true,
    initialColumnFilter,
}: {
    node: GraphNodeData;
    graphNodesById?: Map<string, Cytoscape.NodeDataDefinition>;
    runStatus?: string;
    pipelineId?: string;
    onClose?: () => void;
    onOpenDetails?: () => void;
    onNavigateToNode?: (nodeId: string) => void;
    subSteps?: { name: string; type: string; transform_type?: string }[];
    showTableData?: boolean;
    showMetaTable?: boolean;
    showHeader?: boolean;
    initialColumnFilter?: { column: string; value: string };
}) {
    const type = node.type as string;
    if (type === "table") {
        return (
            <TableNodeDetailBody
                node={node}
                pipelineId={pipelineId}
                onClose={onClose}
                onOpenDetails={onOpenDetails}
                showTableData={showTableData}
                showHeader={showHeader}
                initialColumnFilter={initialColumnFilter}
            />
        );
    }
    if (type === "transform") {
        return (
            <TransformNodeDetailBody
                node={node}
                graphNodesById={graphNodesById}
                runStatus={runStatus}
                pipelineId={pipelineId}
                onClose={onClose}
                onOpenDetails={onOpenDetails}
                onNavigateToNode={onNavigateToNode}
                showMetaTable={showMetaTable}
                showHeader={showHeader}
            />
        );
    }
    if (type === "group") {
        return (
            <GroupNodeDetailBody
                node={node}
                graphNodesById={graphNodesById}
                pipelineId={pipelineId}
                onClose={onClose}
                onOpenDetails={onOpenDetails}
                onNavigateToNode={onNavigateToNode}
                subSteps={subSteps}
                showHeader={showHeader}
            />
        );
    }
    return null;
}
