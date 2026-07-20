import React from "react";
import { Card, Dropdown, Menu, Tooltip } from "antd";
import type { LabelGraphPayload, StageEdge, StageItem } from "../../../types/ops";
import type { EdgeHighlightLevel, LayoutEdge, LayoutNode } from "../utils/labelGraph";
import {
    edgePath,
    getEdgeHighlightLevel,
    isEdgeVisible,
    isSharedVisible,
    layoutLabelGraph,
    nodeToTopLevel,
    normalizeLabelGraphHierarchy,
    resolveLabelGraph,
    sharedBracketPath,
} from "../utils/labelGraph";
import "./PipelineLabelGraphOverview.css";

export type PipelineLabelGraphOverviewProps = {
    pipelineId: string;
    stages: StageItem[];
    stageEdges?: StageEdge[];
    labelGraph?: LabelGraphPayload;
    selectedLabel?: string | null;
    scopeHighlightLabel?: string | null;
    scopeHighlightAll?: boolean;
    allowClickSelect?: boolean;
    scopeMuteOutside?: boolean;
    mode?: "overview" | "compact";
    onLabelSelect?: (label: string) => void;
    onLabelClear?: () => void;
    onStageRun?: (label: string) => void;
};

function LabelGraphContextMenu({
    nodeId,
    onSelect,
    onRun,
    children,
}: {
    nodeId: string;
    onSelect?: (label: string) => void;
    onRun?: (label: string) => void;
    children: React.ReactElement;
}) {
    if (!onSelect && !onRun) {
        return children;
    }

    const menu = (
        <Menu>
            {onSelect && (
                <Menu.Item key="open" onClick={() => onSelect(nodeId)}>
                    Open graph
                </Menu.Item>
            )}
            {onRun && (
                <Menu.Item key="run" onClick={() => onRun(nodeId)}>
                    Run label
                </Menu.Item>
            )}
        </Menu>
    );

    return (
        <Dropdown overlay={menu} trigger={["contextMenu"]}>
            {children}
        </Dropdown>
    );
}

function LabelGraphNodeCard({
    layoutNode,
    selected,
    hovered,
    muted,
    onHover,
    onClickSelect,
    onContextSelect,
    onRun,
    tooltip,
}: {
    layoutNode: LayoutNode;
    selected: boolean;
    hovered: boolean;
    muted: boolean;
    onHover: (id: string | null) => void;
    onClickSelect?: (label: string) => void;
    onContextSelect?: (label: string) => void;
    onRun?: (label: string) => void;
    tooltip?: string;
}) {
    const id = layoutNode.nodeId;
    const label = layoutNode.label ?? id;

    const nodeBody = (
        <div
            className={[
                "label-node",
                selected ? "selected" : "",
                hovered ? "is-hovered" : "",
                muted ? "muted" : "",
            ]
                .filter(Boolean)
                .join(" ")}
            style={{
                left: layoutNode.x,
                top: layoutNode.y,
                width: layoutNode.width,
                height: layoutNode.height,
            }}
            onClick={onClickSelect ? () => onClickSelect(id) : undefined}
            onMouseEnter={() => onHover(id)}
            onMouseLeave={() => onHover(null)}
            role={onClickSelect ? "button" : undefined}
            tabIndex={onClickSelect ? 0 : undefined}
            onKeyDown={
                onClickSelect
                    ? (e) => {
                          if (e.key === "Enter" || e.key === " ") {
                              e.preventDefault();
                              onClickSelect(id);
                          }
                      }
                    : undefined
            }
        >
            <div className="label-node-title">{label}</div>
        </div>
    );

    // Tooltip on hover only — selected state uses visual highlight, not a persistent tooltip.
    const card =
        tooltip && hovered && !selected ? (
            <Tooltip title={<span style={{ whiteSpace: "pre-line" }}>{tooltip}</span>}>
                {nodeBody}
            </Tooltip>
        ) : (
            nodeBody
        );

    if (onContextSelect || onRun) {
        return (
            <LabelGraphContextMenu nodeId={id} onSelect={onContextSelect} onRun={onRun}>
                {card}
            </LabelGraphContextMenu>
        );
    }

    return card;
}

function renderEdge(edge: LayoutEdge, level: EdgeHighlightLevel, exact: boolean): React.ReactNode {
    const focused = level === "focused";
    return (
        <path
            key={edge.id}
            d={edgePath(edge.x1, edge.y1, edge.x2, edge.y2)}
            className={[
                "label-edge",
                exact ? "exact" : "",
                level === "focused" ? "focused" : "",
                level === "context" ? "context" : "",
                level === "muted" ? "muted" : "",
            ]
                .filter(Boolean)
                .join(" ")}
            markerEnd={focused || exact ? "url(#label-arrow-highlight)" : "url(#label-arrow)"}
        />
    );
}

export function PipelineLabelGraphOverview({
    pipelineId,
    stages,
    stageEdges,
    labelGraph: labelGraphProp,
    selectedLabel,
    scopeHighlightLabel = null,
    scopeHighlightAll = false,
    allowClickSelect = true,
    scopeMuteOutside = false,
    mode = "overview",
    onLabelSelect,
    onLabelClear,
    onStageRun,
}: PipelineLabelGraphOverviewProps) {
    void pipelineId;

    const [hoveredNodeId, setHoveredNodeId] = React.useState<string | null>(null);

    const payload = React.useMemo(
        () =>
            labelGraphProp
                ? normalizeLabelGraphHierarchy(labelGraphProp)
                : resolveLabelGraph({ stages, stage_edges: stageEdges }),
        [labelGraphProp, stages, stageEdges],
    );

    const layout = React.useMemo(() => layoutLabelGraph(payload, mode), [payload, mode]);

    const topMap = React.useMemo(() => nodeToTopLevel(payload), [payload]);
    const nodeById = React.useMemo(
        () => new Map(payload.nodes.map((n) => [n.id, n])),
        [payload.nodes],
    );

    const activeIds = React.useMemo(() => {
        const ids = new Set<string>();
        const add = (id?: string | null) => {
            if (!id) return;
            ids.add(id);
            const node = nodeById.get(id);
            if (node?.parent_id) ids.add(node.parent_id);
            node?.children_ids?.forEach((c) => ids.add(c));
            const top = topMap.get(id);
            if (top) ids.add(top);
        };
        add(selectedLabel ?? undefined);
        add(hoveredNodeId);
        return ids;
    }, [selectedLabel, hoveredNodeId, nodeById, topMap]);

    const clickLabelSelect = allowClickSelect ? onLabelSelect : undefined;

    const hasFocus = Boolean(selectedLabel) || activeIds.size > 0;

    const visibleExactEdges = layout.exactEdges.filter((e) =>
        isEdgeVisible(e, selectedLabel, hoveredNodeId),
    );

    const replacedEdgeIds = new Set(
        visibleExactEdges.map((edge) => edge.replacesEdgeId).filter(Boolean) as string[],
    );

    const visibleOrderEdges = layout.orderEdges.filter((edge) => !replacedEdgeIds.has(edge.id));

    const visibleShared = layout.sharedBrackets.filter((b) =>
        isSharedVisible(b, selectedLabel, hoveredNodeId),
    );

    if (!stages.length) return null;

    const containerNodes = layout.nodes.filter((n) => n.kind === "container");
    const interleavedContainers = layout.nodes.filter((n) => n.kind === "interleaved-group");
    const leafNodes = layout.nodes.filter((n) => n.kind === "node");

    const isInRunScope = (nodeId: string): boolean => {
        if (scopeHighlightAll) return true;
        if (!scopeHighlightLabel) return false;
        if (nodeId === scopeHighlightLabel) return true;
        const top = topMap.get(nodeId);
        if (top === scopeHighlightLabel) return true;
        const highlightNode = nodeById.get(scopeHighlightLabel);
        if (highlightNode?.children_ids?.includes(nodeId)) return true;
        if (highlightNode?.parent_id === nodeId) return true;
        return false;
    };

    const isRunScopeHighlighted = (nodeId: string): boolean => {
        if (scopeHighlightAll) return true;
        if (!scopeHighlightLabel) return false;
        return isInRunScope(nodeId);
    };

    const isNodeMuted = (nodeId: string): boolean => {
        if (scopeMuteOutside && scopeHighlightLabel && !isInRunScope(nodeId)) {
            return true;
        }
        if (!hasFocus) return false;
        return !activeIds.has(nodeId) && !activeIds.has(topMap.get(nodeId) ?? nodeId);
    };

    const nodeTooltip = (nodeId: string): string | undefined => {
        const node = nodeById.get(nodeId);
        if (!node) return undefined;
        const lines = [node.label, `Steps: ${node.step_count}`];
        const shared = payload.shared_relations.find(
            (r) => (r.a === nodeId || r.b === nodeId) && r.shared_count > 0,
        );
        if (shared) {
            const other = shared.a === nodeId ? shared.b : shared.a;
            lines.push(`Shared steps with ${other}: ${shared.shared_count}`);
        }
        return lines.join("\n");
    };

    const handleOverviewBackgroundClick = (event: React.MouseEvent<HTMLElement>) => {
        if (!selectedLabel || !onLabelClear) return;
        const target = event.target as HTMLElement;
        if (
            target.closest(
                ".label-node, .label-container, .label-interleaved-container, .label-shared-chip",
            )
        ) {
            return;
        }
        onLabelClear();
    };

    return (
        <Card
            bordered={mode === "overview"}
            className={`label-graph-card pipeline-label-overview pipeline-label-overview-${mode}`}
        >
            {mode === "overview" && (
                <div className="pipeline-label-overview-header">
                    <div className="pipeline-label-overview-title">Pipeline overview</div>
                    <div className="pipeline-label-overview-subtitle">
                        End-to-end pipeline at a glance
                    </div>
                </div>
            )}

            <div
                className="pipeline-label-graph-scroll"
                onClick={handleOverviewBackgroundClick}
            >
                <div
                    className="pipeline-label-graph-canvas"
                    style={{ width: layout.width, height: layout.height }}
                >
                    <svg
                        className="pipeline-label-graph-edges"
                        width={layout.width}
                        height={layout.height}
                        aria-hidden
                    >
                        <defs>
                            <marker
                                id="label-arrow"
                                viewBox="0 0 10 10"
                                refX="8"
                                refY="5"
                                markerWidth="6"
                                markerHeight="6"
                                orient="auto-start-reverse"
                            >
                                <path d="M 0 0 L 10 5 L 0 10 z" fill="#56677f" />
                            </marker>
                            <marker
                                id="label-arrow-highlight"
                                viewBox="0 0 10 10"
                                refX="8"
                                refY="5"
                                markerWidth="6"
                                markerHeight="6"
                                orient="auto-start-reverse"
                            >
                                <path d="M 0 0 L 10 5 L 0 10 z" fill="#1677ff" />
                            </marker>
                        </defs>

                        {visibleOrderEdges.map((edge) =>
                            renderEdge(
                                edge,
                                getEdgeHighlightLevel(edge, selectedLabel, activeIds),
                                false,
                            ),
                        )}

                        {visibleExactEdges.map((edge) =>
                            renderEdge(
                                edge,
                                getEdgeHighlightLevel(edge, selectedLabel, activeIds),
                                true,
                            ),
                        )}

                        {visibleShared.map((bracket) => (
                            <path
                                key={bracket.id}
                                className="label-shared-bracket"
                                d={sharedBracketPath(bracket)}
                            />
                        ))}
                    </svg>

                    <div className="pipeline-label-graph-nodes">
                        {containerNodes.map((cn) => {
                            const isScopeSelected = isRunScopeHighlighted(cn.nodeId);
                            const isSelected =
                                (clickLabelSelect && selectedLabel === cn.nodeId) || isScopeSelected;
                            const isHovered =
                                activeIds.has(cn.nodeId) ||
                                (cn.childIds?.some((c) => activeIds.has(c)) ?? false);
                            const muted = isNodeMuted(cn.nodeId);
                            const containerBody = (
                                <div
                                    className={[
                                        "label-container",
                                        isSelected ? "selected" : "",
                                        isHovered ? "is-hovered" : "",
                                        muted ? "muted" : "",
                                    ]
                                        .filter(Boolean)
                                        .join(" ")}
                                    style={{
                                        left: cn.x,
                                        top: cn.y,
                                        width: cn.width,
                                        height: cn.height,
                                    }}
                                    onMouseEnter={() => setHoveredNodeId(cn.nodeId)}
                                    onMouseLeave={() => setHoveredNodeId(null)}
                                    onClick={
                                        clickLabelSelect
                                            ? () => clickLabelSelect(cn.nodeId)
                                            : undefined
                                    }
                                    role={clickLabelSelect ? "button" : undefined}
                                    tabIndex={clickLabelSelect ? 0 : undefined}
                                    onKeyDown={
                                        clickLabelSelect
                                            ? (e) => {
                                                  if (e.key === "Enter" || e.key === " ") {
                                                      e.preventDefault();
                                                      clickLabelSelect(cn.nodeId);
                                                  }
                                              }
                                            : undefined
                                    }
                                >
                                    <div className="label-container-title">{cn.label}</div>
                                </div>
                            );
                            return (
                                <LabelGraphContextMenu
                                    key={cn.id}
                                    nodeId={cn.nodeId}
                                    onSelect={onLabelSelect}
                                    onRun={onStageRun}
                                >
                                    {containerBody}
                                </LabelGraphContextMenu>
                            );
                        })}

                        {interleavedContainers.map((ic) => {
                            const isSelected =
                                selectedLabel === ic.nodeId ||
                                ic.interleavedLabelIds?.includes(selectedLabel ?? "") === true;
                            const isHovered =
                                activeIds.has(ic.nodeId) ||
                                (ic.interleavedLabelIds?.some((c) => activeIds.has(c)) ?? false);
                            return (
                                <div
                                    key={ic.id}
                                    className={[
                                        "label-interleaved-container",
                                        isSelected ? "selected" : "",
                                        isHovered ? "is-hovered" : "",
                                    ]
                                        .filter(Boolean)
                                        .join(" ")}
                                    style={{
                                        left: ic.x,
                                        top: ic.y,
                                        width: ic.width,
                                        height: ic.height,
                                    }}
                                    onMouseEnter={() => setHoveredNodeId(ic.nodeId)}
                                    onMouseLeave={() => setHoveredNodeId(null)}
                                >
                                    <div className="label-interleaved-title">{ic.label}</div>
                                </div>
                            );
                        })}

                        {leafNodes.map((ln) => {
                            const parentInterleaved = interleavedContainers.find((ic) =>
                                ic.interleavedLabelIds?.includes(ln.nodeId),
                            );
                            const muted =
                                isNodeMuted(ln.nodeId) ||
                                (parentInterleaved
                                    ? isNodeMuted(parentInterleaved.nodeId)
                                    : false);
                            const isHovered = hoveredNodeId === ln.nodeId;
                            const isScopeSelected = isRunScopeHighlighted(ln.nodeId);
                            const isUserSelected =
                                clickLabelSelect && selectedLabel === ln.nodeId;
                            return (
                                <LabelGraphNodeCard
                                    key={ln.id}
                                    layoutNode={ln}
                                    selected={isUserSelected || isScopeSelected}
                                    hovered={isHovered}
                                    muted={muted && !isScopeSelected && selectedLabel !== ln.nodeId}
                                    onHover={setHoveredNodeId}
                                    onClickSelect={clickLabelSelect}
                                    onContextSelect={onLabelSelect}
                                    onRun={onStageRun}
                                    tooltip={nodeTooltip(ln.nodeId)}
                                />
                            );
                        })}

                        {visibleShared.map((bracket) => (
                            <div
                                key={bracket.id}
                                className="label-shared-chip"
                                style={{
                                    left: bracket.x + bracket.width / 2,
                                    top: bracket.y + 20,
                                }}
                            >
                                {bracket.label}
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </Card>
    );
}
