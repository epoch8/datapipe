import React from "react";
import { Tag } from "antd";
import { stepperCurrentIndex } from "./StageStepper";

export type StageItem = { stage: string; status: string; steps: unknown[] };
export type StageEdge = { from: string; to: string; count?: number };

const NODE_WIDTH = 128;
const NODE_GAP = 40;
const ROW_Y = 40;
const SVG_TOP = 8;

function statusTagColor(status: string): string | undefined {
    if (status === "completed") return "success";
    if (status === "failed") return "error";
    if (status === "running") return "processing";
    return undefined;
}

function stepIcon(index: number, current: number, status: string): React.ReactNode {
    if (status === "failed") return <span className="stage-flow-icon stage-flow-icon-error">!</span>;
    if (index < current) return <span className="stage-flow-icon stage-flow-icon-done">✓</span>;
    if (index === current && status !== "completed") {
        return <span className="stage-flow-icon stage-flow-icon-active">{index + 1}</span>;
    }
    return <span className="stage-flow-icon stage-flow-icon-wait">{index + 1}</span>;
}

function edgePath(
    fromIndex: number,
    toIndex: number,
    edgeIndex: number,
    edgeCount: number,
): string {
    const x1 = fromIndex * (NODE_WIDTH + NODE_GAP) + NODE_WIDTH / 2;
    const x2 = toIndex * (NODE_WIDTH + NODE_GAP) + NODE_WIDTH / 2;
    const y = ROW_Y + SVG_TOP;
    const span = Math.abs(toIndex - fromIndex);
    const lane = edgeIndex - (edgeCount - 1) / 2;
    const lift = 16 + span * 10 + Math.abs(lane) * 8;

    if (toIndex > fromIndex) {
        const controlY = y - lift;
        return `M ${x1} ${y} C ${x1} ${controlY}, ${x2} ${controlY}, ${x2} ${y}`;
    }

    const controlY = y + lift + 24;
    return `M ${x1} ${y} C ${x1} ${controlY}, ${x2} ${controlY}, ${x2} ${y}`;
}

export function StageFlowDiagram({
    stages,
    edges,
    onStageSelect,
}: {
    stages: StageItem[];
    edges?: StageEdge[];
    onStageSelect?: (stage: string) => void;
}) {
    if (!stages.length) return null;

    const names = stages.map((s) => s.stage);
    const current = stepperCurrentIndex(stages);
    const width = stages.length * NODE_WIDTH + Math.max(0, stages.length - 1) * NODE_GAP;
    const edgeList = edges?.length
        ? edges
        : names.slice(0, -1).map((stage, index) => ({
              from: stage,
              to: names[index + 1],
              count: 1,
          }));

    const groupedEdges = new Map<string, StageEdge[]>();
    for (const edge of edgeList) {
        const key = `${edge.from}=>${edge.to}`;
        const bucket = groupedEdges.get(key) || [];
        bucket.push(edge);
        groupedEdges.set(key, bucket);
    }

    const expandedEdges: { edge: StageEdge; laneIndex: number; laneCount: number }[] = [];
    groupedEdges.forEach((group) => {
        const edge = group[0];
        const laneCount = edge.count || 1;
        for (let laneIndex = 0; laneIndex < laneCount; laneIndex += 1) {
            expandedEdges.push({ edge, laneIndex, laneCount });
        }
    });

    const svgHeight = 112;

    return (
        <div className="stage-flow" style={{ minWidth: width }}>
            <svg className="stage-flow-svg" width={width} height={svgHeight} aria-hidden>
                <defs>
                    <marker
                        id="stage-flow-arrow"
                        viewBox="0 0 10 10"
                        refX="8"
                        refY="5"
                        markerWidth="6"
                        markerHeight="6"
                        orient="auto-start-reverse"
                    >
                        <path d="M 0 0 L 10 5 L 0 10 z" fill="#91caff" />
                    </marker>
                </defs>
                {expandedEdges.map(({ edge, laneIndex, laneCount }) => {
                    const fromIndex = names.indexOf(edge.from);
                    const toIndex = names.indexOf(edge.to);
                    if (fromIndex < 0 || toIndex < 0 || fromIndex === toIndex) return null;
                    return (
                        <path
                            key={`${edge.from}-${edge.to}-${laneIndex}`}
                            d={edgePath(fromIndex, toIndex, laneIndex, laneCount)}
                            className="stage-flow-edge"
                            markerEnd="url(#stage-flow-arrow)"
                        />
                    );
                })}
            </svg>
            <div className="stage-flow-nodes" style={{ width }}>
                {stages.map((stage, index) => (
                    <div key={stage.stage} className="stage-flow-node" style={{ width: NODE_WIDTH }}>
                        {stepIcon(index, current, stage.status)}
                        <div className="stage-flow-title">
                            {onStageSelect ? (
                                <button
                                    type="button"
                                    className="stage-stepper-title-btn"
                                    onClick={() => onStageSelect(stage.stage)}
                                >
                                    {stage.stage}
                                </button>
                            ) : (
                                stage.stage
                            )}
                        </div>
                        <Tag color={statusTagColor(stage.status)} className="stage-flow-tag">
                            {stage.status}
                        </Tag>
                    </div>
                ))}
            </div>
        </div>
    );
}
