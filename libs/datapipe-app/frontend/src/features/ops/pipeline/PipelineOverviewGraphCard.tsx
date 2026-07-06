import React from "react";
import { useNavigate } from "react-router-dom";
import type { PipelineDetail } from "../../../types/ops";
import { PipelineLabelGraphOverview } from "../components/PipelineLabelGraphOverview";

type Props = {
    pipelineId: string;
    detail: PipelineDetail;
    onStageRun?: (labels: [string, string][]) => void;
};

export function PipelineOverviewGraphCard({ pipelineId, detail, onStageRun }: Props) {
    const navigate = useNavigate();

    return (
        <PipelineLabelGraphOverview
            pipelineId={pipelineId}
            stages={detail.stages}
            stageEdges={detail.stage_edges}
            labelGraph={detail.label_graph}
            mode="overview"
            onLabelSelect={(label) => navigate(`/graph?stage=${encodeURIComponent(label)}`)}
            onStageRun={
                detail.agent_mode && onStageRun
                    ? (label) => onStageRun([["stage", label]])
                    : undefined
            }
        />
    );
}
