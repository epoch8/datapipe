import React from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import type { PipelineDetail } from "../../../types/ops";
import { PipelineLabelGraphOverview } from "../components/PipelineLabelGraphOverview";
import {
    DEFAULT_LABEL_KEY,
    graphHref,
    normalizeLabelKey,
} from "../utils/labelKey";

type Props = {
    pipelineId: string;
    detail: PipelineDetail;
    onStageRun?: (labels: [string, string][]) => void;
    onLabelKeyChange?: (labelKey: string) => void;
};

export function PipelineOverviewGraphCard({
    pipelineId,
    detail,
    onStageRun,
    onLabelKeyChange,
}: Props) {
    const navigate = useNavigate();
    const [searchParams, setSearchParams] = useSearchParams();
    const availableKeys = detail.available_label_keys ?? [];
    const labelKey = normalizeLabelKey(
        searchParams.get("label_key") ?? detail.label_graph?.label_key,
        availableKeys,
    );

    const setLabelKey = (nextKey: string) => {
        setSearchParams(
            (prev) => {
                const next = new URLSearchParams(prev);
                if (nextKey === DEFAULT_LABEL_KEY) next.delete("label_key");
                else next.set("label_key", nextKey);
                return next;
            },
            { replace: true },
        );
        onLabelKeyChange?.(nextKey);
    };

    return (
        <PipelineLabelGraphOverview
            pipelineId={pipelineId}
            stages={detail.stages}
            stageEdges={detail.stage_edges}
            labelGraph={detail.label_graph}
            availableLabelKeys={availableKeys}
            labelKey={labelKey}
            mode="overview"
            onLabelKeyChange={setLabelKey}
            onLabelSelect={(label) => navigate(graphHref(label, labelKey))}
            onStageRun={
                onStageRun ? (label) => onStageRun([[labelKey, label]]) : undefined
            }
        />
    );
}
