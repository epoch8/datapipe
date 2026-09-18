import React from "react";
import { Spin } from "antd";
import { useNavigate, useSearchParams } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../api/client";
import { ApiErrorAlert } from "../../components/ApiErrorAlert";
import { usePipelineId } from "../../hooks/usePipelineId";
import type { PipelineDetail } from "../../types/ops";
import { RunStepsDropdown } from "./components/RunStepsDropdown";
import { PageHeader } from "./shared";
import { PipelineOverviewGraphCard } from "./pipeline/PipelineOverviewGraphCard";
import { prependRecentRun } from "./utils/recentRuns";

/**
 * General / Overview page: label graph for the single cloud pipeline.
 */
export function Overview() {
    const navigate = useNavigate();
    const [searchParams] = useSearchParams();
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [detail, setDetail] = React.useState<PipelineDetail | null>(null);
    const [error, setError] = React.useState<unknown>(null);
    const labelKeyParam = searchParams.get("label_key");

    const load = React.useCallback(() => {
        opsApi
            .getPipeline(labelKeyParam ? { label_key: labelKeyParam } : undefined)
            .then(setDetail)
            .catch((e) => setError(e));
    }, [labelKeyParam]);

    React.useEffect(() => {
        load();
        const timer = setInterval(load, getRefreshIntervalMs());
        return () => clearInterval(timer);
    }, [load]);

    const startRun = (labels: [string, string][]) => {
        opsApi
            .startRun(labels)
            .then((started) => {
                setDetail((current) =>
                    current
                        ? {
                              ...current,
                              recent_runs: prependRecentRun(current.recent_runs ?? [], started),
                          }
                        : current,
                );
                navigate(`/runs/${started.run_id}`);
            })
            .catch((e) => setError(e));
    };

    if (error) return <ApiErrorAlert error={error} />;
    if (pidLoading || !detail) return <Spin />;

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[{ label: "General" }]}
                title="General"
                onRefresh={load}
                extra={
                    pipelineId ? (
                        <RunStepsDropdown
                            pipelineId={pipelineId}
                            stages={detail.stages}
                            availableLabelKeys={detail.available_label_keys}
                            labelGraph={detail.label_graph}
                            defaultLabelKey={labelKeyParam ?? detail.label_graph?.label_key}
                            onStart={startRun}
                        />
                    ) : undefined
                }
            />

            <PipelineOverviewGraphCard
                detail={detail}
                onLabelSelect={(label, labelKey) => {
                    const params = new URLSearchParams();
                    if (labelKey && labelKey !== "stage") params.set("label_key", labelKey);
                    params.set("stage", label);
                    navigate(`/graph?${params.toString()}`);
                }}
                onStageRun={startRun}
            />
        </div>
    );
}
