import React from "react";
import { Alert, Card, Spin, Tag } from "antd";
import { useNavigate, useParams } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../api/ops";
import { ApiErrorAlert } from "../../components/ApiErrorAlert";
import type { PipelineDetail as PipelineDetailType } from "../../types/ops";
import { PluginSection } from "./components/PluginSection";
import { RecentRunsList } from "./components/RecentRunsList";
import { PipelineLabelGraphOverview } from "./components/PipelineLabelGraphOverview";
import { PageHeader } from "./shared";
import { prependRecentRun } from "./utils/recentRuns";
import { RunStepsDropdown } from "./components/RunStepsDropdown";

type PipelineDetailProps = {
    pipelineId?: string;
    embedded?: boolean;
};

export function PipelineDetail({
    pipelineId: pipelineIdProp,
    embedded = false,
}: PipelineDetailProps = {}) {
    const { id: routeId = "" } = useParams();
    const id = pipelineIdProp ?? routeId;
    const navigate = useNavigate();
    const [detail, setDetail] = React.useState<PipelineDetailType | null>(null);
    const [error, setError] = React.useState<unknown>(null);

    const load = React.useCallback(() => {
        if (!id) return;
        opsApi
            .getPipeline(id)
            .then(setDetail)
            .catch((e) => setError(e));
    }, [id]);

    React.useEffect(() => {
        load();
        const timer = setInterval(load, getRefreshIntervalMs());
        return () => clearInterval(timer);
    }, [load]);

    const runStage = (labels: [string, string][]) => {
        opsApi
            .startRun(labels)
            .then((started) => {
                setDetail((current) =>
                    current
                        ? { ...current, recent_runs: prependRecentRun(current.recent_runs, started) }
                        : current,
                );
                navigate(`/runs/${started.run_id}`);
            })
            .catch((e) => setError(e));
    };

    if (!id) return <Alert type="error" message="Pipeline id is required" />;
    if (error) return <ApiErrorAlert error={error} />;
    if (!detail) return <Spin />;

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[{ label: "Overview" }]}
                title={detail.display_name}
                onRefresh={load}
                extra={
                    detail.agent_mode ? (
                        <RunStepsDropdown stages={detail.stages} onStart={runStage} />
                    ) : undefined
                }
            />
            <div style={{ marginBottom: 16 }}>
                {detail.task_type && <Tag>{detail.task_type}</Tag>}
                <Tag color={detail.health === "failed" ? "red" : "green"}>{detail.health}</Tag>
            </div>
            <PipelineLabelGraphOverview
                pipelineId={id}
                stages={detail.stages}
                stageEdges={detail.stage_edges}
                labelGraph={detail.label_graph}
                mode="overview"
                onLabelSelect={(label) =>
                    navigate(`/graph?stage=${encodeURIComponent(label)}`)
                }
                onStageRun={
                    detail.agent_mode
                        ? (label) => runStage([["stage", label]])
                        : undefined
                }
            />
            {detail.last_error && (
                <Card title="Error" style={{ marginBottom: 16 }}>
                    <span style={{ color: "#ff4d4f" }}>{detail.last_error}</span>
                </Card>
            )}
            <PluginSection enrichments={detail.enrichments} />
            <Card title="Recent runs" style={{ marginTop: 16 }}>
                <RecentRunsList runs={detail.recent_runs} />
            </Card>
        </div>
    );
}
