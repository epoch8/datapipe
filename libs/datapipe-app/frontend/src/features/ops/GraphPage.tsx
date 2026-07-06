import React from "react";
import { Alert, Card, Spin } from "antd";
import { useNavigate, useSearchParams } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../api/ops";
import type { Capabilities, PipelineDetail, RecentRunSummary } from "../../types/ops";
import { PipelineGraphAgentOnly } from "./components/PipelineGraph";
import { PipelineLabelGraphOverview } from "./components/PipelineLabelGraphOverview";
import { RecentRunsList } from "./components/RecentRunsList";
import { PageHeader } from "./shared";
import { workflowIconSvg } from "../cy/nodeIcons";
import { prependRecentRun } from "./utils/recentRuns";
import { RunStepsDropdown } from "./components/RunStepsDropdown";

export function GraphPage() {
    const [searchParams] = useSearchParams();
    const navigate = useNavigate();
    const stage = searchParams.get("stage");
    const [capabilities, setCapabilities] = React.useState<Capabilities | null>(null);
    const [detail, setDetail] = React.useState<PipelineDetail | null>(null);
    const [stageRuns, setStageRuns] = React.useState<RecentRunSummary[]>([]);
    const [error, setError] = React.useState<string | null>(null);
    const [graphRefreshToken, setGraphRefreshToken] = React.useState(0);

    const pipelineId = capabilities?.pipeline_id;
    const agentMode = capabilities?.mode === "agent";

    const loadCapabilities = React.useCallback(() => {
        opsApi.getCapabilities().then(setCapabilities).catch((e) => setError(String(e)));
    }, []);

    const loadDetail = React.useCallback(() => {
        if (!pipelineId) return;
        opsApi
            .getPipeline(pipelineId)
            .then(setDetail)
            .catch((e) => setError(String(e)));
    }, [pipelineId]);

    React.useEffect(() => {
        loadCapabilities();
    }, [loadCapabilities]);

    React.useEffect(() => {
        loadDetail();
    }, [loadDetail]);

    const loadStageRuns = React.useCallback(() => {
        if (!stage || !pipelineId) return;
        opsApi
            .resolveStageRecentRuns(pipelineId, stage)
            .then((response) => setStageRuns(response.recent_runs))
            .catch((e) => setError(String(e)));
    }, [stage, pipelineId]);

    React.useEffect(() => {
        if (!pipelineId) return undefined;
        const tick = () => {
            if (stage) loadStageRuns();
            else loadDetail();
        };
        tick();
        const timer = setInterval(tick, getRefreshIntervalMs());
        return () => clearInterval(timer);
    }, [pipelineId, stage, loadStageRuns, loadDetail]);

    const recentRuns = stage ? stageRuns : (detail?.recent_runs ?? []);

    const refresh = React.useCallback(() => {
        loadCapabilities();
        loadDetail();
        loadStageRuns();
        setGraphRefreshToken((token) => token + 1);
    }, [loadCapabilities, loadDetail, loadStageRuns]);

    const startRun = (labels: [string, string][]) => {
        opsApi
            .startRun(labels)
            .then((started) => {
                const stageName = labels.find(([key]) => key === "stage")?.[1];
                const trigger = stageName ? `api:stage:${stageName}` : "api:pipeline";
                const entry = { ...started, trigger };
                if (stageName === stage) {
                    setStageRuns((current) => prependRecentRun(current, entry));
                }
                if (!stage) {
                    setDetail((current) =>
                        current
                            ? {
                                  ...current,
                                  recent_runs: prependRecentRun(current.recent_runs, entry),
                              }
                            : current,
                    );
                }
                navigate(`/runs/${started.run_id}`);
            })
            .catch((e) => setError(String(e)));
    };

    const title = stage ? `Pipeline graph · ${stage}` : "Pipeline graph";

    return (
        <div className="graph-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Overview", href: "/" },
                    { label: "Graph" },
                    ...(stage ? [{ label: stage }] : []),
                ]}
                title={title}
                onRefresh={refresh}
                extra={
                    agentMode && detail ? (
                        <RunStepsDropdown stages={detail.stages} onStart={startRun} />
                    ) : undefined
                }
            />
            {error && (
                <Alert
                    type="error"
                    message={error}
                    style={{ marginBottom: 12 }}
                    closable
                    onClose={() => setError(null)}
                />
            )}
            <div className="graph-page-overview">
                {detail && pipelineId ? (
                    <PipelineLabelGraphOverview
                        pipelineId={pipelineId}
                        stages={detail.stages}
                        stageEdges={detail.stage_edges}
                        labelGraph={detail.label_graph}
                        selectedLabel={stage}
                        mode="compact"
                        onLabelSelect={(label) =>
                            navigate(`/graph?stage=${encodeURIComponent(label)}`)
                        }
                        onLabelClear={() => navigate("/graph")}
                        onStageRun={agentMode ? (label) => startRun([["stage", label]]) : undefined}
                    />
                ) : (
                    <div style={{ display: "flex", justifyContent: "center", alignItems: "center" }}>
                        <Spin />
                    </div>
                )}
            </div>
            <div className="pipeline-card pipeline-card-with-sidebar">
                <aside className="pipeline-stage-sidebar">
                    <Card title="Recent runs" size="small" className="pipeline-stage-runs-card">
                        <RecentRunsList
                            runs={recentRuns}
                            emptyText={
                                stage ? "No runs for this stage yet" : "No pipeline runs yet"
                            }
                        />
                    </Card>
                </aside>
                <div className="pipeline-card-main">
                    <div className="pipeline-card-header">
                        <div className="pipeline-card-title">
                            <span
                                className="pipeline-card-title-icon"
                                dangerouslySetInnerHTML={{ __html: workflowIconSvg }}
                            />
                            {title}
                        </div>
                    </div>
                    <div className="pipeline-card-body">
                        <PipelineGraphAgentOnly
                            stageFilter={stage}
                            height="100%"
                            rankDir="TB"
                            refreshIntervalMs={0}
                            graphRefreshToken={graphRefreshToken}
                        />
                    </div>
                </div>
            </div>
        </div>
    );
}
