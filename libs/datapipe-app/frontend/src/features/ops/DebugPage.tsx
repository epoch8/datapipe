import React from "react";
import { Alert, Button, Card } from "antd";
import { Link, useSearchParams } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../api/ops";
import type { Capabilities, RecentRunSummary } from "../../types/ops";
import { PipelineGraphAgentOnly } from "./components/PipelineGraph";
import { RecentRunsList } from "./components/RecentRunsList";
import { workflowIconSvg } from "../cy/nodeIcons";

export function DebugPage() {
    const [searchParams] = useSearchParams();
    const stage = searchParams.get("stage");
    const [capabilities, setCapabilities] = React.useState<Capabilities | null>(null);
    const [stageRuns, setStageRuns] = React.useState<RecentRunSummary[]>([]);
    const [running, setRunning] = React.useState(false);
    const [error, setError] = React.useState<string | null>(null);

    const pipelineId = capabilities?.pipeline_id;
    const agentMode = capabilities?.mode === "agent";

    React.useEffect(() => {
        opsApi.getCapabilities().then(setCapabilities).catch((e) => setError(String(e)));
    }, []);

    const loadStageRuns = React.useCallback(() => {
        if (!stage || !pipelineId) return;
        opsApi
            .resolveStageRecentRuns(pipelineId, stage)
            .then((response) => setStageRuns(response.recent_runs))
            .catch((e) => setError(String(e)));
    }, [stage, pipelineId]);

    React.useEffect(() => {
        loadStageRuns();
        if (!stage || !pipelineId) return undefined;
        const timer = setInterval(loadStageRuns, getRefreshIntervalMs());
        return () => clearInterval(timer);
    }, [loadStageRuns, stage, pipelineId]);

    const runStage = () => {
        if (!stage) return;
        setRunning(true);
        opsApi
            .startRun([["stage", stage]])
            .then(() => loadStageRuns())
            .catch((e) => setError(String(e)))
            .finally(() => setRunning(false));
    };

    const title = stage ? `Pipeline graph · ${stage}` : "Pipeline graph";

    return (
        <div>
            <div className="datapipe-breadcrumb">
                <Link to="/">Overview</Link> / Debug
                {stage ? ` / ${stage}` : ""}
            </div>
            {error && (
                <Alert type="error" message={error} style={{ marginBottom: 12 }} closable onClose={() => setError(null)} />
            )}
            <div className={`pipeline-card${stage ? " pipeline-card-with-sidebar" : ""}`}>
                {stage && (
                    <aside className="pipeline-stage-sidebar">
                        {agentMode && (
                            <Button type="primary" block loading={running} onClick={runStage} style={{ marginBottom: 16 }}>
                                Run stage
                            </Button>
                        )}
                        <Card title="Recent runs" size="small" className="pipeline-stage-runs-card">
                            <RecentRunsList runs={stageRuns} emptyText="No runs for this stage yet" />
                        </Card>
                    </aside>
                )}
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
                        />
                    </div>
                </div>
            </div>
        </div>
    );
}
