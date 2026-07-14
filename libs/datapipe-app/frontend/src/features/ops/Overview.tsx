import React from "react";
import { Alert, Card, Spin, Tag } from "antd";
import { Link, useNavigate } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../api/ops";
import { ApiErrorAlert } from "../../components/ApiErrorAlert";
import type { PipelineDetail } from "../../types/ops";
import { RunStepsDropdown } from "./components/RunStepsDropdown";
import { RecentRunsList } from "./components/RecentRunsList";
import { PageHeader } from "./shared";
import { PipelineOverviewGraphCard } from "./pipeline/PipelineOverviewGraphCard";
import { PipelineRunStats } from "./pipeline/PipelineRunStats";
import { prependRecentRun } from "./utils/recentRuns";

const RECENT_RUNS_LIMIT = 7;

export function Overview() {
    const navigate = useNavigate();
    const [pipelineId, setPipelineId] = React.useState<string | null>(null);
    const [detail, setDetail] = React.useState<PipelineDetail | null>(null);
    const [runTotal, setRunTotal] = React.useState(0);
    const [countsByStatus, setCountsByStatus] = React.useState<Record<string, number>>({});
    const [error, setError] = React.useState<unknown>(null);

    const load = React.useCallback(() => {
        if (!pipelineId) return;
        opsApi
            .getPipeline(pipelineId)
            .then(setDetail)
            .catch((e) => setError(e));
        opsApi
            .getRuns({ pipeline_id: pipelineId, limit: 1, offset: 0 })
            .then((res) => {
                setRunTotal(res.total);
                setCountsByStatus(res.counts_by_status ?? {});
            })
            .catch(() => undefined);
    }, [pipelineId]);

    React.useEffect(() => {
        opsApi
            .getCapabilities()
            .then((capabilities) => setPipelineId(capabilities.pipeline_id ?? null))
            .catch((e) => setError(e));
    }, []);

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

    if (error) return <ApiErrorAlert error={error} />;
    if (!pipelineId || !detail) return <Spin />;

    const recentRuns = detail.recent_runs.slice(0, RECENT_RUNS_LIMIT);

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[{ label: "Overview" }]}
                title={detail.display_name}
                onRefresh={load}
                extra={<RunStepsDropdown stages={detail.stages} onStart={runStage} />}
            />
            <div style={{ marginBottom: 16 }}>
                {detail.task_type && <Tag>{detail.task_type}</Tag>}
                <Tag color={detail.health === "failed" ? "red" : "green"}>{detail.health}</Tag>
            </div>

            <PipelineOverviewGraphCard
                pipelineId={pipelineId}
                detail={detail}
                onStageRun={runStage}
            />

            <PipelineRunStats
                runs={detail.recent_runs}
                countsByStatus={countsByStatus}
                total={runTotal}
            />

            <Card title="Recent runs" className="overview-recent-runs-card" style={{ marginTop: 16 }}>
                <RecentRunsList
                    runs={recentRuns}
                    compact
                    viewAllHref="/runs"
                />
            </Card>

            {detail.last_error && (
                <Alert
                    type="error"
                    message="Last error"
                    description={detail.last_error}
                    style={{ marginTop: 16 }}
                    showIcon
                />
            )}

            {detail.next_run_at && (
                <Alert
                    type="info"
                    message="Next scheduled run"
                    description={
                        <>
                            {detail.next_run_at}
                            {detail.next_run_at && (
                                <>
                                    {" "}
                                    · <Link to="/runs">View runs</Link>
                                </>
                            )}
                        </>
                    }
                    style={{ marginTop: 16 }}
                    showIcon
                />
            )}
        </div>
    );
}
