import React from "react";
import {
    Alert,
    Card,
    Collapse,
    Progress,
    Spin,
    Tag,
    Typography,
} from "antd";
import { useParams } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../api/ops";
import type { RunDetail as RunDetailType } from "../../types/ops";
import { PageHeader } from "./shared";
import { PipelineGraphAgentOnly } from "./components/PipelineGraph";
import { RunLogsPanel } from "./components/RunLogsPanel";

const { Title } = Typography;

function statusColor(status: string): string {
    if (status === "failed") return "red";
    if (status === "running") return "blue";
    if (status === "completed") return "green";
    return "default";
}

export function RunDetail() {
    const { runId = "" } = useParams();
    const [run, setRun] = React.useState<RunDetailType | null>(null);
    const [error, setError] = React.useState<string | null>(null);
    const [refreshToken, setRefreshToken] = React.useState(0);

    const refresh = React.useCallback(() => {
        setRefreshToken((token) => token + 1);
    }, []);

    React.useEffect(() => {
        let cancelled = false;
        let timer: ReturnType<typeof setInterval> | undefined;

        const load = () => {
            opsApi
                .getRun(runId)
                .then((data) => {
                    if (cancelled) return;
                    setRun(data);
                    if (data.status !== "running" && timer) {
                        clearInterval(timer);
                        timer = undefined;
                    }
                })
                .catch((e) => {
                    if (!cancelled) setError(String(e));
                });
        };

        load();
        timer = setInterval(load, getRefreshIntervalMs());
        return () => {
            cancelled = true;
            if (timer) clearInterval(timer);
        };
    }, [runId, refreshToken]);

    if (error) return <Alert type="error" message={error} />;
    if (!run) return <Spin />;

    const runningSteps = run.steps.filter((s) => s.status === "running");
    const completedCount = run.steps.filter((s) => s.status === "completed").length;
    const progressPct = run.steps.length
        ? Math.round((completedCount / run.steps.length) * 100)
        : 0;

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Overview", href: "/" },
                    { label: run.pipeline_id, href: `/pipelines/${run.pipeline_id}` },
                    { label: "Run" },
                ]}
                title={`Run ${run.run_id.slice(0, 8)}`}
                onRefresh={refresh}
            />

            <Card>
                <Title level={5} style={{ marginTop: 0 }}>
                    Execution status
                </Title>
                <Tag color={statusColor(run.status)}>{run.status}</Tag>
                <div style={{ marginTop: 8 }}>
                    Started: {run.started_at}
                    {run.finished_at && <> · Finished: {run.finished_at}</>}
                </div>
                {run.status === "running" && runningSteps.length > 0 && (
                    <div style={{ marginTop: 8 }}>
                        Current step: <strong>{runningSteps[0].step_name}</strong>
                        {runningSteps[0].total != null && (
                            <span>
                                {" "}
                                ({runningSteps[0].processed ?? 0}/{runningSteps[0].total})
                            </span>
                        )}
                    </div>
                )}
                {run.steps.length > 0 && (
                    <Progress
                        style={{ marginTop: 12, maxWidth: 480 }}
                        percent={progressPct}
                        size="small"
                        status={run.status === "failed" ? "exception" : undefined}
                    />
                )}
                {run.error && (
                    <Alert style={{ marginTop: 12 }} type="error" message={run.error} showIcon />
                )}
            </Card>

            <div style={{ marginTop: 16 }}>
                <RunLogsPanel runId={run.run_id} status={run.status} />
            </div>

            <Collapse style={{ marginTop: 16 }}>
                <Collapse.Panel header="Pipeline steps" key="graph">
                    <PipelineGraphAgentOnly
                        pipelineId={run.pipeline_id}
                        runSteps={run.steps}
                        height={480}
                        rankDir="TB"
                    />
                </Collapse.Panel>
            </Collapse>
        </div>
    );
}
