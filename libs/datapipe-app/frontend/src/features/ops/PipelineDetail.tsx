import React from "react";
import { Alert, Button, Card, Dropdown, Menu, Space, Spin, Tag, Typography } from "antd";
import { Link, useNavigate, useParams } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../api/ops";
import type { ChartSpec, PipelineDetail as PipelineDetailType } from "../../types/ops";
import { ChartGrid } from "./components/ChartGrid";
import { PipelineMetrics } from "./components/PipelineMetrics";
import { PluginSection } from "./components/PluginSection";
import { RecentRunsList } from "./components/RecentRunsList";
import { StageStepper } from "./components/StageStepper";

const { Text, Title } = Typography;

type PipelineDetailProps = {
    pipelineId?: string;
    embedded?: boolean;
    includeMetrics?: boolean;
};

export function PipelineDetail({
    pipelineId: pipelineIdProp,
    embedded = false,
    includeMetrics = false,
}: PipelineDetailProps = {}) {
    const { id: routeId = "" } = useParams();
    const id = pipelineIdProp ?? routeId;
    const navigate = useNavigate();
    const [detail, setDetail] = React.useState<PipelineDetailType | null>(null);
    const [curves, setCurves] = React.useState<ChartSpec[]>([]);
    const [error, setError] = React.useState<string | null>(null);
    const [running, setRunning] = React.useState(false);

    const load = React.useCallback(() => {
        if (!id) return;
        opsApi
            .getPipeline(id)
            .then(setDetail)
            .catch((e) => setError(String(e)));
    }, [id]);

    React.useEffect(() => {
        load();
        const timer = setInterval(load, getRefreshIntervalMs());
        return () => clearInterval(timer);
    }, [load]);

    React.useEffect(() => {
        const enrich = detail?.enrichments?.find((e) => e.type === "ml_training");
        const runKey = (enrich?.payload as { run_key?: string })?.run_key;
        if (!runKey) return;
        opsApi.getTrainingCurves(runKey, 50).then((r) => setCurves(r.charts)).catch(() => undefined);
    }, [detail]);

    const runStage = (labels: [string, string][]) => {
        setRunning(true);
        opsApi
            .startRun(labels)
            .then(() => load())
            .catch((e) => setError(String(e)))
            .finally(() => setRunning(false));
    };

    if (!id) return <Alert type="error" message="Pipeline id is required" />;
    if (error) return <Alert type="error" message={error} />;
    if (!detail) return <Spin />;

    const stageMenu = (
        <Menu>
            {detail.stages.map((s) => (
                <Menu.Item key={s.stage} onClick={() => runStage([["stage", s.stage]])}>
                    {s.stage}
                </Menu.Item>
            ))}
        </Menu>
    );

    return (
        <div>
            {embedded ? (
                <Title level={3} style={{ marginTop: 0, marginBottom: 8 }}>
                    {detail.display_name}
                </Title>
            ) : (
                <Text type="secondary">
                    <Link to="/">Overview</Link> / {detail.display_name}
                </Text>
            )}
            <div style={{ marginTop: embedded ? 0 : 8, marginBottom: 16 }}>
                {detail.task_type && <Tag>{detail.task_type}</Tag>}
                <Tag color={detail.health === "failed" ? "red" : "green"}>{detail.health}</Tag>
            </div>
            <StageStepper
                stages={detail.stages}
                edges={detail.stage_edges}
                onStageSelect={(stage) =>
                    navigate(`/graph?stage=${encodeURIComponent(stage)}`)
                }
                onStageRun={
                    detail.agent_mode
                        ? (stage) => runStage([["stage", stage]])
                        : undefined
                }
            />
            {detail.agent_mode && (
                <Space style={{ marginBottom: 16 }}>
                    <Button loading={running} onClick={() => runStage([])}>
                        Run pipeline
                    </Button>
                    <Dropdown overlay={stageMenu}>
                        <Button loading={running}>Run stage</Button>
                    </Dropdown>
                </Space>
            )}
            {detail.last_error && (
                <Card title="Error" style={{ marginBottom: 16 }}>
                    <Text type="danger">{detail.last_error}</Text>
                </Card>
            )}
            {curves.length > 0 && (
                <Card title="Training preview" extra={<Link to={`/pipelines/${id}/training`}>Training runs</Link>}>
                    <ChartGrid charts={curves} />
                </Card>
            )}
            {includeMetrics && <PipelineMetrics pipelineId={id} />}
            <PluginSection enrichments={detail.enrichments} />
            <Card title="Recent runs" style={{ marginTop: 16 }}>
                <RecentRunsList runs={detail.recent_runs} />
            </Card>
        </div>
    );
}
