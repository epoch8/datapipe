import React from "react";
import {
    Alert,
    Button,
    Card,
    Descriptions,
    Modal,
    Segmented,
    Spin,
    Table,
    Tabs,
    Tag,
    Typography,
    notification,
} from "antd";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../api/client";
import { ApiError } from "../../api/ops";
import type { PipelineDetail, RunDetail as RunDetailType } from "../../types/ops";
import { PageHeader } from "./shared";
import { PipelineGraphAgentOnly } from "./components/PipelineGraph";
import { PipelineLabelGraphOverview } from "./components/PipelineLabelGraphOverview";
import { RunLogsPanel } from "./components/RunLogsPanel";
import { RunStepsDropdown } from "./components/RunStepsDropdown";
import { resolveRunScopeDisplay } from "./utils/runScope";
import { formatRunTriggerLabel } from "./utils/recentRuns";
import { workflowIconSvg } from "../cy/nodeIcons";
import {
    DEFAULT_LABEL_KEY,
    graphHref,
    labelKeyFromRunLabels,
    normalizeLabelKey,
} from "./utils/labelKey";
import {
    FLOW_LAYOUT_SEGMENTED_OPTIONS,
    parseFlowLayout,
} from "./utils/flowLayout";

const { Text } = Typography;

const RUN_TABS = new Set(["logs", "steps"]);

function statusColor(status: string): string {
    if (status === "failed" || status === "interrupted") return "red";
    if (status === "running") return "blue";
    if (status === "completed") return "green";
    return "default";
}

function formatDuration(started?: string, finished?: string): string | undefined {
    if (!started || !finished) return undefined;
    const ms = new Date(finished).getTime() - new Date(started).getTime();
    if (Number.isNaN(ms) || ms < 0) return undefined;
    const sec = Math.round(ms / 1000);
    if (sec < 60) return `${sec}s`;
    const min = Math.floor(sec / 60);
    return `${min}m ${sec % 60}s`;
}

function formatTrigger(trigger?: string): string {
    if (!trigger) return "manual";
    if (trigger === "api:pipeline") return "api";
    if (trigger.startsWith("api:stage:") || trigger === "api") return "api";
    if (trigger.startsWith("cli:stage:") || trigger === "cli" || trigger === "cli:pipeline") return "cli";
    if (trigger.startsWith("api")) return "api";
    if (trigger.includes("schedule")) return "schedule";
    return trigger;
}

export function RunDetail() {
    const { runId = "" } = useParams();
    const navigate = useNavigate();
    const [searchParams, setSearchParams] = useSearchParams();
    const [run, setRun] = React.useState<RunDetailType | null>(null);
    const [pipeline, setPipeline] = React.useState<PipelineDetail | null>(null);
    const [error, setError] = React.useState<string | null>(null);
    const [refreshToken, setRefreshToken] = React.useState(0);

    const tabParam = searchParams.get("tab");
    const activeTab = tabParam && RUN_TABS.has(tabParam) ? tabParam : "logs";
    const logStepFilter = searchParams.get("step");
    const labelKeyOverride = searchParams.get("label_key");
    const runLabelKey = labelKeyFromRunLabels(run?.target_labels);
    const labelKeyParam = labelKeyOverride || runLabelKey;
    const flowLayout = parseFlowLayout(searchParams);

    const setActiveTab = React.useCallback(
        (tab: string) => {
            setSearchParams(
                (prev) => {
                    const next = new URLSearchParams(prev);
                    if (tab === "logs") next.delete("tab");
                    else next.set("tab", tab);
                    // Step filter only applies on the Logs tab.
                    if (tab !== "logs") next.delete("step");
                    return next;
                },
                { replace: false },
            );
        },
        [setSearchParams],
    );

    const openStepLogs = React.useCallback(
        (stepName: string) => {
            setSearchParams(
                (prev) => {
                    const next = new URLSearchParams(prev);
                    next.delete("tab");
                    next.set("step", stepName);
                    return next;
                },
                { replace: false },
            );
        },
        [setSearchParams],
    );

    const clearStepFilter = React.useCallback(() => {
        setSearchParams(
            (prev) => {
                const next = new URLSearchParams(prev);
                next.delete("step");
                return next;
            },
            { replace: true },
        );
    }, [setSearchParams]);

    const setFlowLayout = React.useCallback(
        (nextLayout: string) => {
            setSearchParams(
                (prev) => {
                    const next = new URLSearchParams(prev);
                    next.delete("direction");
                    if (nextLayout === "snake") next.delete("layout");
                    else next.set("layout", nextLayout);
                    return next;
                },
                { replace: true },
            );
        },
        [setSearchParams],
    );

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

    React.useEffect(() => {
        if (!run?.pipeline_id) return;
        opsApi
            .getPipeline(labelKeyParam ? { label_key: labelKeyParam } : undefined)
            .then(setPipeline)
            .catch(() => setPipeline(null));
    }, [run?.pipeline_id, labelKeyParam]);

    if (error) return <Alert type="error" message={error} />;
    if (!run) return <Spin />;

    const { scopeLabel, targetLabel, highlightLabel } = resolveRunScopeDisplay(run);
    const isStageRun = scopeLabel === "stage run";
    const availableKeys = pipeline?.available_label_keys ?? [];
    const labelKey = normalizeLabelKey(
        labelKeyParam ?? pipeline?.label_graph?.label_key,
        availableKeys,
    );
    const duration = formatDuration(run.started_at, run.finished_at);
    const runIsActive = run.status === "running";
    const pipelineGraphTitle = highlightLabel
        ? `Pipeline graph · ${highlightLabel}`
        : "Pipeline graph";

    const setLabelKey = (nextKey: string) => {
        setSearchParams(
            (prev) => {
                const next = new URLSearchParams(prev);
                if (nextKey === DEFAULT_LABEL_KEY && runLabelKey === DEFAULT_LABEL_KEY) {
                    next.delete("label_key");
                } else {
                    next.set("label_key", nextKey);
                }
                return next;
            },
            { replace: true },
        );
    };

    const startRun = (labels: [string, string][]) => {
        opsApi
            .startRun(labels)
            .then((started) => navigate(`/runs/${started.run_id}`))
            .catch((e) => setError(String(e)));
    };

    const stopRun = () => {
        Modal.confirm({
            title: "Stop this run?",
            content:
                "Training subprocesses will be terminated immediately and the run marked as interrupted.",
            okText: "Stop run",
            okButtonProps: { danger: true },
            onOk: async () => {
                try {
                    await opsApi.stopRun(run.run_id);
                    notification.success({ message: "Run stop requested" });
                    refresh();
                } catch (err) {
                    notification.error({
                        message: "Stop failed",
                        description: err instanceof ApiError ? err.message : String(err),
                    });
                    throw err;
                }
            },
        });
    };

    const headerExtra = (
        <>
            {runIsActive ? (
                <Button danger onClick={stopRun} style={{ marginRight: 8 }}>
                    Stop run
                </Button>
            ) : null}
            {pipeline ? (
                <RunStepsDropdown
                    pipelineId={run.pipeline_id}
                    stages={pipeline.stages}
                    availableLabelKeys={availableKeys}
                    labelGraph={pipeline.label_graph}
                    defaultLabelKey={labelKey}
                    onStart={startRun}
                />
            ) : null}
        </>
    );

    const stepPageHref = (stepName: string) =>
        `/pipelines/${encodeURIComponent(run.pipeline_id)}/transforms/${encodeURIComponent(stepName)}`;

    const stepColumns = [
        {
            title: "Step",
            dataIndex: "step_name",
            key: "step_name",
            render: (name: string) => <Link to={stepPageHref(name)}>{name}</Link>,
        },
        {
            title: "Status",
            dataIndex: "status",
            key: "status",
            render: (v: string) => <Tag color={statusColor(v)}>{v}</Tag>,
        },
        {
            title: "Progress",
            key: "progress",
            render: (_: unknown, row: RunDetailType["steps"][number]) => {
                const processed = row.processed;
                const total = row.total;
                if (processed == null && total == null) return "—";
                if (total != null && total > 0) {
                    const pct = Math.round((100 * (processed ?? 0)) / total);
                    return `${processed ?? 0}/${total} (${pct}%)`;
                }
                if (processed != null) return `${processed}/?`;
                return "—";
            },
        },
        { title: "Started", dataIndex: "started_at", key: "started_at" },
        { title: "Finished", dataIndex: "finished_at", key: "finished_at" },
        {
            title: "Duration",
            key: "duration",
            render: (_: unknown, row: RunDetailType["steps"][number]) =>
                formatDuration(row.started_at, row.finished_at) ?? "—",
        },
        {
            title: "Logs",
            key: "logs",
            render: (_: unknown, row: RunDetailType["steps"][number]) => (
                <Button type="link" size="small" onClick={() => openStepLogs(row.step_name)}>
                    View
                </Button>
            ),
        },
    ];

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Overview", href: "/" },
                    { label: run.pipeline_id, href: `/pipelines/${run.pipeline_id}` },
                    { label: `Run ${run.run_id.slice(0, 8)}` },
                ]}
                title={`Run ${run.run_id.slice(0, 8)}`}
                statusChips={[
                    {
                        label: run.status,
                        variant: run.status === "completed" ? "success" : "default",
                    },
                ]}
                onRefresh={refresh}
                extra={headerExtra}
            />

            <Card style={{ marginBottom: 16 }}>
                <Descriptions column={{ xs: 1, sm: 2, md: 3 }} size="small">
                    <Descriptions.Item label="Status">
                        <Tag color={statusColor(run.status)}>{run.status}</Tag>
                    </Descriptions.Item>
                    <Descriptions.Item label="Scope">{scopeLabel}</Descriptions.Item>
                    <Descriptions.Item label="Target label">{targetLabel}</Descriptions.Item>
                    <Descriptions.Item label="Started">{run.started_at ?? "—"}</Descriptions.Item>
                    <Descriptions.Item label="Finished">{run.finished_at ?? "—"}</Descriptions.Item>
                    <Descriptions.Item label="Duration">{duration ?? "—"}</Descriptions.Item>
                    <Descriptions.Item label="Trigger">
                        {formatTrigger(run.trigger)}
                        {formatRunTriggerLabel(run.trigger) && (
                            <Text type="secondary"> ({formatRunTriggerLabel(run.trigger)})</Text>
                        )}
                    </Descriptions.Item>
                </Descriptions>
            </Card>

            {pipeline && (
                <div style={{ marginBottom: 16 }}>
                    <PipelineLabelGraphOverview
                        stages={pipeline.stages}
                        stageEdges={pipeline.stage_edges}
                        labelGraph={pipeline.label_graph}
                        availableLabelKeys={availableKeys}
                        labelKey={labelKey}
                        mode="compact"
                        scopeHighlightAll={!isStageRun && labelKey === runLabelKey}
                        scopeHighlightLabel={
                            labelKey === runLabelKey ? highlightLabel : null
                        }
                        scopeMuteOutside={isStageRun && labelKey === runLabelKey}
                        allowClickSelect={false}
                        onLabelKeyChange={setLabelKey}
                        onLabelSelect={(label) => navigate(graphHref(label, labelKey))}
                        onStageRun={(label) => startRun([[labelKey, label]])}
                    />
                </div>
            )}

            {run.error && (
                <Alert style={{ marginBottom: 16 }} type="error" message={run.error} showIcon />
            )}

            <Tabs activeKey={activeTab} onChange={setActiveTab}>
                <Tabs.TabPane tab="Logs" key="logs">
                    <RunLogsPanel
                        runId={run.run_id}
                        status={run.status}
                        stepFilter={logStepFilter}
                        onClearStepFilter={clearStepFilter}
                    />
                    <div className="pipeline-card pipeline-card-main" style={{ marginTop: 16 }}>
                        <div className="pipeline-card-header">
                            <div className="pipeline-card-header-left">
                                <div className="pipeline-card-title">
                                    <span
                                        className="pipeline-card-title-icon"
                                        dangerouslySetInnerHTML={{ __html: workflowIconSvg }}
                                    />
                                    {pipelineGraphTitle}
                                </div>
                                <Segmented
                                    size="small"
                                    value={flowLayout}
                                    options={[...FLOW_LAYOUT_SEGMENTED_OPTIONS]}
                                    onChange={(value) => setFlowLayout(String(value))}
                                />
                            </div>
                        </div>
                        <div className="pipeline-card-body">
                            <PipelineGraphAgentOnly
                                stageFilter={
                                    labelKey === runLabelKey ? highlightLabel : null
                                }
                                labelKey={labelKey}
                                runSteps={run.steps}
                                height={480}
                                flowLayout={flowLayout}
                                refreshIntervalMs={runIsActive ? getRefreshIntervalMs() : 0}
                                graphRefreshToken={refreshToken}
                                pipelineId={run.pipeline_id}
                            />
                        </div>
                    </div>
                </Tabs.TabPane>
                <Tabs.TabPane tab="Steps" key="steps">
                    <Table
                        rowKey="step_name"
                        size="small"
                        pagination={false}
                        columns={stepColumns}
                        dataSource={run.steps}
                    />
                </Tabs.TabPane>
            </Tabs>

            <div style={{ marginTop: 12 }}>
                <Link to={`/pipelines/${run.pipeline_id}`}>Back to pipeline</Link>
            </div>
        </div>
    );
}
