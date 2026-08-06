import React from "react";
import { Card, Spin } from "antd";
import { useNavigate, useSearchParams } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../api/client";
import { ApiErrorAlert } from "../../components/ApiErrorAlert";
import type { Capabilities, PipelineDetail, RecentRunSummary } from "../../types/ops";
import { PipelineGraphAgentOnly } from "./components/PipelineGraph";
import { PipelineLabelGraphOverview } from "./components/PipelineLabelGraphOverview";
import { RecentRunsList } from "./components/RecentRunsList";
import { PageHeader } from "./shared";
import { workflowIconSvg } from "../cy/nodeIcons";
import { prependRecentRun } from "./utils/recentRuns";
import { RunStepsDropdown } from "./components/RunStepsDropdown";
import {
    DEFAULT_LABEL_KEY,
    graphHref,
    normalizeLabelKey,
} from "./utils/labelKey";

export function GraphPage() {
    const [searchParams, setSearchParams] = useSearchParams();
    const navigate = useNavigate();
    const stage = searchParams.get("stage");
    const labelKeyParam = searchParams.get("label_key") || DEFAULT_LABEL_KEY;
    const [capabilities, setCapabilities] = React.useState<Capabilities | null>(null);
    const [detail, setDetail] = React.useState<PipelineDetail | null>(null);
    const [stageRuns, setStageRuns] = React.useState<RecentRunSummary[]>([]);
    const [error, setError] = React.useState<unknown>(null);
    const [graphRefreshToken, setGraphRefreshToken] = React.useState(0);

    const pipelineId = capabilities?.pipeline_id;
    const availableKeys = detail?.available_label_keys ?? [];
    const labelKey = normalizeLabelKey(
        labelKeyParam ?? detail?.label_graph?.label_key,
        availableKeys,
    );
    const labelOrder = React.useMemo(
        () =>
            [...(detail?.label_graph?.nodes ?? [])]
                .filter((node) => node.kind === "label")
                .sort(
                    (a, b) =>
                        a.order_min - b.order_min ||
                        a.order_max - b.order_max ||
                        a.id.localeCompare(b.id),
                )
                .map((node) => node.id),
        [detail?.label_graph?.nodes],
    );

    const clearLabelFocusHref =
        labelKey === DEFAULT_LABEL_KEY
            ? "/graph"
            : `/graph?label_key=${encodeURIComponent(labelKey)}`;

    const loadCapabilities = React.useCallback(() => {
        opsApi.getCapabilities().then(setCapabilities).catch((e) => setError(e));
    }, []);

    const loadDetail = React.useCallback(() => {
        if (!pipelineId) return;
        opsApi
            .getPipeline(pipelineId, { label_key: labelKeyParam })
            .then(setDetail)
            .catch((e) => setError(e));
    }, [pipelineId, labelKeyParam]);

    React.useEffect(() => {
        loadCapabilities();
    }, [loadCapabilities]);

    React.useEffect(() => {
        loadDetail();
    }, [loadDetail]);

    const loadStageRuns = React.useCallback(() => {
        if (!stage || !pipelineId || labelKeyParam !== DEFAULT_LABEL_KEY) {
            setStageRuns([]);
            return;
        }
        opsApi
            .resolveStageRecentRuns(pipelineId, stage)
            .then((response) => setStageRuns(response.recent_runs))
            .catch((e) => setError(e));
    }, [stage, pipelineId, labelKeyParam]);

    React.useEffect(() => {
        if (!pipelineId) return undefined;
        const tick = () => {
            if (stage && labelKeyParam === DEFAULT_LABEL_KEY) loadStageRuns();
            else loadDetail();
        };
        tick();
        const timer = setInterval(tick, getRefreshIntervalMs());
        return () => clearInterval(timer);
    }, [pipelineId, stage, labelKeyParam, loadStageRuns, loadDetail]);

    const recentRuns =
        stage && labelKeyParam === DEFAULT_LABEL_KEY
            ? stageRuns
            : (detail?.recent_runs ?? []);

    const refresh = React.useCallback(() => {
        loadCapabilities();
        loadDetail();
        loadStageRuns();
        setGraphRefreshToken((token) => token + 1);
    }, [loadCapabilities, loadDetail, loadStageRuns]);

    const setLabelKey = (nextKey: string) => {
        setSearchParams(
            (prev) => {
                const next = new URLSearchParams(prev);
                if (nextKey === DEFAULT_LABEL_KEY) next.delete("label_key");
                else next.set("label_key", nextKey);
                next.delete("stage");
                return next;
            },
            { replace: true },
        );
    };

    const clearLabelFocus = React.useCallback(() => {
        navigate(clearLabelFocusHref);
    }, [navigate, clearLabelFocusHref]);

    const selectGraphLabel = React.useCallback(
        (label: string) => {
            // Second click on the active label clears focus (prototype behavior).
            if (stage === label) {
                clearLabelFocus();
                return;
            }
            navigate(graphHref(label, labelKey));
        },
        [navigate, labelKey, stage, clearLabelFocus],
    );

    const startRun = (labels: [string, string][]) => {
        opsApi
            .startRun(labels)
            .then((started) => {
                const stageName = labels.find(([key]) => key === "stage")?.[1];
                const trigger = stageName ? `api:stage:${stageName}` : "api:pipeline";
                const entry = { ...started, trigger };
                if (stageName === stage && labelKeyParam === DEFAULT_LABEL_KEY) {
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
            .catch((e) => setError(e));
    };

    const title = "Pipeline graph";

    return (
        <div className="graph-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Overview", href: "/" },
                    { label: "Graph" },
                    ...(stage ? [{ label: stage }] : []),
                ]}
                title={stage ? `Pipeline graph · ${stage}` : title}
                onRefresh={refresh}
                extra={
                    detail ? (
                        <RunStepsDropdown stages={detail.stages} onStart={startRun} />
                    ) : undefined
                }
            />
            {error ? (
                <div style={{ marginBottom: 12 }}>
                    <ApiErrorAlert error={error} />
                </div>
            ) : null}
            <div className="graph-page-overview">
                {detail && pipelineId ? (
                    <PipelineLabelGraphOverview
                        pipelineId={pipelineId}
                        stages={detail.stages}
                        stageEdges={detail.stage_edges}
                        labelGraph={detail.label_graph}
                        availableLabelKeys={availableKeys}
                        labelKey={labelKey}
                        selectedLabel={stage}
                        mode="compact"
                        onLabelKeyChange={setLabelKey}
                        onLabelSelect={selectGraphLabel}
                        onLabelClear={clearLabelFocus}
                        onStageRun={(label) => startRun([[labelKey, label]])}
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
                                stage ? "No runs for this label yet" : "No pipeline runs yet"
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
                            {stage ? (
                                <span className="pipeline-card-label-badge" title={`${labelKey}=${stage}`}>
                                    <span className="pipeline-card-label-badge-key">{labelKey}</span>
                                    <span className="pipeline-card-label-badge-value">{stage}</span>
                                    <button
                                        type="button"
                                        className="pipeline-card-label-badge-clear"
                                        onClick={clearLabelFocus}
                                        aria-label="Clear label focus"
                                    >
                                        ×
                                    </button>
                                </span>
                            ) : null}
                        </div>
                    </div>
                    <div className="pipeline-card-body">
                        <PipelineGraphAgentOnly
                            labelFilter={stage}
                            labelKey={labelKey}
                            labelOrder={labelOrder}
                            layoutMode="columns"
                            height="100%"
                            rankDir="LR"
                            refreshIntervalMs={0}
                            graphRefreshToken={graphRefreshToken}
                        />
                    </div>
                </div>
            </div>
        </div>
    );
}
