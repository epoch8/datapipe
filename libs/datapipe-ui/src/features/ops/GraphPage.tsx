import React from "react";
import { Spin } from "antd";
import { useNavigate, useSearchParams } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../api/client";
import { ApiErrorAlert } from "../../components/ApiErrorAlert";
import type { PipelineDetail } from "../../types/ops";
import { PipelineGraphAgentOnly } from "./components/PipelineGraph";
import { PipelineLabelGraphOverview } from "./components/PipelineLabelGraphOverview";
import { PageHeader } from "./shared";
import { workflowIconSvg } from "../cy/nodeIcons";
import {
    DEFAULT_LABEL_KEY,
    graphHref,
    normalizeLabelKey,
} from "./utils/labelKey";
import {
    FLOW_LAYOUT_SEGMENTED_OPTIONS,
    parseFlowLayout,
} from "./utils/flowLayout";
import {
    buildLabelColumnMap,
    topLevelLabelOrder,
} from "../cy/columnLayout";

/**
 * Cloud Ops graph page: label chrome + pipeline DAG against `/api/v1alpha3/graph`.
 * Labels + layout controls live in the right inspector column so the DAG owns
 * the full left height.
 */
export function GraphPage() {
    const [searchParams, setSearchParams] = useSearchParams();
    const navigate = useNavigate();
    const stage = searchParams.get("stage");
    const labelKeyParam = searchParams.get("label_key");
    const flowLayout = parseFlowLayout(searchParams);
    const [detail, setDetail] = React.useState<PipelineDetail | null>(null);
    const [error, setError] = React.useState<unknown>(null);
    const [graphRefreshToken, setGraphRefreshToken] = React.useState(0);

    const availableKeys = detail?.available_label_keys ?? [];
    const labelKey = normalizeLabelKey(
        labelKeyParam ?? detail?.label_graph?.label_key,
        availableKeys,
    );
    const labelOrder = React.useMemo(
        () => topLevelLabelOrder(detail?.label_graph?.nodes ?? []),
        [detail?.label_graph?.nodes],
    );
    const labelColumnMap = React.useMemo(() => {
        const map = buildLabelColumnMap(detail?.label_graph?.nodes ?? []);
        return Object.fromEntries(map.entries());
    }, [detail?.label_graph?.nodes]);

    const clearLabelFocusHref = React.useMemo(() => {
        const params = new URLSearchParams();
        if (labelKey !== DEFAULT_LABEL_KEY) params.set("label_key", labelKey);
        if (flowLayout !== "snake") params.set("layout", flowLayout);
        const qs = params.toString();
        return qs ? `/graph?${qs}` : "/graph";
    }, [labelKey, flowLayout]);

    const loadDetail = React.useCallback(() => {
        opsApi
            .getPipeline(labelKeyParam ? { label_key: labelKeyParam } : undefined)
            .then(setDetail)
            .catch((e) => setError(e));
    }, [labelKeyParam]);

    React.useEffect(() => {
        loadDetail();
        const timer = setInterval(loadDetail, getRefreshIntervalMs());
        return () => clearInterval(timer);
    }, [loadDetail]);

    const refresh = React.useCallback(() => {
        loadDetail();
        setGraphRefreshToken((token) => token + 1);
    }, [loadDetail]);

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

    const setFlowLayout = (nextLayout: string) => {
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
    };

    const clearLabelFocus = React.useCallback(() => {
        navigate(clearLabelFocusHref);
    }, [navigate, clearLabelFocusHref]);

    const selectGraphLabel = React.useCallback(
        (label: string) => {
            if (stage === label) {
                clearLabelFocus();
                return;
            }
            const href = graphHref(label, labelKey);
            if (flowLayout !== "snake") {
                navigate(`${href}&layout=${flowLayout}`);
                return;
            }
            navigate(href);
        },
        [navigate, labelKey, stage, clearLabelFocus, flowLayout],
    );

    const title = "Pipeline graph";

    const sideChrome = (
        <>
            <div className="pipeline-graph-side-labels">
                {detail ? (
                    <PipelineLabelGraphOverview
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
                    />
                ) : (
                    <div className="pipeline-graph-side-labels-loading">
                        <Spin size="small" />
                    </div>
                )}
            </div>
            <div className="pipeline-graph-side-chrome-title">
                <span
                    className="pipeline-card-title-icon"
                    dangerouslySetInnerHTML={{ __html: workflowIconSvg }}
                />
                <span>{title}</span>
            </div>
            {stage ? (
                <div
                    className="pipeline-card-label-badge pipeline-graph-side-chrome-badge"
                    title={`${labelKey}=${stage}`}
                >
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
                </div>
            ) : null}
            <nav className="pipeline-graph-side-chrome-layouts" aria-label="Graph layout">
                {FLOW_LAYOUT_SEGMENTED_OPTIONS.map((opt) => (
                    <button
                        key={opt.value}
                        type="button"
                        className={
                            flowLayout === opt.value
                                ? "pipeline-graph-layout-link active"
                                : "pipeline-graph-layout-link"
                        }
                        onClick={() => setFlowLayout(opt.value)}
                    >
                        {opt.label}
                    </button>
                ))}
            </nav>
        </>
    );

    return (
        <div className="graph-page graph-page-dag-first">
            <PageHeader
                breadcrumbs={[
                    { label: "General", href: "/" },
                    { label: "Graph" },
                    ...(stage ? [{ label: stage }] : []),
                ]}
                title={stage ? `Pipeline graph · ${stage}` : title}
                onRefresh={refresh}
            />
            {error ? (
                <div style={{ marginBottom: 12 }}>
                    <ApiErrorAlert error={error} />
                </div>
            ) : null}
            <div className="pipeline-card pipeline-card-graph-fill">
                <div className="pipeline-card-body">
                    <PipelineGraphAgentOnly
                        labelFilter={stage}
                        labelKey={labelKey}
                        labelOrder={labelOrder}
                        labelColumnMap={labelColumnMap}
                        layoutMode="columns"
                        height="100%"
                        flowLayout={flowLayout}
                        refreshIntervalMs={0}
                        graphRefreshToken={graphRefreshToken}
                        sideChrome={sideChrome}
                    />
                </div>
            </div>
        </div>
    );
}
