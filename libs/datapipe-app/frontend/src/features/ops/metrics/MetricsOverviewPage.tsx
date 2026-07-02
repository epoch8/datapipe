import React from "react";
import { useNavigate } from "react-router-dom";
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import { useUrlNumber, useUrlState } from "../../../hooks/useUrlState";
import type { MetricsRunRow, MetricsSummaryResponse, MetricsTimeseriesResponse } from "../../../types/ops";
import {
    ChartCard,
    defaultDateRange,
    EmptyState,
    FilterBar,
    KpiCard,
    PageHeader,
} from "../shared";
import { AnomaliesPanel, BestRunPanel, MetricLegend, PipelineStagesStrip } from "./MetricsPanels";
import { MetricsRunTable } from "./MetricsRunTable";

export function MetricsOverviewPage() {
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const navigate = useNavigate();
    const [subset, setSubset] = useUrlState("subset");
    const [modelId, setModelId] = useUrlState("model_id");
    const [search, setSearch] = useUrlState("search");
    const [sortBy, setSortBy] = useUrlState("sort_by", "started_at");
    const [sortDir, setSortDir] = useUrlState("sort_dir", "desc");
    const [page, setPage] = useUrlNumber("page", 1);
    const [pageSize, setPageSize] = useUrlNumber("page_size", 25);
    const [selectedRunIds, setSelectedRunIds] = React.useState<string[]>([]);
    const [dateRange] = React.useState(defaultDateRange());

    const [rows, setRows] = React.useState<MetricsRunRow[]>([]);
    const [total, setTotal] = React.useState(0);
    const [summary, setSummary] = React.useState<MetricsSummaryResponse | null>(null);
    const [timeseries, setTimeseries] = React.useState<MetricsTimeseriesResponse | null>(null);
    const [filters, setFilters] = React.useState({ subsets: [] as string[], models: [] as string[] });
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);

    const load = React.useCallback(() => {
        if (!pipelineId) return;
        setLoading(true);
        setError(null);
        Promise.all([
            opsApi.getMetricsRuns(pipelineId, {
                subset: subset || undefined,
                model_id: modelId || undefined,
                search: search || undefined,
                sort_by: sortBy,
                sort_dir: sortDir as "asc" | "desc",
                limit: pageSize,
                offset: (page - 1) * pageSize,
            }),
            opsApi.getMetricsSummary(pipelineId, { subset: subset || undefined, model_id: modelId || undefined }),
            opsApi.getMetricsTimeseries(pipelineId, { metrics: ["mAP50", "f1_score", "precision", "recall"], subset: subset ? [subset] : undefined }),
        ])
            .then(([runsRes, summaryRes, tsRes]) => {
                setRows(runsRes.rows);
                setTotal(runsRes.total);
                setFilters({ subsets: runsRes.available_filters.subsets, models: runsRes.available_filters.models });
                setSummary(summaryRes);
                setTimeseries(tsRes);
            })
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, subset, modelId, search, sortBy, sortDir, page, pageSize]);

    React.useEffect(() => {
        load();
    }, [load]);

    const displayPipeline = pipelineId || "image_detection_e2e";

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Datapipe Ops", href: "/" },
                    { label: displayPipeline },
                ]}
                title="Pipeline Metrics Overview"
                subtitle="End-to-end image detection pipeline"
                statusChips={[
                    { label: "Running", variant: "success" },
                    { label: "Detection", variant: "purple" },
                ]}
                dateRange={dateRange}
                onDateRangeChange={() => undefined}
                onRefresh={load}
                primaryAction={{ label: "Compare runs", onClick: () => navigate(`/training?pipeline=${displayPipeline}`) }}
            />

            <FilterBar
                filters={[
                    { key: "subset", label: "Subset", value: subset, options: [{ label: "All", value: "" }, ...filters.subsets.map((s) => ({ label: s, value: s }))] },
                    { key: "model_id", label: "Model", value: modelId, options: [{ label: "All", value: "" }, ...filters.models.map((m) => ({ label: m, value: m }))] },
                ]}
                onFilterChange={(key, val) => {
                    const v = Array.isArray(val) ? val[0] : val;
                    if (key === "subset") setSubset(v ?? "");
                    if (key === "model_id") setModelId(v ?? "");
                    setPage(1);
                }}
                search={search}
                onSearchChange={(v) => { setSearch(v); setPage(1); }}
                searchPlaceholder="Search runs, models, tags…"
            />

            <PipelineStagesStrip pipelineId={displayPipeline} />

            <EmptyState loading={pidLoading || loading} error={error} empty={!rows.length && !loading}>
                {summary && (
                    <div className="ops-kpi-row">
                        {summary.kpis.map((kpi) => (
                            <KpiCard
                                key={kpi.key}
                                label={kpi.label}
                                value={kpi.value}
                                format={kpi.format}
                                deltaPct={kpi.delta_pct}
                                higherIsBetter={kpi.higher_is_better}
                                trend={kpi.trend}
                                subtitle={kpi.subtitle}
                            />
                        ))}
                    </div>
                )}

                <div className="ops-layout-with-rail">
                    <div>
                        <MetricsRunTable
                            rows={rows}
                            total={total}
                            page={page}
                            pageSize={pageSize}
                            selectedRunIds={selectedRunIds}
                            onPageChange={(p, ps) => { setPage(p); setPageSize(ps); }}
                            onSortChange={(sorts) => {
                                const first = sorts[0];
                                if (first) {
                                    setSortBy(first.field);
                                    setSortDir(first.direction);
                                }
                            }}
                            onSelectionChange={setSelectedRunIds}
                            onCompare={() => navigate(`/training?pipeline=${displayPipeline}`)}
                        />

                        {timeseries && (
                            <div className="ops-chart-grid">
                                {timeseries.series.map((s) => (
                                    <ChartCard
                                        key={s.key}
                                        spec={{
                                            id: s.key,
                                            title: s.label,
                                            type: "line",
                                            xLabel: "Date",
                                            yLabel: s.metric,
                                            series: [{ key: s.key, label: s.label, points: s.points.map((p) => ({ x: p.x, y: p.y })) }],
                                        }}
                                    />
                                ))}
                                <ChartCard
                                    spec={{
                                        id: "pr-scatter",
                                        title: "Precision vs Recall",
                                        type: "scatter",
                                        xLabel: "Precision",
                                        yLabel: "Recall",
                                        series: [{
                                            key: "pr",
                                            label: "Runs",
                                            points: rows.map((r) => ({ x: r.metrics.precision ?? 0, y: r.metrics.recall ?? 0 })),
                                        }],
                                    }}
                                />
                                <ChartCard
                                    spec={{
                                        id: "by-subset",
                                        title: "Key metrics by subset (latest)",
                                        type: "bar",
                                        xLabel: "Subset",
                                        yLabel: "mAP50",
                                        series: [{
                                            key: "mAP50",
                                            label: "mAP50",
                                            points: ["train", "val", "test"].map((sub) => ({
                                                x: sub,
                                                y: rows.find((r) => r.subset === sub)?.metrics.mAP50 ?? null,
                                            })),
                                        }],
                                    }}
                                />
                            </div>
                        )}
                    </div>
                    <div className="ops-rail">
                        <BestRunPanel run={summary?.best_run} />
                        <MetricLegend />
                        <AnomaliesPanel anomalies={summary?.anomalies ?? []} />
                    </div>
                </div>
            </EmptyState>
        </div>
    );
}
