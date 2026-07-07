import React from "react";
import { useSearchParams } from "react-router-dom";
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import type {
    FrozenDatasetRow,
    MetricsModelRow,
    MetricsSummaryResponse,
    MetricsTableSchema,
} from "../../../types/ops";
import {
    defaultDateRange,
    EmptyState,
    FilterBar,
    KpiCard,
    PageHeader,
    parseSortParams,
    SelectedModelChips,
} from "../shared";
import { FrozenDatasetsCompact } from "./FrozenDatasetsCompact";
import { ModelMetricsTable } from "./ModelMetricsTable";
import { buildMetricSchema, type MetricsViewMode } from "./metricsSchema";

export function MetricsOverviewPage() {
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [searchParams, setSearchParams] = useSearchParams();

    const patchParams = React.useCallback(
        (updates: Record<string, string | number | null | undefined>) => {
            setSearchParams(
                (prev) => {
                    const next = new URLSearchParams(prev);
                    for (const [key, value] of Object.entries(updates)) {
                        if (value === null || value === undefined || value === "") {
                            next.delete(key);
                        } else {
                            next.set(key, String(value));
                        }
                    }
                    return next;
                },
                { replace: true },
            );
        },
        [setSearchParams],
    );

    const subsetParam = searchParams.get("subset");
    const subset = subsetParam ?? "val";
    const apiSubset = subset === "all" ? undefined : subset;
    const taskType = searchParams.get("task_type") ?? "";
    const viewMode: MetricsViewMode = searchParams.get("view") === "all" ? "all" : "detailed";
    const modelIds = React.useMemo(
        () => (searchParams.get("model_id") ?? "").split(",").map((s) => s.trim()).filter(Boolean),
        [searchParams],
    );
    const search = searchParams.get("search") ?? "";
    const sortBy = searchParams.get("sort_by") ?? "model_id";
    const sortDir = searchParams.get("sort_dir") ?? "desc";
    const page = searchParams.get("page") ? parseInt(searchParams.get("page")!, 10) : 1;
    const pageSize = searchParams.get("page_size") ? parseInt(searchParams.get("page_size")!, 10) : 25;
    const [dateRange] = React.useState(defaultDateRange());

    const [rows, setRows] = React.useState<MetricsModelRow[]>([]);
    const [frozenDatasets, setFrozenDatasets] = React.useState<FrozenDatasetRow[]>([]);
    const [schema, setSchema] = React.useState<MetricsTableSchema | null>(null);
    const [total, setTotal] = React.useState(0);
    const [summary, setSummary] = React.useState<MetricsSummaryResponse | null>(null);
    const [filters, setFilters] = React.useState({ subsets: [] as string[], models: [] as string[] });
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);

    const load = React.useCallback(() => {
        if (!pipelineId) return;
        setLoading(true);
        setError(null);
        Promise.all([
            opsApi.getMetricsRuns(pipelineId, {
                subset: apiSubset,
                model_id: modelIds.length ? modelIds.join(",") : undefined,
                task_type: taskType || undefined,
                search: search || undefined,
                sort_by: sortBy,
                sort_dir: sortDir as "asc" | "desc",
                limit: pageSize,
                offset: (page - 1) * pageSize,
            }),
            opsApi.getMetricsSummary(pipelineId, {
                subset: apiSubset,
                model_id: modelIds.length ? modelIds.join(",") : undefined,
            }),
            opsApi.getFrozenDatasets(pipelineId),
        ])
            .then(([runsRes, summaryRes, frozenRes]) => {
                setRows(runsRes.rows);
                setTotal(runsRes.total);
                setSchema(buildMetricSchema(summaryRes.best_run?.task_type, runsRes.available_filters.metrics, runsRes.schema));
                setSummary(summaryRes);
                setFrozenDatasets(frozenRes.rows);
            })
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, apiSubset, taskType, modelIds, search, sortBy, sortDir, page, pageSize]);

    React.useLayoutEffect(() => {
        if (subsetParam !== null) return;
        patchParams({ subset: "val" });
    }, [subsetParam, patchParams]);

    React.useEffect(() => {
        if (!pipelineId) return;
        opsApi.getMetricsRuns(pipelineId, { subset: apiSubset, limit: 1 })
            .then((res) => {
                setFilters({
                    subsets: res.available_filters.subsets,
                    models: res.available_filters.models,
                });
            })
            .catch(() => undefined);
    }, [pipelineId, apiSubset]);

    React.useEffect(() => {
        load();
    }, [load]);

    const displayPipeline = pipelineId || "image_detection_e2e";
    const activeSorts = parseSortParams(sortBy, sortDir);
    const visibleKpis = (summary?.kpis ?? []).filter(
        (kpi) => kpi.key !== "weighted_recall" && kpi.key !== "accuracy",
    );
    const modelFilterOptions = React.useMemo(() => {
        const ids = new Set([...filters.models, ...modelIds]);
        return Array.from(ids).sort().map((m) => ({ label: m, value: m }));
    }, [filters.models, modelIds]);

    const removeModel = React.useCallback(
        (id: string) => {
            const next = modelIds.filter((item) => item !== id);
            patchParams({ model_id: next.length ? next.join(",") : null, page: 1 });
        },
        [modelIds, patchParams],
    );

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Datapipe Ops", href: "/" },
                    { label: displayPipeline },
                ]}
                title="Pipeline Metrics Overview"
                subtitle="Model-centric metrics across frozen datasets"
                statusChips={[
                    { label: "Running", variant: "success" },
                    { label: schema?.task_type ?? "Detection", variant: "purple" },
                ]}
                dateRange={dateRange}
                onDateRangeChange={() => undefined}
                onRefresh={load}
            />

            <div className="ops-metrics-toolbar">
                <FilterBar
                    filters={[
                        { key: "subset", label: "Subset", value: subset, options: [{ label: "All", value: "all" }, ...filters.subsets.map((s) => ({ label: s, value: s }))] },
                        {
                            key: "model_id",
                            label: "Model",
                            mode: "multiple",
                            minWidth: 360,
                            dropdownMinWidth: 360,
                            value: modelIds,
                            placeholder: "All models",
                            options: modelFilterOptions,
                        },
                        {
                            key: "task_type",
                            label: "Task",
                            value: taskType,
                            options: [
                                { label: "Auto", value: "" },
                                { label: "Detection", value: "detection" },
                                { label: "Classification", value: "classification" },
                                { label: "Segmentation", value: "segmentation" },
                                { label: "Keypoints", value: "keypoints" },
                            ],
                        },
                    ]}
                    onFilterChange={(key, val) => {
                        if (key === "subset") {
                            const v = Array.isArray(val) ? val[0] : val;
                            patchParams({ subset: v === "all" || v === "" ? "all" : (v ?? "val"), page: 1 });
                        }
                        if (key === "model_id") {
                            const ids = Array.isArray(val) ? val : val ? [val] : [];
                            patchParams({ model_id: ids.length ? ids.join(",") : null, page: 1 });
                        }
                        if (key === "task_type") {
                            const v = Array.isArray(val) ? val[0] : val;
                            patchParams({ task_type: v ?? "", page: 1 });
                        }
                    }}
                    search={search}
                    onSearchChange={(v) => patchParams({ search: v, page: 1 })}
                    searchPlaceholder="Search models, datasets, tags…"
                />
            </div>

            <SelectedModelChips modelIds={modelIds} onRemove={removeModel} />

            <EmptyState
                loading={pidLoading || loading}
                error={error}
                empty={!rows.length && !loading}
                keepChildrenWhileLoading={rows.length > 0}
            >
                {visibleKpis.length > 0 && (
                    <div className="ops-kpi-row">
                        {visibleKpis.map((kpi) => (
                            <KpiCard
                                key={kpi.key}
                                label={kpi.label}
                                value={kpi.value}
                                format={kpi.format}
                                subtitle={kpi.subtitle}
                            />
                        ))}
                    </div>
                )}

                <div className="ops-metrics-frozen-row">
                    <FrozenDatasetsCompact rows={frozenDatasets} loading={loading} pipelineId={pipelineId} />
                </div>

                {schema && (
                    <ModelMetricsTable
                        rows={rows}
                        schema={schema}
                        viewMode={viewMode}
                        onViewModeChange={(v) => patchParams({ view: v })}
                        total={total}
                        page={page}
                        pageSize={pageSize}
                        loading={loading}
                        activeSorts={activeSorts}
                        onPageChange={(p, ps) => patchParams({ page: p, page_size: ps })}
                        onSortChange={(sorts) => {
                            const first = sorts[0];
                            if (first) {
                                patchParams({ sort_by: first.field, sort_dir: first.direction, page: 1 });
                            } else {
                                patchParams({ sort_by: null, sort_dir: null, page: 1 });
                            }
                        }}
                    />
                )}
            </EmptyState>
        </div>
    );
}
