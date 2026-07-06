import React from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { Tag } from "antd";
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import type { FrozenDatasetRow, MetricsRunRow, MetricsSummaryResponse } from "../../../types/ops";
import {
    defaultDateRange,
    EmptyState,
    FilterBar,
    KpiCard,
    PageHeader,
    parseSortParams,
} from "../shared";
import { BestModelPanel } from "./MetricsPanels";
import { FrozenDatasetsTable } from "./FrozenDatasetsTable";
import { MetricsRunTable } from "./MetricsRunTable";

export function MetricsOverviewPage() {
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const navigate = useNavigate();
    const [searchParams, setSearchParams] = useSearchParams();

    // All URL params are updated through a single atomic patch. Calling several
    // independent useSearchParams setters in one tick makes each read the same
    // stale snapshot and clobber the others (only the last one survives), which
    // is why sorting/filter+page updates silently dropped params before.
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

    const subset = searchParams.get("subset") ?? "";
    const modelIds = React.useMemo(
        () => (searchParams.get("model_id") ?? "").split(",").map((s) => s.trim()).filter(Boolean),
        [searchParams],
    );
    const search = searchParams.get("search") ?? "";
    const sortBy = searchParams.get("sort_by") ?? "started_at";
    const sortDir = searchParams.get("sort_dir") ?? "desc";
    const page = searchParams.get("page") ? parseInt(searchParams.get("page")!, 10) : 1;
    const pageSize = searchParams.get("page_size") ? parseInt(searchParams.get("page_size")!, 10) : 25;
    const [selectedRunIds, setSelectedRunIds] = React.useState<string[]>([]);
    const [dateRange] = React.useState(defaultDateRange());

    const [rows, setRows] = React.useState<MetricsRunRow[]>([]);
    const [frozenDatasets, setFrozenDatasets] = React.useState<FrozenDatasetRow[]>([]);
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
                subset: subset || undefined,
                model_id: modelIds.length ? modelIds.join(",") : undefined,
                search: search || undefined,
                sort_by: sortBy,
                sort_dir: sortDir as "asc" | "desc",
                limit: pageSize,
                offset: (page - 1) * pageSize,
            }),
            opsApi.getMetricsSummary(pipelineId, {
                subset: subset || undefined,
                model_id: modelIds.length ? modelIds.join(",") : undefined,
            }),
            opsApi.getFrozenDatasets(pipelineId),
        ])
            .then(([runsRes, summaryRes, frozenRes]) => {
                setRows(runsRes.rows);
                setTotal(runsRes.total);
                setFilters({ subsets: runsRes.available_filters.subsets, models: runsRes.available_filters.models });
                setSummary(summaryRes);
                setFrozenDatasets(frozenRes.rows);
            })
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, subset, modelIds, search, sortBy, sortDir, page, pageSize]);

    React.useEffect(() => {
        load();
    }, [load]);

    // Default subset to test (or val) once on first load when URL has no subset param.
    const subsetDefaultApplied = React.useRef(false);
    React.useEffect(() => {
        if (subsetDefaultApplied.current) return;
        if (searchParams.has("subset")) {
            subsetDefaultApplied.current = true;
            return;
        }
        const { subsets } = filters;
        if (!subsets.length) return;
        const preferred = subsets.includes("test") ? "test" : subsets.includes("val") ? "val" : "";
        if (preferred) patchParams({ subset: preferred });
        subsetDefaultApplied.current = true;
    }, [filters.subsets, searchParams, patchParams]);

    const displayPipeline = pipelineId || "image_detection_e2e";
    const activeSorts = parseSortParams(sortBy, sortDir);
    const visibleKpis = (summary?.kpis ?? []).filter(
        (kpi) => kpi.key !== "weighted_recall" && kpi.key !== "accuracy",
    );

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
                    {
                        key: "model_id",
                        label: "Model",
                        mode: "multiple",
                        minWidth: 560,
                        dropdownMinWidth: 560,
                        value: modelIds,
                        placeholder: "All models",
                        options: filters.models.map((m) => ({ label: m, value: m })),
                    },
                ]}
                onFilterChange={(key, val) => {
                    if (key === "subset") {
                        const v = Array.isArray(val) ? val[0] : val;
                        patchParams({ subset: v ?? "", page: 1 });
                    }
                    if (key === "model_id") {
                        const ids = Array.isArray(val) ? val : val ? [val] : [];
                        patchParams({ model_id: ids.length ? ids.join(",") : null, page: 1 });
                    }
                }}
                search={search}
                onSearchChange={(v) => patchParams({ search: v, page: 1 })}
                searchPlaceholder="Search runs, models, tags…"
            />

            {modelIds.length > 0 && (
                <div className="ops-selected-filters">
                    <span className="ops-selected-filters-label">Selected models</span>
                    {modelIds.map((id) => (
                        <Tag
                            key={id}
                            closable
                            className="ops-selected-filter-tag"
                            onClose={() => {
                                const next = modelIds.filter((item) => item !== id);
                                patchParams({ model_id: next.length ? next.join(",") : null, page: 1 });
                            }}
                        >
                            {id}
                        </Tag>
                    ))}
                </div>
            )}

            <EmptyState
                loading={pidLoading || loading}
                error={error}
                empty={!rows.length && !loading}
                keepChildrenWhileLoading={rows.length > 0 || summary != null}
            >
                {summary?.best_run && <BestModelPanel run={summary.best_run} />}

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

                <div>
                    <FrozenDatasetsTable rows={frozenDatasets} loading={loading} />
                    <div style={{ marginTop: 16 }}>
                        <MetricsRunTable
                            rows={rows}
                            total={total}
                            page={page}
                            pageSize={pageSize}
                            activeSorts={activeSorts}
                            selectedRunIds={selectedRunIds}
                            onPageChange={(p, ps) => patchParams({ page: p, page_size: ps })}
                            onSortChange={(sorts) => {
                                const first = sorts[0];
                                if (first) {
                                    patchParams({ sort_by: first.field, sort_dir: first.direction, page: 1 });
                                } else {
                                    patchParams({ sort_by: null, sort_dir: null, page: 1 });
                                }
                            }}
                            onSelectionChange={setSelectedRunIds}
                            onCompare={() => navigate(`/training?pipeline=${displayPipeline}`)}
                        />
                    </div>
                </div>
            </EmptyState>
        </div>
    );
}
