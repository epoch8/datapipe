import React from "react";
import { Tabs } from "antd";
import { useNavigate } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import { useUrlState } from "../../../hooks/useUrlState";
import type { PipelineDetail, RunListRow } from "../../../types/ops";
import { ApiErrorAlert } from "../../../components/ApiErrorAlert";
import { RunStepsDropdown } from "../components/RunStepsDropdown";
import { EmptyState, FilterBar, PageHeader, parseSortParams } from "../shared";
import { prependRecentRun } from "../utils/recentRuns";
import { RunsTable } from "./RunsTable";

const PAGE_SIZE = 25;

export function RunsPage() {
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const navigate = useNavigate();
    const [search, setSearch] = useUrlState("search");
    const [statusFilter, setStatusFilter] = useUrlState("status");
    const [stageFilter, setStageFilter] = useUrlState("stage");
    const [triggerFilter, setTriggerFilter] = useUrlState("trigger");
    const [page, setPage] = React.useState(1);
    const [sortBy, setSortBy] = useUrlState("sort_by", "started_at");
    const [sortDir, setSortDir] = useUrlState("sort_dir", "desc");

    const [rows, setRows] = React.useState<RunListRow[]>([]);
    const [total, setTotal] = React.useState(0);
    const [filters, setFilters] = React.useState({ statuses: [] as string[], stages: [] as string[], triggers: [] as string[] });
    const [countsByStatus, setCountsByStatus] = React.useState<Record<string, number>>({});
    const [pipeline, setPipeline] = React.useState<PipelineDetail | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<unknown>(null);

    const loadPipeline = React.useCallback(() => {
        if (!pipelineId) return;
        opsApi.getPipeline(pipelineId).then(setPipeline).catch(() => undefined);
    }, [pipelineId]);

    const loadRuns = React.useCallback(() => {
        if (!pipelineId) return;
        setLoading(true);
        opsApi
            .getRuns({
                pipeline_id: pipelineId,
                search: search || undefined,
                status: statusFilter || undefined,
                stage: stageFilter || undefined,
                trigger: triggerFilter || undefined,
                limit: PAGE_SIZE,
                offset: (page - 1) * PAGE_SIZE,
                sort_by: (sortBy as "started_at" | "duration" | "status" | "stage") || "started_at",
                sort_dir: (sortDir as "asc" | "desc") || "desc",
            })
            .then((res) => {
                setRows(res.rows);
                setTotal(res.total);
                setFilters(res.filters);
                setCountsByStatus(res.counts_by_status ?? {});
            })
            .catch((e) => setError(e))
            .finally(() => setLoading(false));
    }, [pipelineId, search, statusFilter, stageFilter, triggerFilter, page, sortBy, sortDir]);

    React.useEffect(() => {
        loadPipeline();
        loadRuns();
        const timer = setInterval(() => {
            loadRuns();
        }, getRefreshIntervalMs());
        return () => clearInterval(timer);
    }, [loadPipeline, loadRuns]);

    const runStage = (labels: [string, string][]) => {
        opsApi
            .startRun(labels)
            .then((started) => {
                setPipeline((current) =>
                    current
                        ? { ...current, recent_runs: prependRecentRun(current.recent_runs, started) }
                        : current,
                );
                navigate(`/runs/${started.run_id}`);
            })
            .catch((e) => setError(e));
    };

    const statusTabs = [
        { key: "", label: "All", count: Object.values(countsByStatus).reduce((a, b) => a + b, 0) || total },
        { key: "running", label: "Running", count: countsByStatus.running ?? 0 },
        { key: "completed", label: "Completed", count: countsByStatus.completed ?? 0 },
        { key: "failed", label: "Failed", count: countsByStatus.failed ?? 0 },
    ];

    if (error) return <ApiErrorAlert error={error} />;

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[{ label: "Overview", href: "/" }, { label: "Runs" }]}
                title="Runs"
                subtitle="All pipeline executions and stage runs"
                onRefresh={loadRuns}
                extra={
                    pipeline?.agent_mode ? (
                        <RunStepsDropdown stages={pipeline.stages} onStart={runStage} />
                    ) : undefined
                }
            />

            <FilterBar
                filters={[
                    {
                        key: "status",
                        label: "Status",
                        value: statusFilter || undefined,
                        options: filters.statuses.map((s) => ({ label: s, value: s })),
                    },
                    {
                        key: "stage",
                        label: "Stage",
                        value: stageFilter || undefined,
                        options: filters.stages.map((s) => ({ label: s, value: s })),
                    },
                    {
                        key: "trigger",
                        label: "Trigger",
                        value: triggerFilter || undefined,
                        options: filters.triggers.map((t) => ({ label: t, value: t })),
                    },
                ]}
                onFilterChange={(key, value) => {
                    const next = typeof value === "string" ? value : undefined;
                    if (key === "status") setStatusFilter(next ?? "");
                    if (key === "stage") setStageFilter(next ?? "");
                    if (key === "trigger") setTriggerFilter(next ?? "");
                    setPage(1);
                }}
                search={search}
                onSearchChange={(value) => {
                    setSearch(value);
                    setPage(1);
                }}
                searchPlaceholder="Search run id…"
            />

            <Tabs
                className="ops-runs-status-tabs"
                activeKey={statusFilter || ""}
                onChange={(key) => {
                    setStatusFilter(key);
                    setPage(1);
                }}
            >
                {statusTabs.map((tab) => (
                    <Tabs.TabPane tab={`${tab.label} ${tab.count}`} key={tab.key} />
                ))}
            </Tabs>

            <EmptyState loading={pidLoading || loading} error={error ? String(error) : null}>
                <RunsTable
                    rows={rows}
                    total={total}
                    page={page}
                    pageSize={PAGE_SIZE}
                    loading={loading}
                    activeSorts={parseSortParams(sortBy, sortDir)}
                    onPageChange={(nextPage) => setPage(nextPage)}
                    onSortChange={(sorts) => {
                        const primary = sorts[0];
                        setSortBy(primary?.field ?? "started_at");
                        setSortDir(primary?.direction ?? "desc");
                    }}
                />
            </EmptyState>
        </div>
    );
}
