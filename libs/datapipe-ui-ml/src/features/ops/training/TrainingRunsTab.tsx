import React from "react";
import { Link } from "react-router-dom";
import { Table, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import { opsApi } from "@datapipe/ui-ml/api/client";
import type { OpsColumn, OpsRowsResponse, OpsSpecDetail } from "../../../types/opsSpecs";
import { EmptyState } from "../shared";

type Row = Record<string, unknown>;

type Props = {
    pipelineId: string;
    specId: string;
    spec: OpsSpecDetail;
    /** Bumping this value forces a reload (e.g. after a new run is launched). */
    refreshToken?: number;
};

const ACTIVE_POLL_MS = 5000;
const IDLE_POLL_MS = 15000;

function displayValue(value: unknown): React.ReactNode {
    if (value === null || value === undefined || value === "") return "—";
    if (typeof value === "number") {
        return Number.isInteger(value) ? value.toLocaleString() : value.toFixed(3);
    }
    if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}T/.test(value)) {
        return value.slice(0, 16).replace("T", " ");
    }
    return String(value);
}

function statusColor(value: unknown): string {
    const s = String(value ?? "").toLowerCase();
    if (s.includes("fail") || s.includes("error")) return "red";
    if (s.includes("run") || s.includes("queue")) return "blue";
    if (s.includes("success") || s.includes("complete")) return "green";
    return "default";
}

function formatDuration(value: unknown): React.ReactNode {
    const seconds = Number(value);
    if (!Number.isFinite(seconds)) return displayValue(value);
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    const rest = seconds % 60;
    return rest ? `${minutes}m ${rest}s` : `${minutes}m`;
}

function buildColumns(columns: OpsColumn[], specId: string): ColumnsType<Row> {
    return columns.map((column) => ({
        title: column.label,
        dataIndex: column.source,
        key: column.id,
        width: column.width ?? undefined,
        render: (value: unknown) => {
            if (column.kind === "status") {
                return <Tag color={statusColor(value)}>{displayValue(value)}</Tag>;
            }
            if (column.kind === "chip") {
                return <Tag className="ops-soft-chip">{displayValue(value)}</Tag>;
            }
            if (column.kind === "duration") return formatDuration(value);
            if (column.link_to === "training_run" && value != null && value !== "") {
                return (
                    <Link
                        className="dp-entity-link"
                        to={`/training/${encodeURIComponent(specId)}/runs/${encodeURIComponent(String(value))}`}
                    >
                        {String(value)}
                    </Link>
                );
            }
            return displayValue(value);
        },
    }));
}

function hasActiveRun(rows: Row[], statusSource?: string): boolean {
    if (!statusSource) return false;
    return rows.some((row) => {
        const s = String(row[statusSource] ?? "").toLowerCase();
        return s.includes("run") || s.includes("queue");
    });
}

export function TrainingRunsTab({ pipelineId, specId, spec, refreshToken }: Props) {
    const columns = React.useMemo(() => spec.training?.columns ?? [], [spec]);
    const statusSource = React.useMemo(
        () => columns.find((column) => column.kind === "status")?.source,
        [columns],
    );
    const tableColumns = React.useMemo(() => buildColumns(columns, specId), [columns, specId]);

    const [data, setData] = React.useState<OpsRowsResponse | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<unknown>(null);
    const [page, setPage] = React.useState(1);
    const pageSize = 10;

    const load = React.useCallback(() => {
        if (!pipelineId || !specId) return Promise.resolve();
        setError(null);
        return opsApi
            .getOpsTrainingRows(pipelineId, specId, {
                limit: pageSize,
                offset: (page - 1) * pageSize,
            })
            .then((res) => setData(res))
            .catch((e) => setError(e))
            .finally(() => setLoading(false));
    }, [pipelineId, specId, page]);

    React.useEffect(() => {
        setLoading(true);
        load();
    }, [load, refreshToken]);

    // Adaptive polling: fast while a run is queued/running, slow otherwise.
    React.useEffect(() => {
        const rows = data?.rows ?? [];
        const interval = hasActiveRun(rows, statusSource) ? ACTIVE_POLL_MS : IDLE_POLL_MS;
        const timer = window.setInterval(load, interval);
        return () => window.clearInterval(timer);
    }, [data, statusSource, load]);

    return (
        <div className="ops-panel ops-polished-panel ops-spec-table-panel">
            <div className="ops-spec-table-head">
                <div className="ops-panel-title">Training runs</div>
                {data ? (
                    <div className="ops-table-filter-summary">
                        Showing {data.rows.length} of {data.total} runs
                    </div>
                ) : null}
            </div>
            <EmptyState
                loading={loading}
                error={error}
                empty={!data?.rows.length && !loading}
                keepChildrenWhileLoading
            >
                <Table
                    className="ops-table ops-spec-table"
                    size="middle"
                    columns={tableColumns}
                    dataSource={data?.rows ?? []}
                    rowKey={(row, i) => String(row[columns[0]?.source ?? ""] ?? i)}
                    pagination={{
                        current: page,
                        pageSize,
                        total: data?.total ?? 0,
                        onChange: (nextPage) => setPage(nextPage),
                    }}
                    scroll={{ x: "max-content" }}
                />
            </EmptyState>
        </div>
    );
}
