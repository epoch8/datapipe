import React from "react";
import { Link } from "react-router-dom";
import { Button, Modal, Table, Tag, notification } from "antd";
import type { ColumnsType } from "antd/es/table";
import { opsApi } from "@datapipe/ui-ml/api/client";
import { ApiError } from "@datapipe/ui/api/ops";
import type { TrainingRequestListRow } from "../../../types/opsMl";
import { EmptyState } from "../shared";
import { buildDatasetUrl, buildModelUrl } from "../shared/entityUrls";

type Props = {
    pipelineId: string;
    specId: string;
    /** Bumping this value forces a reload (e.g. after a new request is created). */
    refreshToken?: number;
};

const ACTIVE_POLL_MS = 5000;
const IDLE_POLL_MS = 15000;

function displayValue(value: unknown): React.ReactNode {
    if (value === null || value === undefined || value === "") return "—";
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

function hasActiveRequest(rows: TrainingRequestListRow[]): boolean {
    return rows.some((row) => {
        const s = String(row.status ?? row.state ?? "").toLowerCase();
        return s.includes("run") || s.includes("queue");
    });
}

function buildColumns(
    specId: string,
    onDelete: (row: TrainingRequestListRow) => void,
    deletingId: string | null,
): ColumnsType<TrainingRequestListRow> {
    return [
        {
            title: "Request",
            dataIndex: "id",
            key: "id",
            render: (value: string) => displayValue(value),
        },
        {
            title: "Experiment",
            key: "experiment",
            render: (_: unknown, row) => {
                const label = row.config_name || row.train_config_id;
                if (!row.train_config_id) return displayValue(label);
                return (
                    <Link
                        className="dp-entity-link"
                        to={`/training/${encodeURIComponent(specId)}/experiments/${encodeURIComponent(row.train_config_id)}`}
                    >
                        {label}
                    </Link>
                );
            },
        },
        {
            title: "Frozen dataset",
            dataIndex: "frozen_dataset_id",
            key: "frozen_dataset_id",
            render: (value: string | null | undefined) => {
                if (!value) return "—";
                return (
                    <Link className="dp-entity-link" to={buildDatasetUrl(value, { specId })}>
                        {value}
                    </Link>
                );
            },
        },
        {
            title: "Requested",
            dataIndex: "requested_at",
            key: "requested_at",
            render: (value: string | null | undefined) => displayValue(value),
        },
        {
            title: "Status",
            key: "status",
            render: (_: unknown, row) => {
                const value = row.status ?? row.state;
                return <Tag color={statusColor(value)}>{displayValue(value)}</Tag>;
            },
        },
        {
            title: "Run",
            dataIndex: "run_key",
            key: "run_key",
            render: (value: string | null | undefined) => {
                if (!value) return "—";
                return (
                    <Link
                        className="dp-entity-link"
                        to={`/training/${encodeURIComponent(specId)}/runs/${encodeURIComponent(value)}`}
                    >
                        {value}
                    </Link>
                );
            },
        },
        {
            title: "Model",
            dataIndex: "model_id",
            key: "model_id",
            render: (value: string | null | undefined, row) => {
                if (!value) return "—";
                return (
                    <Link
                        className="dp-entity-link"
                        to={buildModelUrl(value, {
                            specId,
                            dataset_id: row.frozen_dataset_id ?? undefined,
                        })}
                    >
                        {value}
                    </Link>
                );
            },
        },
        {
            title: "",
            key: "actions",
            width: 90,
            render: (_: unknown, row) =>
                row.can_delete ? (
                    <Button
                        danger
                        type="link"
                        size="small"
                        loading={deletingId === row.id}
                        onClick={() => onDelete(row)}
                    >
                        Delete
                    </Button>
                ) : null,
        },
    ];
}

export function TrainingRequestsTab({ pipelineId, specId, refreshToken }: Props) {
    const [rows, setRows] = React.useState<TrainingRequestListRow[]>([]);
    const [total, setTotal] = React.useState(0);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<unknown>(null);
    const [page, setPage] = React.useState(1);
    const [deletingId, setDeletingId] = React.useState<string | null>(null);
    const pageSize = 10;

    const load = React.useCallback(() => {
        if (!pipelineId || !specId) return Promise.resolve();
        setError(null);
        return opsApi
            .getTrainingRequests(pipelineId, specId, {
                limit: pageSize,
                offset: (page - 1) * pageSize,
            })
            .then((res) => {
                setRows(res.rows);
                setTotal(res.total);
            })
            .catch((e) => setError(e))
            .finally(() => setLoading(false));
    }, [pipelineId, specId, page]);

    React.useEffect(() => {
        setLoading(true);
        load();
    }, [load, refreshToken]);

    React.useEffect(() => {
        const interval = hasActiveRequest(rows) ? ACTIVE_POLL_MS : IDLE_POLL_MS;
        const timer = window.setInterval(load, interval);
        return () => window.clearInterval(timer);
    }, [rows, load]);

    const handleDelete = React.useCallback(
        (row: TrainingRequestListRow) => {
            Modal.confirm({
                title: "Delete training request?",
                content: `"${row.id}" will be permanently removed. Only allowed before launch/training.`,
                okText: "Delete",
                okButtonProps: { danger: true },
                onOk: async () => {
                    setDeletingId(row.id);
                    try {
                        await opsApi.deleteTrainingRequest(pipelineId, specId, row.id);
                        notification.success({ message: "Request deleted" });
                        await load();
                    } catch (err) {
                        notification.error({
                            message: "Delete failed",
                            description: err instanceof ApiError ? err.message : String(err),
                        });
                        throw err;
                    } finally {
                        setDeletingId(null);
                    }
                },
            });
        },
        [load, pipelineId, specId],
    );

    const tableColumns = React.useMemo(
        () => buildColumns(specId, handleDelete, deletingId),
        [specId, handleDelete, deletingId],
    );

    return (
        <div className="ops-panel ops-polished-panel ops-spec-table-panel">
            <div className="ops-spec-table-head">
                <div className="ops-panel-title">Training requests</div>
                <div className="ops-table-filter-summary">
                    Showing {rows.length} of {total} requests
                </div>
            </div>
            <EmptyState
                loading={loading}
                error={error}
                empty={!rows.length && !loading}
                keepChildrenWhileLoading
            >
                <Table
                    className="ops-table ops-spec-table"
                    size="middle"
                    columns={tableColumns}
                    dataSource={rows}
                    rowKey={(row) => row.id}
                    pagination={{
                        current: page,
                        pageSize,
                        total,
                        onChange: (nextPage) => setPage(nextPage),
                    }}
                    scroll={{ x: "max-content" }}
                />
            </EmptyState>
        </div>
    );
}
