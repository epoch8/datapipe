import React from "react";
import { Button, Tag } from "antd";
import { EyeOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import { useNavigate } from "react-router-dom";
import type { RunListRow } from "../../../types/ops";
import { SortableDataTable, type SortSpec } from "../shared";
import { formatRunListStage } from "../utils/runScope";

const STATUS_COLORS: Record<string, string> = {
    completed: "success",
    failed: "error",
    running: "processing",
    pending: "default",
};

function formatDuration(durationS?: number, startedAt?: string, finishedAt?: string): string {
    if (durationS != null && durationS >= 0) {
        if (durationS < 60) return `${durationS}s`;
        const min = Math.floor(durationS / 60);
        return `${min}m ${durationS % 60}s`;
    }
    if (!startedAt || !finishedAt) return "—";
    const sec = Math.round((new Date(finishedAt).getTime() - new Date(startedAt).getTime()) / 1000);
    if (sec < 60) return `${sec}s`;
    return `${Math.floor(sec / 60)}m ${sec % 60}s`;
}

function formatTrigger(trigger?: string): string {
    if (!trigger) return "manual";
    if (trigger === "api:pipeline" || trigger.startsWith("api:stage:") || trigger === "api") return "api";
    if (trigger.includes("schedule")) return "schedule";
    return trigger;
}

type Props = {
    rows: RunListRow[];
    total: number;
    page: number;
    pageSize: number;
    loading?: boolean;
    activeSorts?: SortSpec[];
    onPageChange: (page: number, pageSize: number) => void;
    onSortChange?: (sorts: SortSpec[]) => void;
};

export function RunsTable({
    rows,
    total,
    page,
    pageSize,
    loading,
    activeSorts,
    onPageChange,
    onSortChange,
}: Props) {
    const navigate = useNavigate();

    const columns: ColumnsType<RunListRow> = [
        {
            title: "Run ID",
            dataIndex: "run_id",
            sorter: true,
            render: (runId: string) => (
                <Button type="link" className="recent-run-link" onClick={() => navigate(`/runs/${runId}`)}>
                    {runId.slice(0, 8)}…
                </Button>
            ),
        },
        {
            title: "Status",
            dataIndex: "status",
            sorter: true,
            render: (status: string) => <Tag color={STATUS_COLORS[status]}>{status}</Tag>,
        },
        {
            title: "Scope / Stage",
            key: "stage",
            sorter: true,
            render: (_, row) => formatRunListStage(row),
        },
        {
            title: "Started",
            dataIndex: "started_at",
            sorter: true,
            render: (v?: string) => v?.slice(0, 16)?.replace("T", " ") ?? "—",
        },
        {
            title: "Duration",
            dataIndex: "duration_s",
            sorter: true,
            render: (v: number | undefined, row) => formatDuration(v, row.started_at, row.finished_at),
        },
        {
            title: "Trigger",
            dataIndex: "trigger",
            render: (v?: string) => formatTrigger(v),
        },
        {
            title: "Action",
            key: "action",
            render: (_, row) => (
                <Button
                    type="text"
                    icon={<EyeOutlined />}
                    aria-label="Open run"
                    onClick={() => navigate(`/runs/${row.run_id}`)}
                />
            ),
        },
    ];

    return (
        <SortableDataTable
            columns={columns}
            dataSource={rows}
            rowKey="run_id"
            total={total}
            page={page}
            pageSize={pageSize}
            loading={loading}
            activeSorts={activeSorts}
            onPageChange={onPageChange}
            onSortChange={onSortChange}
            scroll={{ x: 900 }}
        />
    );
}
