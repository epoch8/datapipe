import React from "react";
import { Table } from "antd";
import { Link } from "react-router-dom";
import type { ColumnsType } from "antd/es/table";
import type { TrainingExperimentRow } from "../../../types/opsMl";
import { TrainingExperimentStatusTag, formatExperimentParams } from "./TrainingExperimentStatusTag";

type Props = {
    rows: TrainingExperimentRow[];
    loading?: boolean;
    selectedId?: string;
    specId: string;
    onSelect: (row: TrainingExperimentRow) => void;
};

function fmtDate(value?: string | null): string {
    if (!value) return "—";
    return value.slice(0, 16).replace("T", " ");
}

export function TrainingExperimentsTable({ rows, loading, selectedId, specId, onSelect }: Props) {
    const columns: ColumnsType<TrainingExperimentRow> = [
        {
            title: "Experiment",
            dataIndex: "display_name",
            key: "display_name",
            render: (_value, row) => (
                <div>
                    <Link
                        className="dp-entity-link"
                        to={`/training/${encodeURIComponent(specId)}/experiments/${encodeURIComponent(row.id)}`}
                        onClick={(event) => event.stopPropagation()}
                    >
                        {row.display_name ?? row.id}
                    </Link>
                    <div className="ops-muted" style={{ fontSize: 12 }}>
                        {row.source}
                    </div>
                </div>
            ),
        },
        {
            title: "Status",
            key: "status",
            width: 120,
            render: (_value, row) => <TrainingExperimentStatusTag row={row} />,
        },
        {
            title: "Main params",
            key: "params",
            render: (_value, row) => <span className="te-params">{formatExperimentParams(row)}</span>,
        },
        {
            title: "Requests",
            dataIndex: "requests_count",
            key: "requests_count",
            width: 100,
        },
        {
            title: "Runs",
            dataIndex: "runs_count",
            key: "runs_count",
            width: 80,
        },
        {
            title: "Updated",
            dataIndex: "updated_at",
            key: "updated_at",
            width: 160,
            render: (value: string | null | undefined) => fmtDate(value),
        },
    ];

    return (
        <Table
            className="ops-table ops-spec-table"
            size="middle"
            rowKey="id"
            loading={loading}
            columns={columns}
            dataSource={rows}
            pagination={false}
            rowClassName={(row) => (row.id === selectedId ? "ops-selected-row" : "")}
            onRow={(row) => ({ onClick: () => onSelect(row) })}
            scroll={{ x: "max-content" }}
        />
    );
}
