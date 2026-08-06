import React from "react";
import type { ColumnsType } from "antd/es/table";
import type { FrozenDatasetRow } from "../../../types/opsMl";
import { MetricValue, SortableDataTable } from "../shared";

type Props = {
    rows: FrozenDatasetRow[];
    loading?: boolean;
};

export function FrozenDatasetsTable({ rows, loading }: Props) {
    const columns: ColumnsType<FrozenDatasetRow> = [
        { title: "Dataset ID", dataIndex: "dataset_id" },
        {
            title: "Frozen at",
            dataIndex: "frozen_at",
            render: (v?: string) => v?.slice(0, 16)?.replace("T", " ") ?? "—",
        },
        {
            title: "Train",
            dataIndex: "train_count",
            render: (v) => <MetricValue value={v} format="integer" />,
        },
        {
            title: "Val",
            dataIndex: "val_count",
            render: (v) => <MetricValue value={v} format="integer" />,
        },
        {
            title: "Test",
            dataIndex: "test_count",
            render: (v) => <MetricValue value={v} format="integer" />,
        },
    ];

    return (
        <SortableDataTable
            title="Frozen datasets"
            columns={columns}
            dataSource={rows}
            rowKey="dataset_id"
            loading={loading}
            total={rows.length}
            page={1}
            pageSize={Math.max(rows.length, 1)}
            onPageChange={() => undefined}
            scroll={{ x: 900 }}
        />
    );
}
