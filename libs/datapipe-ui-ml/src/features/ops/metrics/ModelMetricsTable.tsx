import React from "react";
import { Segmented, Tooltip } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { MetricsModelRow, MetricsTableSchema } from "../../../types/opsMl";
import { SortableDataTable, type SortSpec } from "../shared";
import { EntityLink } from "./EntityLink";
import { splitSizeLabel } from "./FrozenDatasetsCompact";
import { buildMetricColumns } from "./metricTableColumns";
import type { MetricsViewMode } from "./metricsSchema";

type Props = {
    rows: MetricsModelRow[];
    schema: MetricsTableSchema;
    viewMode: MetricsViewMode;
    onViewModeChange?: (mode: MetricsViewMode) => void;
    total: number;
    page: number;
    pageSize: number;
    loading?: boolean;
    activeSorts?: SortSpec[];
    onPageChange: (page: number, pageSize: number) => void;
    onSortChange: (sorts: SortSpec[]) => void;
};

export function ModelMetricsTable({
    rows,
    schema,
    viewMode,
    onViewModeChange,
    total,
    page,
    pageSize,
    loading,
    activeSorts,
    onPageChange,
    onSortChange,
}: Props) {
    const metricColumns = React.useMemo(
        () => buildMetricColumns(schema, viewMode, rows),
        [schema, viewMode, rows],
    );

    const baseColumns: ColumnsType<MetricsModelRow> = [
        {
            title: "Model",
            dataIndex: "model_id",
            fixed: "left",
            sorter: true,
            render: (v: string, row) => (
                <EntityLink kind="model" id={v} datasetId={row.dataset_id} subset={row.subset} />
            ),
        },
        {
            title: "Dataset",
            dataIndex: "dataset_id",
            render: (v?: string, row?: MetricsModelRow) =>
                v ? (
                    <EntityLink kind="dataset" id={v} subset={row?.subset} />
                ) : (
                    <span className="ops-muted">no frozen dataset</span>
                ),
        },
        { title: "Subset", dataIndex: "subset", width: 64, sorter: true },
        {
            title: "Split",
            key: "split_size",
            width: 88,
            render: (_, row) => <Tooltip title="train / val / test">{splitSizeLabel(row)}</Tooltip>,
        },
    ];

    const columns: ColumnsType<MetricsModelRow> = [...baseColumns, ...metricColumns];

    const tableExtra = onViewModeChange ? (
        <div className="ops-metrics-view-toggle">
            <span className="ops-muted">View</span>
            <Segmented
                size="small"
                value={viewMode}
                onChange={(v) => onViewModeChange(v as MetricsViewMode)}
                options={[
                    { label: "Detailed", value: "detailed" },
                    { label: "All metrics", value: "all" },
                ]}
            />
        </div>
    ) : undefined;

    return (
        <SortableDataTable
            title="Model metrics"
            extra={tableExtra}
            className="ops-model-metrics-table"
            columns={columns}
            dataSource={rows}
            rowKey="id"
            loading={loading}
            total={total}
            page={page}
            pageSize={pageSize}
            onPageChange={onPageChange}
            onSortChange={onSortChange}
            activeSorts={activeSorts}
            scroll={{ x: "max-content" }}
        />
    );
}
