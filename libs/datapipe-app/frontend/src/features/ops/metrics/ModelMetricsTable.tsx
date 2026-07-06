import React from "react";
import { Button, Drawer, Segmented, Tooltip } from "antd";
import { CopyOutlined } from "@ant-design/icons";
import type { ColumnsType } from "antd/es/table";
import type { MetricsModelRow, MetricsTableSchema } from "../../../types/ops";
import { SortableDataTable, type SortSpec } from "../shared";
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
    selectedRowIds: string[];
    onPageChange: (page: number, pageSize: number) => void;
    onSortChange: (sorts: SortSpec[]) => void;
    onSelectionChange: (ids: string[]) => void;
    onCompute?: (rowId: string) => void;
};

const SOURCE_LABELS: Record<string, string> = {
    manual: "manual candidate",
    artifact: "artifact",
    training_run: "training run",
    registry: "registry",
};

function truncateId(id: string, max = 24): string {
    if (!id || id.length <= max) return id || "—";
    return `${id.slice(0, max)}…`;
}

function copyText(text: string) {
    void navigator.clipboard?.writeText(text);
}

function modelStateHint(row: MetricsModelRow): React.ReactNode {
    if (row.metrics_state === "running" || row.metrics_state === "queued") {
        return <div className="ops-model-hint">computing metrics…</div>;
    }
    if (row.metrics_state === "failed") {
        return <div className="ops-model-hint">metrics failed</div>;
    }
    if (!row.has_metrics) {
        return <div className="ops-model-hint">metrics not computed</div>;
    }
    return null;
}

function renderActions(row: MetricsModelRow, onCompute?: (rowId: string) => void, onDetails?: () => void) {
    if (row.metrics_state === "running" || row.metrics_state === "queued") {
        return <span className="ops-muted">Computing…</span>;
    }
    return (
        <div className="ops-table-actions">
            {onDetails && (
                <Button type="link" size="small" onClick={onDetails}>
                    Details
                </Button>
            )}
            {row.metrics_state === "failed" && onCompute && (
                <Button type="link" size="small" onClick={() => onCompute(row.id)}>
                    Retry metrics
                </Button>
            )}
            {!row.has_metrics && row.metrics_state !== "failed" && onCompute && (
                <Button type="link" size="small" onClick={() => onCompute(row.id)}>
                    Compute metrics
                </Button>
            )}
            {row.has_metrics && row.metrics_state !== "failed" && onCompute && (
                <Button type="link" size="small" onClick={() => onCompute(row.id)}>
                    Recompute
                </Button>
            )}
        </div>
    );
}

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
    selectedRowIds,
    onPageChange,
    onSortChange,
    onSelectionChange,
    onCompute,
}: Props) {
    const [detailRow, setDetailRow] = React.useState<MetricsModelRow | null>(null);

    const metricColumns = React.useMemo(
        () => buildMetricColumns(schema, viewMode, rows),
        [schema, viewMode, rows],
    );

    const baseColumns: ColumnsType<MetricsModelRow> = [
        {
            title: "Model",
            dataIndex: "model_id",
            fixed: "left",
            width: 220,
            sorter: true,
            render: (v: string, row) => (
                <div className="ops-model-cell">
                    <Tooltip title={v}>
                        <span className="ops-truncate-id">{truncateId(v, 36)}</span>
                    </Tooltip>
                    {row.model_source && (
                        <div className="ops-model-hint">{SOURCE_LABELS[row.model_source] ?? row.model_source}</div>
                    )}
                    {modelStateHint(row)}
                </div>
            ),
        },
        {
            title: "Dataset",
            dataIndex: "dataset_id",
            width: 200,
            render: (v?: string) =>
                v ? (
                    <span className="ops-dataset-cell">
                        <Tooltip title={v}>
                            <span className="ops-truncate-id">{truncateId(v, 32)}</span>
                        </Tooltip>
                        <Button type="text" size="small" icon={<CopyOutlined />} onClick={() => copyText(v)} />
                    </span>
                ) : (
                    "—"
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

    const columns: ColumnsType<MetricsModelRow> = [
        ...baseColumns,
        ...metricColumns,
        ...(viewMode === "detailed"
            ? [
                  {
                      title: "Actions",
                      key: "actions",
                      width: 130,
                      fixed: "right" as const,
                      render: (_: unknown, row: MetricsModelRow) =>
                          renderActions(row, onCompute, () => setDetailRow(row)),
                  },
              ]
            : []),
    ];

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
        <>
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
                rowSelection={
                    viewMode === "detailed"
                        ? {
                              selectedRowKeys: selectedRowIds,
                              onChange: (keys) => onSelectionChange(keys as string[]),
                          }
                        : undefined
                }
                scroll={{ x: viewMode === "all" ? 2200 : 1400 }}
            />

            <Drawer
                title={detailRow?.model_id ?? "Model details"}
                visible={!!detailRow}
                onClose={() => setDetailRow(null)}
                width={400}
            >
                {detailRow && (
                    <dl className="ops-detail-list">
                        <dt>Dataset</dt>
                        <dd>{detailRow.dataset_id ?? "—"}</dd>
                        <dt>Subset</dt>
                        <dd>{detailRow.subset}</dd>
                        <dt>Task</dt>
                        <dd>{detailRow.task_type ?? "—"}</dd>
                        <dt>Source</dt>
                        <dd>{detailRow.model_source ?? "—"}</dd>
                        <dt>Run ID</dt>
                        <dd>{detailRow.run_id ?? "—"}</dd>
                        <dt>Started at</dt>
                        <dd>{detailRow.started_at?.slice(0, 16)?.replace("T", " ") ?? "—"}</dd>
                        <dt>Duration</dt>
                        <dd>{detailRow.duration_s ? `${Math.floor(detailRow.duration_s / 60)}m ${detailRow.duration_s % 60}s` : "—"}</dd>
                        <dt>Status</dt>
                        <dd>{detailRow.status ?? "—"}</dd>
                    </dl>
                )}
            </Drawer>
        </>
    );
}
