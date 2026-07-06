import React from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { Button, Table } from "antd";
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import type { MetricsModelRow, MetricsModelDetailResponse, MetricsTableSchema } from "../../../types/ops";
import { EmptyState, PageHeader } from "../shared";
import { EntityLink } from "./EntityLink";
import { MetricKpiStrip } from "./MetricKpiStrip";
import { SourceRecordCard } from "./SourceRecordCard";
import { buildMetricColumns } from "./metricTableColumns";
import { buildMetricSchema } from "./metricsSchema";
import { buildMetricsUrl } from "./entityUrls";
import { splitSizeLabel } from "./FrozenDatasetsCompact";

export function ModelDetailPage() {
    const { modelId: rawModelId = "" } = useParams<{ modelId: string }>();
    const modelId = decodeURIComponent(rawModelId);
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [searchParams] = useSearchParams();
    const datasetId = searchParams.get("dataset_id") ?? undefined;
    const subset = searchParams.get("subset") ?? undefined;

    const [data, setData] = React.useState<MetricsModelDetailResponse | null>(null);
    const [schema, setSchema] = React.useState<MetricsTableSchema | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [metricsView, setMetricsView] = React.useState<"all" | "detailed">("detailed");

    const load = React.useCallback(() => {
        if (!pipelineId || !modelId) return;
        setLoading(true);
        setError(null);
        opsApi
            .getModelDetail(pipelineId, modelId, { dataset_id: datasetId, subset })
            .then((res) => {
                setData(res);
                const keys = Array.from(new Set(res.metrics_rows.flatMap((r) => Object.keys(r.metrics ?? {}))));
                setSchema(buildMetricSchema(res.model_row?.task_type, keys));
            })
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, modelId, datasetId, subset]);

    React.useEffect(() => {
        load();
    }, [load]);

    const metricColumns = React.useMemo(() => {
        if (!schema || !data) return [];
        return buildMetricColumns(schema, metricsView === "all" ? "all" : "detailed", data.metrics_rows);
    }, [schema, data, metricsView]);

    const columns = React.useMemo(
        () => [
            { title: "Subset", dataIndex: "subset", width: 72 },
            {
                title: "Split",
                key: "split",
                width: 88,
                render: (_: unknown, row: MetricsModelRow) => splitSizeLabel(row),
            },
            ...metricColumns,
        ],
        [metricColumns],
    );

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Datapipe Ops", href: "/" },
                    { label: pipelineId || "pipeline" },
                    { label: "Metrics", href: buildMetricsUrl(pipelineId || undefined) },
                    { label: "Models" },
                    { label: modelId },
                ]}
                title={`Model: ${modelId}`}
                subtitle="Model detail page with source record, linked frozen dataset, and available metrics."
                statusChips={[
                    { label: "trained", variant: "success" },
                    ...(data?.model_row?.task_type ? [{ label: data.model_row.task_type, variant: "purple" as const }] : []),
                    ...(subset ? [{ label: subset, variant: "default" as const }] : []),
                ]}
                extra={
                    <>
                        <Button href={data?.source_table_url ?? undefined} disabled={!data?.source_table_url}>
                            Open model row
                        </Button>
                        <Link to={buildMetricsUrl(pipelineId || undefined)}>
                            <Button>Back to Metrics</Button>
                        </Link>
                    </>
                }
                onRefresh={load}
            />

            <EmptyState loading={pidLoading || loading} error={error} empty={!data}>
                {data && (
                    <>
                        <MetricKpiStrip items={data.kpis} />

                        <div className="ops-entity-layout">
                            <SourceRecordCard
                                title="Source model record"
                                record={data.source_record}
                                preferredFields={[
                                    "model_id",
                                    "task",
                                    "task_type",
                                    "created_at",
                                    "run_key",
                                    "checkpoint",
                                    "train_subset",
                                    "frozen_dataset_id",
                                ]}
                                sourceTable={data.source_table}
                                sourceTableUrl={data.source_table_url}
                                pipelineId={pipelineId}
                            />

                            <div className="ops-panel ops-entity-summary-panel">
                                <div className="ops-panel-title">Linked frozen dataset</div>
                                {data.frozen_dataset ? (
                                    <>
                                        <EntityLink kind="dataset" id={data.frozen_dataset.dataset_id} />
                                        <dl className="ops-source-record-dl ops-entity-summary-dl">
                                            <dt>Frozen at</dt>
                                            <dd>{data.frozen_dataset.frozen_at ?? "—"}</dd>
                                            <dt>Split</dt>
                                            <dd>
                                                {data.frozen_dataset.train_count ?? 0} / {data.frozen_dataset.val_count ?? 0} /{" "}
                                                {data.frozen_dataset.test_count ?? 0}
                                            </dd>
                                        </dl>
                                        {data.frozen_dataset_source_table && (
                                            <Button
                                                type="link"
                                                size="small"
                                                href={
                                                    data.frozen_dataset_source_table && pipelineId
                                                        ? `/pipelines/${encodeURIComponent(pipelineId)}/tables/${encodeURIComponent(data.frozen_dataset_source_table)}`
                                                        : undefined
                                                }
                                            >
                                                Open dataset table
                                            </Button>
                                        )}
                                    </>
                                ) : (
                                    <div className="ops-muted">No linked frozen dataset.</div>
                                )}
                            </div>
                        </div>

                        <div className="ops-data-table ops-entity-metrics-table ops-model-detail-metrics-table">
                            <div className="ops-data-table-toolbar">
                                <div className="ops-data-table-title">Metrics for this model</div>
                                <div className="ops-metrics-view-toggle">
                                    <Button
                                        size="small"
                                        type={metricsView === "detailed" ? "primary" : "default"}
                                        onClick={() => setMetricsView("detailed")}
                                    >
                                        Detailed
                                    </Button>
                                    <Button
                                        size="small"
                                        type={metricsView === "all" ? "primary" : "default"}
                                        onClick={() => setMetricsView("all")}
                                    >
                                        All metrics
                                    </Button>
                                </div>
                            </div>
                            <Table
                                className="ops-table"
                                size="middle"
                                rowKey="id"
                                columns={columns}
                                dataSource={data.metrics_rows}
                                pagination={false}
                                scroll={{ x: "max-content" }}
                            />
                        </div>
                    </>
                )}
            </EmptyState>
        </div>
    );
}
