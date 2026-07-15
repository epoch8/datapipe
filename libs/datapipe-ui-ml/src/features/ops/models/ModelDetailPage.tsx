import React from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { Button, Table } from "antd";
import { opsApi } from "@datapipe/ui-ml/api/client";
import { usePipelineId } from "@datapipe/ui/hooks/usePipelineId";
import type { MetricsModelRow, MetricsModelDetailResponse, MetricsTableSchema } from "../../../types/opsMl";
import type { OpsSpecDetail } from "../../../types/opsSpecs";
import { EmptyState, PageHeader } from "../shared";
import { EntityLink, MetricKpiStrip, SourceRecordCard, splitSizeLabel } from "../shared";
import { buildMetricColumns } from "../metrics/metricTableColumns";
import { buildMetricSchema } from "../metrics/metricsSchema";
import { buildMetricsUrl } from "../shared/entityUrls";
import { ImageRecordsTable } from "../images/ImageRecordsTable";
import { modelHighlightFields } from "./modelRecordFields";

function resolveLinkedDatasetId(data: MetricsModelDetailResponse): string | undefined {
    if (data.frozen_dataset?.dataset_id) return data.frozen_dataset.dataset_id;
    if (data.related?.dataset_id) return data.related.dataset_id;
    if (!data.source_record) return undefined;
    for (const [key, value] of Object.entries(data.source_record)) {
        if (key.endsWith("_frozen_dataset_id") && typeof value === "string" && value) {
            return value;
        }
    }
    return undefined;
}

export function ModelDetailPage() {
    const { modelId: rawModelId = "", entityId = "", specId = "" } = useParams<{
        modelId?: string;
        entityId?: string;
        specId?: string;
    }>();
    const modelId = decodeURIComponent(rawModelId || entityId);
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [searchParams] = useSearchParams();
    const datasetId = searchParams.get("dataset_id") ?? undefined;
    const subset = searchParams.get("subset") ?? undefined;

    const [data, setData] = React.useState<MetricsModelDetailResponse | null>(null);
    const [spec, setSpec] = React.useState<OpsSpecDetail | null>(null);
    const [schema, setSchema] = React.useState<MetricsTableSchema | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [metricsView, setMetricsView] = React.useState<"all" | "detailed">("detailed");

    const load = React.useCallback(() => {
        if (!pipelineId || !modelId) return;
        setLoading(true);
        setError(null);
        Promise.all([
            opsApi.getModelDetail(pipelineId, modelId, { dataset_id: datasetId, subset }),
            specId ? opsApi.getOpsSpec(pipelineId, specId) : Promise.resolve(null),
        ])
            .then(([res, specDetail]) => {
                setData(res);
                setSpec(specDetail);
                const keys = Array.from(new Set(res.metrics_rows.flatMap((r) => Object.keys(r.metrics ?? {}))));
                setSchema(buildMetricSchema(res.model_row?.task_type, keys));
            })
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, modelId, datasetId, subset, specId]);

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

    const linkedDatasetId = data ? resolveLinkedDatasetId(data) : undefined;

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={
                    specId
                        ? [
                              { label: "Datapipe Ops", href: "/" },
                              { label: "Metrics", href: "/metrics" },
                              { label: specId, href: `/metrics/${encodeURIComponent(specId)}` },
                              { label: modelId },
                          ]
                        : [
                              { label: "Datapipe Ops", href: "/" },
                              { label: pipelineId || "pipeline" },
                              { label: "Metrics", href: buildMetricsUrl(pipelineId || undefined) },
                              { label: "Models" },
                              { label: modelId },
                          ]
                }
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
                        <Link to={specId ? `/metrics/${encodeURIComponent(specId)}` : buildMetricsUrl(pipelineId || undefined)}>
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
                                sourcePk={data.source_pk}
                                highlightFields={spec ? modelHighlightFields(spec) : []}
                                sourceTable={data.source_table}
                                sourceTableUrl={data.source_table_url}
                                pipelineId={pipelineId}
                            />

                            <div className="ops-panel ops-entity-summary-panel">
                                <div className="ops-panel-title">Linked frozen dataset</div>
                                {linkedDatasetId ? (
                                    <>
                                        <EntityLink kind="dataset" id={linkedDatasetId} />
                                        <dl className="ops-source-record-dl ops-entity-summary-dl">
                                            <dt>Frozen at</dt>
                                            <dd>{data.frozen_dataset?.frozen_at ?? "—"}</dd>
                                            <dt>Split</dt>
                                            <dd>
                                                {data.frozen_dataset?.train_count ?? 0} / {data.frozen_dataset?.val_count ?? 0} /{" "}
                                                {data.frozen_dataset?.test_count ?? 0}
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

                        {spec?.model?.prediction_view?.kind === "image" && pipelineId && specId ? (
                            <ImageRecordsTable
                                pipelineId={pipelineId}
                                specId={specId}
                                parentId={modelId}
                                scope="model_prediction"
                                mode="prediction"
                                title="Predictions for this model"
                            />
                        ) : null}
                    </>
                )}
            </EmptyState>
        </div>
    );
}
