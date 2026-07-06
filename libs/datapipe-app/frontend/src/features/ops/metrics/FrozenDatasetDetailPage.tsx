import React from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { Button, Table } from "antd";
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import type { FrozenDatasetDetailResponse } from "../../../types/ops";
import { EmptyState, PageHeader } from "../shared";
import { EntityLink } from "./EntityLink";
import { MetricKpiStrip } from "./MetricKpiStrip";
import { SourceRecordCard } from "./SourceRecordCard";
import { buildMetricsUrl } from "./entityUrls";
import { formatFrozenAt } from "./frozenDatasetFormat";

function formatCreatedAt(value?: string | null): string | null {
    if (!value) return null;
    return formatFrozenAt(value) ?? value.slice(0, 16).replace("T", " ");
}

export function FrozenDatasetDetailPage() {
    const { datasetId: rawDatasetId = "" } = useParams<{ datasetId: string }>();
    const datasetId = decodeURIComponent(rawDatasetId);
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [searchParams] = useSearchParams();
    const subset = searchParams.get("subset") ?? undefined;

    const [data, setData] = React.useState<FrozenDatasetDetailResponse | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);

    const load = React.useCallback(() => {
        if (!pipelineId || !datasetId) return;
        setLoading(true);
        setError(null);
        opsApi
            .getFrozenDatasetDetail(pipelineId, datasetId, { subset })
            .then(setData)
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, datasetId, subset]);

    React.useEffect(() => {
        load();
    }, [load]);

    const kpiItems = React.useMemo(() => {
        if (!data) return [];
        const d = data.dataset;
        return [
            { key: "train", label: "Train rows", value: d.train_count ?? 0, format: "integer" as const },
            { key: "val", label: "Val rows", value: d.val_count ?? 0, format: "integer" as const },
            { key: "test", label: "Test rows", value: d.test_count ?? 0, format: "integer" as const },
            { key: "models", label: "Models trained", value: data.coverage.models_total, format: "integer" as const },
            {
                key: "best",
                label: "Best W-F1",
                value: data.coverage.best_metric_value ?? null,
                format: "float" as const,
            },
            {
                key: "best_model",
                label: "Best model",
                value: data.coverage.best_model_id ?? "",
                format: "string" as const,
            },
        ];
    }, [data]);

    const columns = [
        {
            title: "Model",
            dataIndex: "model_id",
            render: (v: string) => <EntityLink kind="model" id={v} datasetId={datasetId} subset={subset} />,
        },
        {
            title: "Created at",
            dataIndex: "created_at",
            width: 180,
            render: (v?: string | null) => formatCreatedAt(v),
        },
    ];

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Datapipe Ops", href: "/" },
                    { label: pipelineId || "pipeline" },
                    { label: "Metrics", href: buildMetricsUrl(pipelineId || undefined) },
                    { label: "Frozen datasets" },
                    { label: datasetId },
                ]}
                title={`Frozen dataset: ${datasetId}`}
                subtitle="Snapshot of a frozen dataset split used to train models and compute metrics."
                statusChips={[
                    { label: "frozen", variant: "purple" },
                    ...(subset ? [{ label: subset, variant: "default" as const }] : []),
                ]}
                extra={
                    <>
                        <Button href={data?.source_table_url ?? undefined} disabled={!data?.source_table_url}>
                            Open dataset row
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
                        <MetricKpiStrip items={kpiItems} />

                        <div className="ops-entity-layout">
                            <SourceRecordCard
                                title="Source frozen dataset record"
                                record={data.source_record}
                                preferredFields={[
                                    "frozen_dataset_id",
                                    "train_images_count",
                                    "val_images_count",
                                    "test_images_count",
                                    "created_at",
                                ]}
                                sourceTable={data.source_table}
                                sourceTableUrl={data.source_table_url}
                                pipelineId={pipelineId}
                            />

                            <div className="ops-panel ops-entity-summary-panel">
                                <div className="ops-panel-title">Split summary</div>
                                <dl className="ops-source-record-dl ops-entity-summary-dl">
                                    <dt>Frozen at</dt>
                                    <dd>{formatFrozenAt(data.dataset.frozen_at)}</dd>
                                    <dt>Train / Val / Test</dt>
                                    <dd>
                                        {data.dataset.train_count ?? 0} / {data.dataset.val_count ?? 0} / {data.dataset.test_count ?? 0}
                                    </dd>
                                    <dt>Models trained</dt>
                                    <dd>{data.coverage.models_total}</dd>
                                </dl>
                            </div>
                        </div>

                        <div className="ops-data-table ops-entity-metrics-table ops-frozen-linked-models-table">
                            <div className="ops-data-table-toolbar">
                                <div className="ops-data-table-title">Models trained on this frozen dataset</div>
                            </div>
                            <Table
                                className="ops-table"
                                size="middle"
                                rowKey="model_id"
                                columns={columns}
                                dataSource={data.linked_models}
                                pagination={false}
                            />
                        </div>

                        <div className="ops-entity-footer-strip">
                            <span>
                                Models with metrics: {data.coverage.models_with_metrics} / {data.coverage.models_total}
                            </span>
                            <span>Subsets covered: {data.coverage.subsets.join(", ") || "—"}</span>
                            {data.source_table && (
                                <span>
                                    Related links:{" "}
                                    <Link to={`/pipelines/${encodeURIComponent(pipelineId)}/tables/${encodeURIComponent(data.source_table)}`}>
                                        {data.source_table}
                                    </Link>
                                </span>
                            )}
                        </div>
                    </>
                )}
            </EmptyState>
        </div>
    );
}
