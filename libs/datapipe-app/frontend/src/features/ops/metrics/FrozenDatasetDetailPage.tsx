import React from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { Button, Table, Tooltip } from "antd";
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import type { FrozenDatasetDetailResponse, MetricsModelRow } from "../../../types/ops";
import { MetricValue, EmptyState, PageHeader } from "../shared";
import { EntityLink } from "./EntityLink";
import { MetricKpiStrip } from "./MetricKpiStrip";
import { SourceRecordCard } from "./SourceRecordCard";
import { buildMetricsUrl, truncateMiddle } from "./entityUrls";
import { formatFrozenAt } from "./frozenDatasetFormat";

function precisionRecallCell(row: MetricsModelRow): React.ReactNode {
    if (!row.has_metrics) return null;
    const p = row.metrics?.weighted_precision ?? row.metrics?.weighted_without_pseudo_classes_precision;
    const r = row.metrics?.weighted_recall ?? row.metrics?.weighted_without_pseudo_classes_recall;
    if (p == null && r == null) return null;
    return (
        <span>
            {p != null ? <MetricValue value={p} format="float" /> : ""}
            {p != null && r != null ? " / " : ""}
            {r != null ? <MetricValue value={r} format="float" /> : ""}
        </span>
    );
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
            width: 220,
            render: (v: string) => <EntityLink kind="model" id={v} datasetId={datasetId} subset={subset} />,
        },
        {
            title: "Created at",
            dataIndex: "started_at",
            width: 140,
            render: (v?: string) => (v ? v.slice(0, 16).replace("T", " ") : null),
        },
        {
            title: "Run",
            dataIndex: "run_key",
            width: 120,
            render: (v?: string, row?: MetricsModelRow) => v ?? row?.run_id ?? null,
        },
        { title: "Subset", dataIndex: "subset", width: 72 },
        {
            title: "Best metric",
            key: "best_metric",
            width: 100,
            align: "right" as const,
            render: (_: unknown, row: MetricsModelRow) => {
                if (!row.has_metrics) return null;
                const val = row.metrics?.weighted_f1_score ?? row.metrics?.mAP50_95;
                return val != null ? <MetricValue value={val} format="float" /> : null;
            },
        },
        {
            title: "Precision / Recall",
            key: "pr",
            width: 140,
            align: "right" as const,
            render: (_: unknown, row: MetricsModelRow) => precisionRecallCell(row),
        },
        {
            title: "Support",
            key: "support",
            width: 80,
            align: "right" as const,
            render: (_: unknown, row: MetricsModelRow) => {
                const val = row.metrics?.support;
                return val != null ? <MetricValue value={val} format="integer" /> : null;
            },
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
                    { label: truncateMiddle(datasetId, 24) },
                ]}
                title={`Frozen dataset: ${truncateMiddle(datasetId, 40)}`}
                titleTooltip={datasetId}
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

                        <div className="ops-entity-cards-row">
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
                            <div className="ops-panel ops-source-record-card">
                                <div className="ops-panel-title">Split summary</div>
                                <dl className="ops-source-record-dl">
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

                        <div className="ops-data-table ops-entity-metrics-table">
                            <div className="ops-data-table-toolbar">
                                <div className="ops-data-table-title">Models trained on this frozen dataset</div>
                            </div>
                            <Table
                                className="ops-table"
                                size="middle"
                                rowKey="id"
                                columns={columns}
                                dataSource={data.models}
                                pagination={false}
                                scroll={{ x: 1000 }}
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
