import React from "react";
import { Button, Table } from "antd";
import { Link, useNavigate, useParams } from "react-router-dom";
import { opsApi } from "@datapipe/ui-ml/api/client";
import { usePipelineId } from "@datapipe/ui/hooks/usePipelineId";
import type {
    TrainingExperimentDetailResponse,
    TrainingExperimentModelRow,
} from "../../../types/opsMl";
import { EmptyState, EntityLink, MetricKpiStrip, PageHeader } from "../shared";
import { TrainingExperimentStatusTag, formatExperimentParams } from "./TrainingExperimentStatusTag";
import { NewTrainingRunDrawer } from "./NewTrainingRunDrawer";
import "./trainingExperiments.css";

function fmtDate(value?: string | null): string {
    if (!value) return "—";
    return value.slice(0, 16).replace("T", " ");
}

export function TrainingExperimentDetailPage() {
    const { specId = "", experimentId: rawExperimentId = "" } = useParams();
    const experimentId = decodeURIComponent(rawExperimentId);
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const navigate = useNavigate();

    const [data, setData] = React.useState<TrainingExperimentDetailResponse | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<unknown>(null);
    const [runOpen, setRunOpen] = React.useState(false);

    const load = React.useCallback(() => {
        if (!pipelineId || !specId || !experimentId) return;
        setLoading(true);
        setError(null);
        opsApi
            .getTrainingExperimentDetail(pipelineId, specId, experimentId)
            .then(setData)
            .catch((e) => setError(e))
            .finally(() => setLoading(false));
    }, [pipelineId, specId, experimentId]);

    React.useEffect(() => {
        load();
    }, [load]);

    const experiment = data?.experiment;
    const models = data?.models ?? [];
    const datasetsCount = new Set(models.map((row) => row.frozen_dataset_id)).size;

    const columns = [
        {
            title: "Model",
            dataIndex: "model_id",
            render: (value: string, row: TrainingExperimentModelRow) => (
                <EntityLink kind="model" id={value} datasetId={row.frozen_dataset_id} specId={specId} />
            ),
        },
        {
            title: "Frozen dataset",
            dataIndex: "frozen_dataset_id",
            render: (value: string, row: TrainingExperimentModelRow) => (
                <EntityLink kind="dataset" id={value} specId={specId}>
                    {row.frozen_dataset_display_name || value}
                </EntityLink>
            ),
        },
        {
            title: "Trained at",
            dataIndex: "created_at",
            width: 180,
            render: (value?: string | null) => fmtDate(value),
        },
        {
            title: "Run",
            dataIndex: "run_key",
            width: 200,
            render: (value?: string | null) => value || "—",
        },
    ];

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Datapipe Ops", href: "/" },
                    { label: "Training", href: "/training" },
                    { label: specId, href: `/training/${encodeURIComponent(specId)}` },
                    { label: experiment?.display_name ?? experimentId },
                ]}
                title={experiment?.display_name ?? experimentId}
                subtitle="Models trained with this experiment and the frozen datasets they used."
                statusChips={[
                    ...(experiment
                        ? [{ label: experiment.source, variant: "default" as const }]
                        : []),
                    { label: specId, variant: "purple" as const },
                ]}
                extra={
                    <>
                        <Button
                            type="primary"
                            disabled={!experiment?.capabilities.can_launch}
                            onClick={() => setRunOpen(true)}
                        >
                            New training run
                        </Button>
                        <Link to={`/training/${encodeURIComponent(specId)}?tab=experiments`}>
                            <Button>Back to experiments</Button>
                        </Link>
                    </>
                }
                onRefresh={load}
            />

            {pipelineId && (
                <NewTrainingRunDrawer
                    open={runOpen}
                    pipelineId={pipelineId}
                    specId={specId}
                    preselectExperimentId={experimentId}
                    onClose={() => setRunOpen(false)}
                    onLaunched={() => load()}
                />
            )}

            <EmptyState loading={pidLoading || loading} error={error} empty={!data && !loading}>
                {experiment && (
                    <>
                        <MetricKpiStrip
                            items={[
                                { key: "models", label: "Models", value: models.length, format: "integer" },
                                {
                                    key: "datasets",
                                    label: "Datasets",
                                    value: datasetsCount,
                                    format: "integer",
                                },
                                {
                                    key: "requests",
                                    label: "Requests",
                                    value: experiment.requests_count,
                                    format: "integer",
                                },
                                {
                                    key: "runs",
                                    label: "Runs",
                                    value: experiment.runs_count,
                                    format: "integer",
                                },
                            ]}
                        />

                        <div className="ops-entity-layout">
                            <div className="ops-panel ops-polished-panel">
                                <div className="ops-panel-title">Experiment</div>
                                <div style={{ marginBottom: 10 }}>
                                    <TrainingExperimentStatusTag row={experiment} />
                                </div>
                                {experiment.description ? (
                                    <p className="te-params">{experiment.description}</p>
                                ) : null}
                                <dl className="ops-detail-list ops-detail-list-wide">
                                    <dt>Main params</dt>
                                    <dd>{formatExperimentParams(experiment)}</dd>
                                    <dt>Config type</dt>
                                    <dd>{experiment.config_type}</dd>
                                    <dt>Revision</dt>
                                    <dd>{experiment.revision}</dd>
                                    <dt>Last used</dt>
                                    <dd>{fmtDate(experiment.last_used_at)}</dd>
                                    <dt>Updated</dt>
                                    <dd>{fmtDate(experiment.updated_at)}</dd>
                                </dl>
                            </div>

                            <div className="ops-panel ops-polished-panel ops-spec-table-panel">
                                <div className="ops-panel-title">Trained models</div>
                                <EmptyState
                                    empty={!models.length}
                                    emptyMessage="No models trained with this experiment yet."
                                >
                                    <Table
                                        className="ops-table ops-spec-table"
                                        size="middle"
                                        rowKey={(row) => `${row.model_id}::${row.frozen_dataset_id}`}
                                        columns={columns}
                                        dataSource={models}
                                        pagination={false}
                                        scroll={{ x: "max-content" }}
                                    />
                                </EmptyState>
                            </div>
                        </div>
                    </>
                )}
            </EmptyState>
        </div>
    );
}
