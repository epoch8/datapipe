import React from "react";
import { useParams } from "react-router-dom";
import { Radio, Switch, Table } from "antd";
import type { ColumnsType } from "antd/es/table";
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import type { OpsBBoxRow, OpsImageRecordDetailResponse } from "../../../types/ops";
import { EmptyState, PageHeader } from "../shared";
import { ImagePanel } from "./ImagePanel";

export function ModelPredictionRecordDetailPage() {
    const { specId: rawSpecId = "", entityId: rawEntityId = "", recordKey: rawRecordKey = "" } = useParams<{
        specId?: string;
        entityId?: string;
        recordKey?: string;
    }>();

    const specId = decodeURIComponent(rawSpecId || "");
    const modelId = decodeURIComponent(rawEntityId || "");
    const recordKey = decodeURIComponent(rawRecordKey || "");
    const { pipelineId, loading: pidLoading } = usePipelineId();

    const [detail, setDetail] = React.useState<OpsImageRecordDetailResponse | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);

    const [gtOn, setGtOn] = React.useState(true);
    const [predictionOn, setPredictionOn] = React.useState(true);
    const [viewMode, setViewMode] = React.useState<"both" | "gt" | "prediction">("both");

    const load = React.useCallback(() => {
        if (!pipelineId || !specId || !modelId || !recordKey) return;
        setLoading(true);
        setError(null);
        opsApi
            .getModelPredictionDetail(pipelineId, specId, modelId, recordKey)
            .then(setDetail)
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, specId, modelId, recordKey]);

    React.useEffect(() => {
        load();
    }, [load]);

    const imageName = detail?.pk?.image_name ? String(detail.pk.image_name) : "";

    const predColumns: ColumnsType<OpsBBoxRow> = React.useMemo(
        () => [
            { title: "#", key: "i", render: (_v, _row, idx) => idx + 1 },
            { title: "label", dataIndex: "label", key: "label" },
            { title: "confidence", dataIndex: "confidence", key: "confidence" },
            { title: "x1", dataIndex: "x1", key: "x1" },
            { title: "y1", dataIndex: "y1", key: "y1" },
            { title: "x2", dataIndex: "x2", key: "x2" },
            { title: "y2", dataIndex: "y2", key: "y2" },
        ],
        [],
    );

    const gtColumns: ColumnsType<OpsBBoxRow> = React.useMemo(
        () => [
            { title: "#", key: "i", render: (_v, _row, idx) => idx + 1 },
            { title: "label", dataIndex: "label", key: "label" },
            { title: "x1", dataIndex: "x1", key: "x1" },
            { title: "y1", dataIndex: "y1", key: "y1" },
            { title: "x2", dataIndex: "x2", key: "x2" },
            { title: "y2", dataIndex: "y2", key: "y2" },
        ],
        [],
    );

    const gtUrl = gtOn ? detail?.gt_visualization_url : detail?.plain_gt_image_url;
    const predictionUrl = predictionOn ? detail?.prediction_visualization_url : detail?.plain_prediction_image_url;

    return (
        <div className="ops-page ops-spec-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Datapipe Ops", href: "/" },
                    { label: "Metrics", href: "/metrics" },
                    { label: specId, href: `/metrics/${encodeURIComponent(specId)}` },
                    { label: modelId, href: `/metrics/${encodeURIComponent(specId)}/models/${encodeURIComponent(modelId)}` },
                    { label: "Prediction" },
                ]}
                title={imageName ? `Prediction: ${imageName}` : "Prediction record"}
                subtitle={modelId ? `Model: ${modelId}` : undefined}
                onRefresh={load}
            />

            <EmptyState loading={pidLoading || loading} error={error ?? undefined} empty={!detail && !loading}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 16, marginBottom: 12, flexWrap: "wrap" }}>
                    <div>
                        <div style={{ fontWeight: 600, marginBottom: 6 }}>View</div>
                        <Radio.Group value={viewMode} onChange={(e) => setViewMode(e.target.value)} size="small">
                            <Radio.Button value="both">Show both</Radio.Button>
                            <Radio.Button value="gt">GT only</Radio.Button>
                            <Radio.Button value="prediction">Prediction only</Radio.Button>
                        </Radio.Group>
                    </div>
                    <div style={{ display: "flex", gap: 18, alignItems: "center" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                            <span>GT</span>
                            <Switch checked={gtOn} onChange={setGtOn} />
                        </div>
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                            <span>Prediction</span>
                            <Switch checked={predictionOn} onChange={setPredictionOn} />
                        </div>
                    </div>
                </div>

                <div className="ops-prediction-compare-grid" style={{ marginBottom: 18 }}>
                    {(viewMode === "both" || viewMode === "gt") && (
                        <ImagePanel title="Ground truth" tone="gt" imageUrl={gtUrl} />
                    )}
                    {(viewMode === "both" || viewMode === "prediction") && (
                        <ImagePanel title="Prediction" tone="prediction" imageUrl={predictionUrl} />
                    )}
                </div>

                <div className="ops-record-detail-layout">
                    <div>
                        <div className="ops-panel ops-polished-panel" style={{ marginBottom: 16 }}>
                            <div className="ops-panel-title" style={{ marginBottom: 10 }}>
                                Prediction boxes
                            </div>
                            <Table size="small" rowKey={(_r, i) => `p-${i}`} columns={predColumns} dataSource={detail?.prediction_bbox_rows ?? []} pagination={false} />
                        </div>
                        <div className="ops-panel ops-polished-panel">
                            <div className="ops-panel-title" style={{ marginBottom: 10 }}>
                                Ground truth boxes
                            </div>
                            <Table size="small" rowKey={(_r, i) => `g-${i}`} columns={gtColumns} dataSource={detail?.gt_bbox_rows ?? []} pagination={false} />
                        </div>
                    </div>
                    <div>
                        <div className="ops-panel ops-polished-panel" style={{ marginBottom: 16 }}>
                            <div className="ops-panel-title" style={{ marginBottom: 10 }}>
                                Prediction summary
                            </div>
                            <dl className="ops-source-record-dl">
                                <dt>image_name</dt>
                                <dd>{imageName || "—"}</dd>
                                <dt>model_id</dt>
                                <dd>{modelId || "—"}</dd>
                                <dt>prediction boxes</dt>
                                <dd>{detail?.prediction_bbox_count ?? 0}</dd>
                                <dt>gt boxes</dt>
                                <dd>{detail?.gt_bbox_count ?? 0}</dd>
                                <dt>subset</dt>
                                <dd>{detail?.subset ?? "—"}</dd>
                                <dt>image_url</dt>
                                <dd style={{ wordBreak: "break-all" }}>{detail?.image_url ?? "—"}</dd>
                            </dl>
                        </div>
                        <div className="ops-panel ops-polished-panel">
                            <div className="ops-panel-title" style={{ marginBottom: 10 }}>
                                Raw JSON
                            </div>
                            <pre className="ops-record-json">{JSON.stringify(detail?.record ?? {}, null, 2)}</pre>
                        </div>
                    </div>
                </div>
            </EmptyState>
        </div>
    );
}
