import React from "react";
import { Link } from "react-router-dom";
import { Modal, Radio, Spin, Switch } from "antd";
import { opsApi } from "@datapipe/ui-ml/api/client";
import type { OpsImageRecordDetailResponse } from "../../../types/opsMl";
import { ImagePanel } from "./ImagePanel";

type Scope = "data" | "frozen_dataset" | "model_prediction";

export function ImageRecordPreviewModal({
    open,
    onClose,
    pipelineId,
    specId,
    scope,
    parentId,
    recordKey,
}: {
    open: boolean;
    onClose: () => void;
    pipelineId: string;
    specId: string;
    scope: Scope;
    parentId?: string;
    recordKey: string;
}) {
    const [loading, setLoading] = React.useState(false);
    const [error, setError] = React.useState<string | null>(null);
    const [detail, setDetail] = React.useState<OpsImageRecordDetailResponse | null>(null);

    const [annotationsOn, setAnnotationsOn] = React.useState(true);
    const [gtOn, setGtOn] = React.useState(true);
    const [predictionOn, setPredictionOn] = React.useState(true);
    const [viewMode, setViewMode] = React.useState<"both" | "gt" | "prediction">("both");

    React.useEffect(() => {
        if (!open) return;
        setLoading(true);
        setError(null);
        setDetail(null);
        const fetch = async () => {
            if (scope === "data") {
                const d = await opsApi.getImageRecordDetail(pipelineId, specId, recordKey);
                setDetail(d);
                return;
            }
            if (scope === "frozen_dataset") {
                if (!parentId) throw new Error("parentId is required for frozen_dataset scope");
                const d = await opsApi.getFrozenDatasetRecordDetail(pipelineId, specId, parentId, recordKey);
                setDetail(d);
                return;
            }
            if (scope === "model_prediction") {
                if (!parentId) throw new Error("parentId is required for model_prediction scope");
                const d = await opsApi.getModelPredictionDetail(pipelineId, specId, parentId, recordKey);
                setDetail(d);
            }
        };
        fetch().catch((e) => setError(String(e))).finally(() => setLoading(false));
    }, [open, pipelineId, specId, scope, parentId, recordKey]);

    React.useEffect(() => {
        if (!open) return;
        setAnnotationsOn(true);
        setGtOn(true);
        setPredictionOn(true);
        setViewMode("both");
    }, [open]);

    const openHref = React.useMemo(() => {
        const eSpec = encodeURIComponent(specId);
        const eKey = encodeURIComponent(recordKey);
        if (scope === "data") return `/image/${eSpec}/records/${eKey}`;
        if (scope === "frozen_dataset") {
            if (!parentId) return "#";
            return `/frozen-datasets/${eSpec}/datasets/${encodeURIComponent(parentId)}/records/${eKey}`;
        }
        if (!parentId) return "#";
        return `/metrics/${eSpec}/models/${encodeURIComponent(parentId)}/predictions/${eKey}`;
    }, [scope, specId, parentId, recordKey]);

    return (
        <Modal
            visible={open}
            title={scope === "model_prediction" ? "Prediction preview" : "Image preview"}
            onCancel={onClose}
            width={1100}
            footer={[
                <Link key="open" to={openHref}>
                    Open full record
                </Link>,
                <div key="spacer" style={{ flex: 1 }} />,
                <button key="close" type="button" className="dp-link-button" onClick={onClose}>
                    Close
                </button>,
            ]}
        >
            {error ? <div style={{ color: "var(--dp-red-500)" }}>{error}</div> : null}
            {loading || !detail ? (
                <div style={{ padding: 24, textAlign: "center" }}>
                    <Spin />
                </div>
            ) : scope === "model_prediction" ? (
                <div>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 16, marginBottom: 12 }}>
                        <div>
                            <div style={{ fontWeight: 600, marginBottom: 6 }}>View</div>
                            <Radio.Group value={viewMode} onChange={(e) => setViewMode(e.target.value)} size="small">
                                <Radio.Button value="both">GT+Prediction</Radio.Button>
                                <Radio.Button value="gt">GT only</Radio.Button>
                                <Radio.Button value="prediction">Prediction only</Radio.Button>
                            </Radio.Group>
                        </div>
                        <div style={{ display: "flex", gap: 18, alignItems: "center" }}>
                            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                                <span>GT annotations</span>
                                <Switch checked={gtOn} onChange={setGtOn} />
                            </div>
                            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                                <span>Prediction annotations</span>
                                <Switch checked={predictionOn} onChange={setPredictionOn} />
                            </div>
                        </div>
                    </div>
                    <div className="ops-prediction-compare-grid">
                        {(viewMode === "both" || viewMode === "gt") && (
                            <ImagePanel
                                tone="gt"
                                title="Ground truth"
                                imageUrl={
                                    detail.gt_label != null
                                        ? detail.plain_gt_image_url ?? (gtOn ? detail.gt_visualization_url : detail.plain_gt_image_url)
                                        : gtOn
                                          ? detail.gt_visualization_url
                                          : detail.plain_gt_image_url
                                }
                                label={detail.gt_label}
                            />
                        )}
                        {(viewMode === "both" || viewMode === "prediction") && (
                            <ImagePanel
                                tone="prediction"
                                title="Prediction"
                                imageUrl={
                                    detail.prediction_label != null || detail.label != null
                                        ? detail.plain_prediction_image_url ??
                                          (predictionOn ? detail.prediction_visualization_url : detail.plain_prediction_image_url)
                                        : predictionOn
                                          ? detail.prediction_visualization_url
                                          : detail.plain_prediction_image_url
                                }
                                label={detail.prediction_label ?? detail.label}
                            />
                        )}
                    </div>
                </div>
            ) : (
                <div>
                    <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 12 }}>
                        <div style={{ fontWeight: 600 }}>Annotations</div>
                        <Switch checked={annotationsOn} onChange={setAnnotationsOn} />
                        <span>{annotationsOn ? "On" : "Off"}</span>
                    </div>
                    <ImagePanel
                        title="Image"
                        tone="neutral"
                        imageUrl={annotationsOn ? detail.visualization_url : detail.plain_image_url}
                        label={detail.label ?? detail.gt_label}
                    />
                </div>
            )}
        </Modal>
    );
}

