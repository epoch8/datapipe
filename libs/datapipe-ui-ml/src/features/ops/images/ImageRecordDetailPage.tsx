import React from "react";
import { useParams } from "react-router-dom";
import { Switch, Table, Tag } from "antd";
import type { ColumnsType } from "antd/es/table";
import { opsApi } from "@datapipe/ui/api/client";
import { usePipelineId } from "@datapipe/ui/hooks/usePipelineId";
import type { OpsBBoxRow, OpsImageRecordDetailResponse } from "../../../types/opsMl";
import { EmptyState, PageHeader } from "../shared";

export function ImageRecordDetailPage() {
    const { specId: rawSpecId = "", recordKey: rawRecordKey = "" } = useParams<{
        specId?: string;
        recordKey?: string;
    }>();

    const specId = decodeURIComponent(rawSpecId || "");
    const recordKey = decodeURIComponent(rawRecordKey || "");
    const { pipelineId, loading: pidLoading } = usePipelineId();

    const [detail, setDetail] = React.useState<OpsImageRecordDetailResponse | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [annotationsOn, setAnnotationsOn] = React.useState(true);

    const load = React.useCallback(() => {
        if (!pipelineId || !specId || !recordKey) return;
        setLoading(true);
        setError(null);
        opsApi
            .getImageRecordDetail(pipelineId, specId, recordKey)
            .then(setDetail)
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, specId, recordKey]);

    React.useEffect(() => {
        load();
    }, [load]);

    const imageName = detail?.pk?.image_name ? String(detail.pk.image_name) : "";
    const imageUrl = annotationsOn ? detail?.visualization_url : detail?.plain_image_url;

    const columns: ColumnsType<OpsBBoxRow> = React.useMemo(
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

    return (
        <div className="ops-page ops-spec-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Datapipe Ops", href: "/" },
                    { label: "Image", href: "/image" },
                    { label: specId, href: `/image/${encodeURIComponent(specId)}` },
                    { label: "Record" },
                ]}
                title={imageName ? `Record: ${imageName}` : "Image record"}
                subtitle={detail?.subset ? `Subset: ${detail.subset}` : undefined}
                statusChips={detail ? [{ label: `${detail.bbox_count ?? 0} boxes`, variant: "purple" }] : []}
                onRefresh={load}
            />

            <EmptyState loading={pidLoading || loading} error={error ?? undefined} empty={!detail && !loading}>
                <div className="ops-record-detail-layout">
                    <div>
                        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 12 }}>
                            <div style={{ fontWeight: 600 }}>Annotations</div>
                            <Switch checked={annotationsOn} onChange={setAnnotationsOn} />
                            <span>{annotationsOn ? "On" : "Off"}</span>
                            {detail?.subset ? <Tag className="ops-soft-chip">{detail.subset}</Tag> : null}
                        </div>

                        {imageUrl ? (
                            <div className="ops-image-panel-body" style={{ padding: 0 }}>
                                <img src={imageUrl} alt={imageName} style={{ width: "100%", maxHeight: 720, objectFit: "contain" }} />
                            </div>
                        ) : null}

                        <div style={{ marginTop: 18 }}>
                            <div className="ops-panel-title" style={{ marginBottom: 10 }}>
                                Ground truth boxes
                            </div>
                            <Table
                                size="small"
                                rowKey={(r, i) => String(i)}
                                columns={columns}
                                dataSource={detail?.bbox_rows ?? []}
                                pagination={false}
                            />
                        </div>

                        <div style={{ marginTop: 18 }}>
                            <div className="ops-panel-title" style={{ marginBottom: 10 }}>
                                Raw JSON
                            </div>
                            <pre className="ops-record-json">{JSON.stringify(detail?.record ?? {}, null, 2)}</pre>
                        </div>
                    </div>
                    <div>
                        <div className="ops-panel ops-polished-panel">
                            <div className="ops-panel-title" style={{ marginBottom: 10 }}>
                                Record summary
                            </div>
                            <div style={{ display: "grid", gap: 8 }}>
                                <div>
                                    <div style={{ color: "var(--dp-gray-500)", fontSize: 12 }}>Record key</div>
                                    <div style={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace" }}>
                                        {detail?.record_key}
                                    </div>
                                </div>
                                <div>
                                    <div style={{ color: "var(--dp-gray-500)", fontSize: 12 }}>PK</div>
                                    <pre className="ops-record-json" style={{ margin: 0 }}>
                                        {JSON.stringify(detail?.pk ?? {}, null, 2)}
                                    </pre>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </EmptyState>
        </div>
    );
}

