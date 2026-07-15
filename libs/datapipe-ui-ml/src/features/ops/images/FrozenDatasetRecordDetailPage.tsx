import React from "react";
import { useParams } from "react-router-dom";
import { Switch, Table } from "antd";
import type { ColumnsType } from "antd/es/table";
import { opsApi } from "@datapipe/ui/api/client";
import { usePipelineId } from "@datapipe/ui/hooks/usePipelineId";
import type { OpsBBoxRow, OpsImageRecordDetailResponse } from "../../../types/opsMl";
import { EmptyState, PageHeader } from "../shared";

export function FrozenDatasetRecordDetailPage() {
    const { specId: rawSpecId = "", entityId: rawEntityId = "", recordKey: rawRecordKey = "" } = useParams<{
        specId?: string;
        entityId?: string;
        recordKey?: string;
    }>();

    const specId = decodeURIComponent(rawSpecId || "");
    const datasetId = decodeURIComponent(rawEntityId || "");
    const recordKey = decodeURIComponent(rawRecordKey || "");
    const { pipelineId, loading: pidLoading } = usePipelineId();

    const [detail, setDetail] = React.useState<OpsImageRecordDetailResponse | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [annotationsOn, setAnnotationsOn] = React.useState(true);

    const load = React.useCallback(() => {
        if (!pipelineId || !specId || !datasetId || !recordKey) return;
        setLoading(true);
        setError(null);
        opsApi
            .getFrozenDatasetRecordDetail(pipelineId, specId, datasetId, recordKey)
            .then(setDetail)
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, specId, datasetId, recordKey]);

    React.useEffect(() => {
        load();
    }, [load]);

    const imageName = detail?.pk?.image_name ? String(detail.pk.image_name) : "";
    const subset = detail?.subset ?? (detail?.pk?.subset_id ? String(detail.pk.subset_id) : "");
    const imageUrl = annotationsOn ? detail?.visualization_url : detail?.plain_image_url;

    const columns: ColumnsType<OpsBBoxRow> = React.useMemo(
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

    return (
        <div className="ops-page ops-spec-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Datapipe Ops", href: "/" },
                    { label: "Frozen Datasets", href: "/frozen-datasets" },
                    { label: specId, href: `/frozen-datasets/${encodeURIComponent(specId)}` },
                    { label: datasetId, href: `/frozen-datasets/${encodeURIComponent(specId)}/datasets/${encodeURIComponent(datasetId)}` },
                    { label: "Record" },
                ]}
                title={imageName ? `Record: ${imageName}` : "Frozen dataset record"}
                subtitle={subset ? `Subset: ${subset}` : undefined}
                onRefresh={load}
            />

            <EmptyState loading={pidLoading || loading} error={error ?? undefined} empty={!detail && !loading}>
                <div className="ops-record-detail-layout">
                    <div>
                        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 12 }}>
                            <div style={{ fontWeight: 600 }}>Annotations</div>
                            <Switch checked={annotationsOn} onChange={setAnnotationsOn} />
                            <span>{annotationsOn ? "On" : "Off"}</span>
                        </div>
                        {imageUrl ? (
                            <img
                                src={imageUrl}
                                alt={imageName}
                                style={{ width: "100%", maxHeight: 720, objectFit: "contain", borderRadius: 8 }}
                            />
                        ) : null}
                        <div style={{ marginTop: 18 }}>
                            <div className="ops-panel-title" style={{ marginBottom: 10 }}>
                                Boxes
                            </div>
                            <Table size="small" rowKey={(_r, i) => String(i)} columns={columns} dataSource={detail?.bbox_rows ?? []} pagination={false} />
                        </div>
                        <div style={{ marginTop: 18 }}>
                            <div className="ops-panel-title" style={{ marginBottom: 10 }}>
                                Raw record JSON
                            </div>
                            <pre className="ops-record-json">{JSON.stringify(detail?.record ?? {}, null, 2)}</pre>
                        </div>
                    </div>
                    <div className="ops-panel ops-polished-panel">
                        <div className="ops-panel-title" style={{ marginBottom: 10 }}>
                            Record summary
                        </div>
                        <dl className="ops-source-record-dl">
                            <dt>image_name</dt>
                            <dd>{imageName || "—"}</dd>
                            <dt>subset_id</dt>
                            <dd>{subset || "—"}</dd>
                            <dt>bboxes</dt>
                            <dd>{detail?.bbox_count ?? 0}</dd>
                            <dt>image_url</dt>
                            <dd style={{ wordBreak: "break-all" }}>{detail?.image_url ?? "—"}</dd>
                        </dl>
                    </div>
                </div>
            </EmptyState>
        </div>
    );
}
