import React, { useEffect, useState } from "react";
import {
    Alert,
    Button,
    Card,
    Popconfirm,
    Space,
    Spin,
} from "antd";
import { useParams } from "react-router-dom";
import { GraphNodeDetailBody } from "../cy/GraphNodeDetailContent";
import { graphNodesByIdFromGraph, nodeDataFromTransform } from "../cy/graphNodes";
import { fetchTransformMetaSize } from "../../api/graph";
import { ApiErrorAlert } from "../../components/ApiErrorAlert";
import { getApiErrorMessage } from "../../api/http";
import { opsApi } from "../../api/client";
import {
    findTransformInGraph,
    usePipelineGraph,
} from "../../hooks/usePipelineGraph";
import { PageHeader, TableSizeControl } from "./shared";
import { TableDataPanel } from "./components/TableDataPanel";
import { TransformRunPanel } from "./components/TransformRunPanel";

export function TransformDetail() {
    const { id: pipelineId = "", transformName = "" } = useParams();
    const decodedName = decodeURIComponent(transformName);
    const { graph, loading, error, refresh } = usePipelineGraph();
    const [resetting, setResetting] = useState(false);
    const [metaRefreshKey, setMetaRefreshKey] = useState(0);
    const [metaSize, setMetaSize] = useState<number | null>(null);
    const [countingMeta, setCountingMeta] = useState(false);
    const [metaSizeError, setMetaSizeError] = useState<string | null>(null);
    const [resetAlert, setResetAlert] = useState<{ type: "success" | "error"; message: string } | null>(
        null,
    );

    useEffect(() => {
        setMetaRefreshKey(0);
    }, [decodedName]);

    const step = graph ? findTransformInGraph(graph, decodedName) : null;
    const node = step
        ? nodeDataFromTransform(step)
        : nodeDataFromTransform({
              name: decodedName,
              inputs: [],
              outputs: [],
              indexes: [],
          });
    const graphNodesById = graph ? graphNodesByIdFromGraph(graph) : undefined;
    const indexKeys = step?.indexes ?? [];
    const hasTransformMeta =
        node.has_transform_meta === true ||
        node.total_idx_count != null ||
        node.changed_idx_count != null;

    const metaTable = {
        id: decodedName,
        indexes: indexKeys,
        size: metaSize,
        store_class: step?.transform_type ?? "transform",
        type: "transform",
    };

    const countMetaSize = () => {
        setCountingMeta(true);
        setMetaSizeError(null);
        fetchTransformMetaSize(decodedName)
            .then(setMetaSize)
            .catch((e) => setMetaSizeError(getApiErrorMessage(e)))
            .finally(() => setCountingMeta(false));
    };

    const resetTransformMeta = () => {
        setResetting(true);
        setResetAlert(null);
        opsApi
            .resetTransformMetadata(pipelineId, decodedName)
            .then(() => {
                setMetaRefreshKey((key) => key + 1);
                setMetaSize(null);
                setResetAlert({
                    type: "success",
                    message: "Transform meta table reset. All rows are marked unprocessed.",
                });
            })
            .catch((e) => setResetAlert({ type: "error", message: getApiErrorMessage(e) }))
            .finally(() => setResetting(false));
    };

    if (error) return <ApiErrorAlert error={error} />;
    if (graph && !step) {
        return <Alert type="error" message={`Transform not found: ${decodedName}`} />;
    }

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Overview", href: "/" },
                    { label: pipelineId, href: `/pipelines/${pipelineId}` },
                    { label: "Transform" },
                ]}
                title={decodedName}
                onRefresh={refresh}
            />
            {loading && !graph && (
                <div style={{ marginBottom: 12 }}>
                    <Spin size="small" /> Loading catalog metadata…
                </div>
            )}
            <Card className="graph-node-detail-page" style={{ marginBottom: 16 }}>
                <GraphNodeDetailBody
                    node={node}
                    graphNodesById={graphNodesById}
                    pipelineId={pipelineId}
                    showHeader={false}
                    showMetaTable={false}
                />
            </Card>
            <Card title="Run transform" style={{ marginBottom: 16 }}>
                <TransformRunPanel transformName={decodedName} indexKeys={indexKeys} />
            </Card>
            {hasTransformMeta && (
                <Card title="Actions" style={{ marginBottom: 16 }}>
                    <Space direction="vertical" style={{ width: "100%" }}>
                        <Popconfirm
                            title="Reset transform meta table? All rows will be marked unprocessed and the transform will re-run on the next execution."
                            onConfirm={resetTransformMeta}
                            okText="Reset"
                            okButtonProps={{ danger: true }}
                        >
                            <Button danger loading={resetting}>
                                Reset Transform Meta Table
                            </Button>
                        </Popconfirm>
                        {resetAlert && (
                            <Alert
                                type={resetAlert.type}
                                message={resetAlert.message}
                                showIcon
                                closable
                                onClose={() => setResetAlert(null)}
                            />
                        )}
                    </Space>
                </Card>
            )}
            {hasTransformMeta && (
                <Card title="Transform meta table">
                    <div style={{ marginBottom: 12 }}>
                        <TableSizeControl
                            size={metaSize}
                            loading={countingMeta}
                            onCount={countMetaSize}
                        />
                        {metaSizeError ? (
                            <div style={{ color: "#ff4d4f", marginTop: 8 }}>{metaSizeError}</div>
                        ) : null}
                    </div>
                    <TableDataPanel
                        key={metaRefreshKey}
                        table={metaTable}
                        knownRowCount={metaSize}
                        hideRunStep
                    />
                </Card>
            )}
        </div>
    );
}
