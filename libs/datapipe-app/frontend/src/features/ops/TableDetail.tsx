import React from "react";
import { Alert, Card, Spin } from "antd";
import { useParams, useSearchParams } from "react-router-dom";
import { ApiErrorAlert } from "../../components/ApiErrorAlert";
import { GraphNodeDetailBody } from "../cy/GraphNodeDetailContent";
import { nodeDataFromTable } from "../cy/graphNodes";
import {
    findTableInGraph,
    usePipelineGraph,
} from "../../hooks/usePipelineGraph";
import { PageHeader } from "./shared";

export function TableDetail() {
    const { id: pipelineId = "", tableName = "" } = useParams();
    const [searchParams] = useSearchParams();
    const decodedName = decodeURIComponent(tableName);
    const focusCol = searchParams.get("focus_col") ?? undefined;
    const focusValue = searchParams.get("focus_value") ?? undefined;
    const initialColumnFilter =
        focusCol && focusValue ? { column: focusCol, value: focusValue } : undefined;
    const { graph, loading, error, refresh } = usePipelineGraph();

    const table = graph ? findTableInGraph(graph, decodedName) : null;
    const node = nodeDataFromTable(
        decodedName,
        table?.indexes ?? [],
        table?.store_class ?? "TableStoreDB",
        table?.size ?? null,
        table?.schema ?? [],
    );

    if (error) return <ApiErrorAlert error={error} />;
    if (graph && !table) {
        return <Alert type="error" message={`Table not found: ${decodedName}`} />;
    }

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Overview", href: "/" },
                    { label: pipelineId, href: `/pipelines/${pipelineId}` },
                    { label: "Table" },
                ]}
                title={decodedName}
                onRefresh={refresh}
            />
            {loading && !graph && (
                <div style={{ marginBottom: 12 }}>
                    <Spin size="small" /> Loading catalog metadata…
                </div>
            )}
            <Card className="graph-node-detail-page">
                <GraphNodeDetailBody
                    node={node}
                    pipelineId={pipelineId}
                    showHeader={false}
                    showTableData
                    initialColumnFilter={initialColumnFilter}
                />
            </Card>
        </div>
    );
}
