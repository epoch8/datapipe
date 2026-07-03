import React from "react";
import { Alert, Card, Spin } from "antd";
import { useParams } from "react-router-dom";
import { GraphNodeDetailBody } from "../cy/GraphNodeDetailContent";
import { graphNodesByIdFromGraph, nodeDataFromMeta } from "../cy/graphNodes";
import {
    findMetaStepInGraph,
    usePipelineGraph,
} from "../../hooks/usePipelineGraph";
import { PageHeader } from "./shared";

export function MetaStepDetail() {
    const { id: pipelineId = "", stepName = "" } = useParams();
    const decodedName = decodeURIComponent(stepName);
    const { graph, loading, error, refresh } = usePipelineGraph();

    if (error) return <Alert type="error" message={error} />;
    if (loading || !graph) return <Spin />;

    const meta = findMetaStepInGraph(graph, decodedName);
    if (!meta || meta.type !== "meta") {
        return <Alert type="error" message={`Pipeline step not found: ${decodedName}`} />;
    }

    const subSteps = meta.graph?.pipeline ?? [];
    const node = nodeDataFromMeta(meta);
    const graphNodesById = graphNodesByIdFromGraph(graph);

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Overview", href: "/" },
                    { label: pipelineId, href: `/pipelines/${pipelineId}` },
                    { label: "Pipeline step" },
                ]}
                title={decodedName}
                onRefresh={refresh}
            />
            <Card className="graph-node-detail-page">
                <GraphNodeDetailBody
                    node={node}
                    graphNodesById={graphNodesById}
                    pipelineId={pipelineId}
                    subSteps={subSteps}
                    showHeader={false}
                    showTableData={false}
                    showMetaTable={false}
                />
            </Card>
        </div>
    );
}
