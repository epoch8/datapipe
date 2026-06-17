import React from "react";
import {
    Alert,
    Card,
    Collapse,
    Descriptions,
    Spin,
    Tag,
    Typography,
} from "antd";
import { Link, useParams } from "react-router-dom";
import {
    findTransformInGraph,
    usePipelineGraph,
} from "../../hooks/usePipelineGraph";
import { TableDataPanel } from "./components/TableDataPanel";
import { TransformRunPanel } from "./components/TransformRunPanel";

const { Text } = Typography;

export function TransformDetail() {
    const { id: pipelineId = "", transformName = "" } = useParams();
    const decodedName = decodeURIComponent(transformName);
    const { graph, loading, error } = usePipelineGraph();

    if (error) return <Alert type="error" message={error} />;
    if (loading || !graph) return <Spin />;

    const step = findTransformInGraph(graph, decodedName);
    if (!step || step.type !== "transform") {
        return <Alert type="error" message={`Transform not found: ${decodedName}`} />;
    }

    const metaTable = {
        id: step.name,
        indexes: step.indexes ?? [],
        size: 0,
        store_class: step.transform_type ?? "transform",
        type: "transform",
    };

    const indexKeys = step.indexes ?? [];

    return (
        <div>
            <Text type="secondary">
                <Link to="/">Overview</Link> /{" "}
                <Link to={`/pipelines/${pipelineId}`}>{pipelineId}</Link> / Transform
            </Text>
            <Card title={decodedName} style={{ marginTop: 16 }}>
                <Descriptions column={1} bordered size="small">
                    <Descriptions.Item label="Name">{step.name}</Descriptions.Item>
                    <Descriptions.Item label="Type">
                        <Tag>{step.transform_type}</Tag>
                    </Descriptions.Item>
                    <Descriptions.Item label="Inputs">
                        {step.inputs.map((t) => (
                            <Link
                                key={t}
                                to={`/pipelines/${pipelineId}/tables/${encodeURIComponent(t)}`}
                            >
                                {t}
                            </Link>
                        ))}
                    </Descriptions.Item>
                    <Descriptions.Item label="Outputs">
                        {step.outputs.map((t) => (
                            <Link
                                key={t}
                                to={`/pipelines/${pipelineId}/tables/${encodeURIComponent(t)}`}
                            >
                                {t}
                            </Link>
                        ))}
                    </Descriptions.Item>
                    {step.labels && step.labels.length > 0 && (
                        <Descriptions.Item label="Labels">
                            {step.labels.map(([k, v]) => (
                                <Tag key={`${k}:${v}`}>
                                    {k}={v}
                                </Tag>
                            ))}
                        </Descriptions.Item>
                    )}
                </Descriptions>
            </Card>

            <Collapse defaultActiveKey={["run"]} style={{ marginTop: 16 }}>
                <Collapse.Panel header="Run transform" key="run">
                    <TransformRunPanel
                        transformName={step.name}
                        indexKeys={indexKeys}
                    />
                </Collapse.Panel>
                <Collapse.Panel header="Transform meta table" key="meta">
                    <TableDataPanel table={metaTable} />
                </Collapse.Panel>
            </Collapse>
        </div>
    );
}
