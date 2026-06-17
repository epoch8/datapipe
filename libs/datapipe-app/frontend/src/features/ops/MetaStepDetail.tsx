import React from "react";
import {
    Alert,
    Card,
    Descriptions,
    List,
    Spin,
    Tag,
    Typography,
} from "antd";
import { Link, useParams } from "react-router-dom";
import {
    findMetaStepInGraph,
    usePipelineGraph,
} from "../../hooks/usePipelineGraph";

const { Text } = Typography;

export function MetaStepDetail() {
    const { id: pipelineId = "", stepName = "" } = useParams();
    const decodedName = decodeURIComponent(stepName);
    const { graph, loading, error } = usePipelineGraph();

    if (error) return <Alert type="error" message={error} />;
    if (loading || !graph) return <Spin />;

    const meta = findMetaStepInGraph(graph, decodedName);
    if (!meta || meta.type !== "meta") {
        return <Alert type="error" message={`Pipeline step not found: ${decodedName}`} />;
    }

    const subSteps = meta.graph?.pipeline ?? [];

    return (
        <div>
            <Text type="secondary">
                <Link to="/">Overview</Link> /{" "}
                <Link to={`/pipelines/${pipelineId}`}>{pipelineId}</Link> / Pipeline step
            </Text>
            <Card title={decodedName} style={{ marginTop: 16 }}>
                <Descriptions column={1} bordered size="small">
                    <Descriptions.Item label="Class">{meta.name}</Descriptions.Item>
                    <Descriptions.Item label="Sub-steps">{subSteps.length}</Descriptions.Item>
                    <Descriptions.Item label="External inputs">
                        {(meta.inputs ?? []).map((t) => (
                            <Link
                                key={t}
                                to={`/pipelines/${pipelineId}/tables/${encodeURIComponent(t)}`}
                            >
                                {t}
                            </Link>
                        ))}
                    </Descriptions.Item>
                    <Descriptions.Item label="External outputs">
                        {(meta.outputs ?? []).map((t) => (
                            <Link
                                key={t}
                                to={`/pipelines/${pipelineId}/tables/${encodeURIComponent(t)}`}
                            >
                                {t}
                            </Link>
                        ))}
                    </Descriptions.Item>
                    {meta.labels && meta.labels.length > 0 && (
                        <Descriptions.Item label="Labels">
                            {meta.labels.map(([k, v]) => (
                                <Tag key={`${k}:${v}`}>
                                    {k}={v}
                                </Tag>
                            ))}
                        </Descriptions.Item>
                    )}
                </Descriptions>
            </Card>
            <Card title="Sub-steps" style={{ marginTop: 16 }}>
                <List
                    dataSource={subSteps}
                    renderItem={(item) => (
                        <List.Item>
                            {item.type === "transform" ? (
                                <Link
                                    to={`/pipelines/${pipelineId}/transforms/${encodeURIComponent(item.name)}`}
                                >
                                    {item.name}
                                </Link>
                            ) : (
                                <span>{item.name}</span>
                            )}
                            {item.type === "transform" && (
                                <Tag style={{ marginLeft: 8 }}>{item.transform_type}</Tag>
                            )}
                        </List.Item>
                    )}
                />
            </Card>
        </div>
    );
}
