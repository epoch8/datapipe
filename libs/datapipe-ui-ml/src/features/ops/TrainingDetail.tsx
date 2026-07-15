import React from "react";
import { Alert, Card, Descriptions, Spin, Typography } from "antd";
import { Link, useParams } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "@datapipe/ui/api/client";

const { Text } = Typography;

export function TrainingDetail() {
    const { runKey = "" } = useParams();
    const [detail, setDetail] = React.useState<Record<string, unknown>>({});
    const [error, setError] = React.useState<string | null>(null);

    const load = React.useCallback(() => {
        opsApi.getTrainingRun(decodeURIComponent(runKey)).then(setDetail).catch((e) => setError(String(e)));
    }, [runKey]);

    React.useEffect(() => {
        load();
        const id = setInterval(load, getRefreshIntervalMs());
        return () => clearInterval(id);
    }, [load]);

    if (error) return <Alert type="error" message={error} />;
    if (!detail) return <Spin />;

    const params = (detail.params as Record<string, unknown>) || {};

    return (
        <div>
            <Text type="secondary">
                <Link to="/">Overview</Link> / Training
            </Text>
            <Card title={`Training · ${String(detail.model_id || runKey)}`} style={{ marginTop: 16 }}>
                <Descriptions column={1} size="small" title="Parameters">
                    {Object.entries(params).map(([k, v]) => (
                        <Descriptions.Item key={k} label={k}>
                            {typeof v === "object" ? JSON.stringify(v) : String(v)}
                        </Descriptions.Item>
                    ))}
                </Descriptions>
                {detail.artifacts != null && (
                    <Descriptions column={1} size="small" title="Artifacts" style={{ marginTop: 16 }}>
                        {Object.entries(detail.artifacts as Record<string, unknown>).map(([k, v]) => (
                            <Descriptions.Item key={k} label={k}>
                                {String(v)}
                            </Descriptions.Item>
                        ))}
                    </Descriptions>
                )}
            </Card>
        </div>
    );
}
