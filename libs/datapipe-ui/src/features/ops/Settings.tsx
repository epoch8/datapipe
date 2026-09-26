import React from "react";
import { Alert, Card, InputNumber, Slider, Typography } from "antd";
import { opsApi } from "../../api/client";
import type { SettingsInfo } from "../../types/ops";

const { Paragraph, Title } = Typography;

export function Settings() {
    const [info, setInfo] = React.useState<SettingsInfo | null>(null);
    const [refresh, setRefresh] = React.useState(30);

    React.useEffect(() => {
        opsApi.getSettings().then(setInfo);
        const stored = localStorage.getItem("datapipe_ops_refresh_s");
        if (stored) setRefresh(parseInt(stored, 10));
    }, []);

    const onRefreshChange = (v: number) => {
        setRefresh(v);
        localStorage.setItem("datapipe_ops_refresh_s", String(v));
    };

    return (
        <Card>
            <Title level={4}>Settings</Title>
            <Paragraph>Refresh interval (seconds)</Paragraph>
            <Slider min={15} max={120} value={refresh} onChange={onRefreshChange} />
            <InputNumber min={15} max={120} value={refresh} onChange={(v) => onRefreshChange(v || 30)} />
            {info && (
                <div style={{ marginTop: 24 }}>
                    <Paragraph>
                        Pipeline ID: <strong>{info.pipeline_id || "—"}</strong>
                    </Paragraph>
                    <Paragraph>
                        Observability DB:{" "}
                        {info.observability_db_connected ? (
                            <span style={{ color: "green" }}>connected</span>
                        ) : (
                            <span style={{ color: "red" }}>disconnected</span>
                        )}
                    </Paragraph>
                    <Paragraph>Version: {info.version}</Paragraph>
                    {!info.run_logs_configured && (
                        <Alert
                            style={{ marginTop: 12 }}
                            type="warning"
                            showIcon
                            message="Run logs are not being recorded"
                            description="Pass run_logs_backend to DatapipeAPI (e.g. RunLogsBackend.clickhouse(...))."
                        />
                    )}
                </div>
            )}
            <Alert style={{ marginTop: 16 }} message="Notifications are disabled in v1" type="info" />
        </Card>
    );
}
