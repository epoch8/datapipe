import React from "react";
import { Alert, Card, Spin } from "antd";
import { opsApi, getRefreshIntervalMs } from "../../../api/ops";
import type { ChartSpec } from "../../../types/ops";
import { ChartGrid } from "./ChartGrid";

export function PipelineMetrics({ pipelineId }: { pipelineId: string }) {
    const [charts, setCharts] = React.useState<ChartSpec[]>([]);
    const [error, setError] = React.useState<string | null>(null);
    const [loading, setLoading] = React.useState(true);

    React.useEffect(() => {
        const load = () => {
            opsApi
                .getMetricsCharts(pipelineId)
                .then((response) => {
                    setCharts(response.charts);
                    setError(null);
                })
                .catch((e) => setError(String(e)))
                .finally(() => setLoading(false));
        };
        load();
        const timer = setInterval(load, getRefreshIntervalMs());
        return () => clearInterval(timer);
    }, [pipelineId]);

    if (loading && !charts.length) return <Spin />;
    if (error) return <Alert type="error" message={error} />;
    if (!charts.length) return null;

    return (
        <Card title="Metrics" style={{ marginBottom: 16 }}>
            <ChartGrid charts={charts} />
        </Card>
    );
}
