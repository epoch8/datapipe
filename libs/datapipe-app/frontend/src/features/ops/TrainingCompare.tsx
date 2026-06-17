import React from "react";
import { Alert, Card, Typography } from "antd";
import { useSearchParams } from "react-router-dom";
import { opsApi } from "../../api/ops";
import type { ChartSpec } from "../../types/ops";
import { ChartGrid } from "./components/ChartGrid";

const { Paragraph, Title } = Typography;

export function TrainingCompare() {
    const [params] = useSearchParams();
    const runKeys = params.get("run_keys") || "";
    const [charts, setCharts] = React.useState<ChartSpec[]>([]);
    const [error, setError] = React.useState<string | null>(null);

    React.useEffect(() => {
        if (!runKeys) return;
        fetch(`/api/v1alpha3/training/compare?run_keys=${encodeURIComponent(runKeys)}`)
            .then((r) => r.json())
            .then((d) => setCharts(d.charts || []))
            .catch((e) => setError(String(e)));
    }, [runKeys]);

    if (!runKeys) return <Alert message="No runs selected" type="info" />;
    if (error) return <Alert type="error" message={error} />;

    return (
        <div>
            <Title level={4}>Compare training runs</Title>
            <Paragraph type="secondary">{runKeys}</Paragraph>
            <ChartGrid charts={charts} />
        </div>
    );
}
