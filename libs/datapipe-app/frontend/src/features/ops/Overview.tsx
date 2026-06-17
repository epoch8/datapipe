import React from "react";
import { Alert, Spin } from "antd";
import { opsApi, getRefreshIntervalMs } from "../../api/ops";
import type { OverviewResponse } from "../../types/ops";
import { PipelineCard } from "./components/PipelineCard";

export function Overview() {
    const [data, setData] = React.useState<OverviewResponse | null>(null);
    const [error, setError] = React.useState<string | null>(null);

    const load = React.useCallback(() => {
        opsApi
            .getOverview()
            .then(setData)
            .catch((e) => setError(String(e)));
    }, []);

    React.useEffect(() => {
        load();
        const id = setInterval(load, getRefreshIntervalMs());
        return () => clearInterval(id);
    }, [load]);

    if (error) return <Alert type="error" message={error} />;
    if (!data) return <Spin />;

    return (
        <div>
            {data.pipelines.map((card) => (
                <PipelineCard key={card.pipeline_id} card={card} />
            ))}
            {!data.pipelines.length && <Alert message="No pipelines registered yet" type="info" />}
        </div>
    );
}
