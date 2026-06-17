import React from "react";
import { Alert, Spin, Tabs } from "antd";
import { opsApi, getRefreshIntervalMs } from "../../api/ops";
import type { ChartSpec, OverviewResponse } from "../../types/ops";
import { ChartGrid } from "./components/ChartGrid";

const { TabPane } = Tabs;

export function Metrics() {
    const [overview, setOverview] = React.useState<OverviewResponse | null>(null);
    const [activeId, setActiveId] = React.useState<string>("");
    const [charts, setCharts] = React.useState<ChartSpec[]>([]);
    const [error, setError] = React.useState<string | null>(null);

    React.useEffect(() => {
        opsApi.getOverview().then((o) => {
            setOverview(o);
            if (o.pipelines.length) setActiveId(o.pipelines[0].pipeline_id);
        });
    }, []);

    React.useEffect(() => {
        if (!activeId) return;
        const load = () => {
            opsApi
                .getMetricsCharts(activeId)
                .then((r) => setCharts(r.charts))
                .catch((e) => setError(String(e)));
        };
        load();
        const id = setInterval(load, getRefreshIntervalMs());
        return () => clearInterval(id);
    }, [activeId]);

    if (!overview) return <Spin />;
    if (error) return <Alert type="error" message={error} />;

    return (
        <div>
            <Tabs activeKey={activeId} onChange={setActiveId}>
                {overview.pipelines.map((p) => (
                    <TabPane tab={p.display_name} key={p.pipeline_id} />
                ))}
            </Tabs>
            <ChartGrid charts={charts} />
        </div>
    );
}
