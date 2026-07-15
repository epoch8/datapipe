import React from "react";
import { Card, Checkbox, Empty, Typography } from "antd";
import type { ChartSpec } from "../../../types/ops";

const { Text } = Typography;

function SimpleLineChart({ chart }: { chart: ChartSpec }) {
    const [hidden, setHidden] = React.useState<Record<string, boolean>>({});
    const allPoints = chart.series
        .filter((s) => !hidden[s.key])
        .flatMap((s) => s.data.map((d) => Number(d.y)))
        .filter((v) => Number.isFinite(v));
    const minY = allPoints.length ? Math.min(...allPoints) : 0;
    const maxY = allPoints.length ? Math.max(...allPoints) : 1;
    const range = maxY - minY || 1;
    const width = 360;
    const height = 160;
    const pad = 24;

    return (
        <Card size="small" title={chart.title} style={{ minHeight: 220 }}>
            <div style={{ marginBottom: 8 }}>
                {chart.series.map((s) => (
                    <Checkbox
                        key={s.key}
                        checked={!hidden[s.key]}
                        onChange={(e) =>
                            setHidden((prev) => ({ ...prev, [s.key]: !e.target.checked }))
                        }
                        style={{ marginRight: 12 }}
                    >
                        {s.label}
                    </Checkbox>
                ))}
            </div>
            <svg width="100%" viewBox={`0 0 ${width} ${height}`} role="img">
                {chart.series
                    .filter((s) => !hidden[s.key] && s.data.length > 0)
                    .map((s) => {
                        const points = s.data
                            .map((d, i) => {
                                const x =
                                    pad +
                                    (i / Math.max(s.data.length - 1, 1)) * (width - pad * 2);
                                const y =
                                    height -
                                    pad -
                                    ((Number(d.y) - minY) / range) * (height - pad * 2);
                                return `${x},${y}`;
                            })
                            .join(" ");
                        return (
                            <polyline
                                key={s.key}
                                fill="none"
                                stroke={s.key.includes("val") ? "#1890ff" : "#52c41a"}
                                strokeWidth="2"
                                points={points}
                            />
                        );
                    })}
            </svg>
            <Text type="secondary" style={{ fontSize: 12 }}>
                {chart.x.label} → {chart.y.label}
            </Text>
        </Card>
    );
}

export function ChartGrid({ charts }: { charts: ChartSpec[] }) {
    if (!charts.length) {
        return <Empty description="No chart data yet" />;
    }
    return (
        <div
            style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))",
                gap: 16,
            }}
        >
            {charts.map((c) => (
                <SimpleLineChart key={c.chart_id} chart={c} />
            ))}
        </div>
    );
}
