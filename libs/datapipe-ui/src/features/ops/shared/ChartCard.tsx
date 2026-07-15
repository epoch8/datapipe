import React from "react";
import {
    Bar,
    BarChart,
    CartesianGrid,
    Legend,
    Line,
    LineChart,
    ResponsiveContainer,
    Scatter,
    ScatterChart,
    Tooltip,
    XAxis,
    YAxis,
    ZAxis,
} from "recharts";

export type ChartSeries = {
    key: string;
    label: string;
    color?: string;
    points: { x: string | number; y: number | null; meta?: Record<string, unknown> }[];
};

export type UiChartSpec = {
    id: string;
    title: string;
    type: "line" | "bar" | "scatter" | "area";
    xLabel?: string;
    yLabel?: string;
    series: ChartSeries[];
};

type Props = {
    spec: UiChartSpec;
    height?: number;
    logScale?: boolean;
    extra?: React.ReactNode;
};

function toRechartsData(spec: UiChartSpec): Record<string, unknown>[] {
    const xSet = new Set<string | number>();
    spec.series.forEach((s) => s.points.forEach((p) => xSet.add(p.x)));
    const xs = Array.from(xSet);
    return xs.map((x) => {
        const row: Record<string, unknown> = { x };
        spec.series.forEach((s) => {
            const pt = s.points.find((p) => p.x === x);
            row[s.key] = pt?.y ?? null;
        });
        return row;
    });
}

export function ChartCard({ spec, height = 240, logScale = false, extra }: Props) {
    const data = toRechartsData(spec);

    return (
        <div className="ops-chart-card">
            <div className="ops-chart-card-header">
                <span className="ops-chart-card-title">{spec.title}</span>
                {extra}
            </div>
            <div className="ops-chart-card-body" style={{ height }}>
                <ResponsiveContainer width="100%" height="100%">
                    {spec.type === "bar" ? (
                        <BarChart data={data}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                            <XAxis dataKey="x" tick={{ fontSize: 11 }} />
                            <YAxis scale={logScale ? "log" : "auto"} tick={{ fontSize: 11 }} />
                            <Tooltip />
                            <Legend />
                            {spec.series.map((s) => (
                                <Bar key={s.key} dataKey={s.key} name={s.label} fill={s.color ?? "#116DFF"} />
                            ))}
                        </BarChart>
                    ) : spec.type === "scatter" ? (
                        <ScatterChart>
                            <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                            <XAxis type="number" dataKey="x" name={spec.xLabel} tick={{ fontSize: 11 }} />
                            <YAxis type="number" dataKey="y" name={spec.yLabel} tick={{ fontSize: 11 }} />
                            <ZAxis range={[40, 400]} />
                            <Tooltip cursor={{ strokeDasharray: "3 3" }} />
                            <Legend />
                            {spec.series.map((s) => (
                                <Scatter
                                    key={s.key}
                                    name={s.label}
                                    data={s.points.map((p) => ({ x: p.x, y: p.y }))}
                                    fill={s.color ?? "#116DFF"}
                                />
                            ))}
                        </ScatterChart>
                    ) : (
                        <LineChart data={data}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
                            <XAxis dataKey="x" tick={{ fontSize: 11 }} />
                            <YAxis scale={logScale ? "log" : "auto"} tick={{ fontSize: 11 }} />
                            <Tooltip />
                            <Legend />
                            {spec.series.map((s) => (
                                <Line
                                    key={s.key}
                                    type="monotone"
                                    dataKey={s.key}
                                    name={s.label}
                                    stroke={s.color ?? "#116DFF"}
                                    strokeWidth={2}
                                    dot={false}
                                    connectNulls
                                />
                            ))}
                        </LineChart>
                    )}
                </ResponsiveContainer>
            </div>
        </div>
    );
}
