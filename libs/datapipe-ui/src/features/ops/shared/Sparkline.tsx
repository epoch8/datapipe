import React from "react";
import { Line, LineChart, ResponsiveContainer } from "recharts";

type Point = { x: string | number; y: number | null };

type Props = {
    data: Point[];
    color?: string;
    height?: number;
};

export function Sparkline({ data, color = "#116DFF", height = 32 }: Props) {
    const chartData = data.map((p, i) => ({ i, y: p.y ?? 0 }));
    if (!chartData.length) {
        return <div className="ops-sparkline-empty" style={{ height }} />;
    }
    return (
        <div className="ops-sparkline" style={{ height }}>
            <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData}>
                    <Line
                        type="monotone"
                        dataKey="y"
                        stroke={color}
                        strokeWidth={1.5}
                        dot={false}
                        isAnimationActive={false}
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}
