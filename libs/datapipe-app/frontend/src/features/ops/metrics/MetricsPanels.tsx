import React from "react";
import { Tag } from "antd";
import { RightOutlined } from "@ant-design/icons";
import { Link } from "react-router-dom";

const STAGES = [
    { name: "annotation", status: "completed", duration: "2m 18s" },
    { name: "ls-sync", status: "completed", duration: "1m 05s" },
    { name: "train-prepare", status: "completed", duration: "3m 22s" },
    { name: "train", status: "completed", duration: "32m 41s" },
    { name: "inference", status: "completed", duration: "8m 11s" },
    { name: "count-metrics", status: "completed", duration: "1m 14s" },
    { name: "fiftyone", status: "running", duration: "2m 45s" },
];

type Props = { pipelineId: string };

export function PipelineStagesStrip({ pipelineId }: Props) {
    return (
        <div className="ops-stages-strip">
            {STAGES.map((stage, i) => (
                <React.Fragment key={stage.name}>
                    <div className={`ops-stage-item ${stage.status}`}>
                        <strong>{stage.name}</strong>
                        <span>{stage.status}</span>
                        <span>{stage.duration}</span>
                    </div>
                    {i < STAGES.length - 1 && <RightOutlined className="ops-stage-arrow" />}
                </React.Fragment>
            ))}
            <Link to={`/pipelines/${pipelineId}`} style={{ marginLeft: "auto", whiteSpace: "nowrap" }}>
                View pipeline →
            </Link>
        </div>
    );
}

export function BestRunPanel({ run }: { run?: { run_id?: string; model_id?: string; subset?: string; metrics?: Record<string, number | null> } }) {
    if (!run) return null;
    return (
        <div className="ops-panel">
            <div className="ops-panel-title">Best run (test)</div>
            <div style={{ fontSize: 12, color: "var(--dp-gray-500)" }}>
                <div>Run: <strong>{run.run_id}</strong></div>
                <div>Model: <strong>{run.model_id}</strong></div>
                <div>Subset: <Tag>{run.subset}</Tag></div>
                {run.metrics && (
                    <div style={{ marginTop: 8 }}>
                        mAP50: <strong>{run.metrics.mAP50?.toFixed(3)}</strong>
                        {" · "}
                        F1: <strong>{run.metrics.f1_score?.toFixed(3)}</strong>
                    </div>
                )}
            </div>
        </div>
    );
}

export function AnomaliesPanel({ anomalies }: { anomalies: { severity: string; title: string; description: string }[] }) {
    return (
        <div className="ops-panel">
            <div className="ops-panel-title">Anomalies ({anomalies.length})</div>
            {anomalies.map((a, i) => (
                <div key={i} className={`ops-anomaly-item ops-anomaly-${a.severity}`}>
                    <strong>{a.title}</strong>
                    <div>{a.description}</div>
                </div>
            ))}
        </div>
    );
}

export function MetricLegend() {
    const items = [
        { color: "#116DFF", label: "mAP50" },
        { color: "#705CFF", label: "Precision" },
        { color: "#16A34A", label: "Recall" },
        { color: "#F97316", label: "F1" },
    ];
    return (
        <div className="ops-panel">
            <div className="ops-panel-title">Metric legend</div>
            {items.map((item) => (
                <div key={item.label} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12, marginBottom: 6 }}>
                    <span style={{ width: 12, height: 3, background: item.color, borderRadius: 2 }} />
                    {item.label}
                </div>
            ))}
        </div>
    );
}
