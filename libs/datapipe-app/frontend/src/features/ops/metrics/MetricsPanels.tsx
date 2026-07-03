import React from "react";
import { Tag } from "antd";
import type { MetricsRunRow } from "../../../types/ops";

const INTEGER_METRICS = new Set(["support", "TP", "FP", "FN"]);

// Metrics intentionally hidden from the Best model panel.
const HIDDEN_METRICS = new Set(["images_support"]);

// Precision/Recall/F1 groups rendered as a compact matrix (rows = averaging
// scheme, columns = Precision/Recall/F1). All three columns always show, even
// when a value is missing (rendered as "—").
type PrfGroup = { title: string; precision: string; recall: string; f1: string };

const PRF_GROUPS: PrfGroup[] = [
    { title: "Weighted", precision: "weighted_precision", recall: "weighted_recall", f1: "weighted_f1_score" },
    {
        title: "Weighted (no pseudo)",
        precision: "weighted_without_pseudo_classes_precision",
        recall: "weighted_without_pseudo_classes_recall",
        f1: "weighted_without_pseudo_classes_f1_score",
    },
    { title: "Macro", precision: "macro_precision", recall: "macro_recall", f1: "macro_f1_score" },
    {
        title: "Macro (no pseudo)",
        precision: "macro_without_pseudo_classes_precision",
        recall: "macro_without_pseudo_classes_recall",
        f1: "macro_without_pseudo_classes_f1_score",
    },
    { title: "Overall", precision: "precision", recall: "recall", f1: "f1_score" },
];

// Single-value metrics shown as compact chips below the matrix.
const SCALAR_METRICS: string[] = [
    "accuracy",
    "mAP50",
    "mAP50_95",
    "iou_mean",
    "TP",
    "FP",
    "FN",
    "support",
];

const SCALAR_LABELS: Record<string, string> = {
    accuracy: "Accuracy",
    mAP50: "mAP50",
    mAP50_95: "mAP50-95",
    iou_mean: "IoU",
    TP: "TP",
    FP: "FP",
    FN: "FN",
    support: "Support",
};

function formatMetric(key: string, value: number | null | undefined): string {
    if (value == null) return "—";
    if (INTEGER_METRICS.has(key)) return String(Math.round(value));
    return value.toFixed(3);
}

export function BestModelPanel({ run }: { run?: MetricsRunRow }) {
    if (!run) return null;
    const metrics = run.metrics ?? {};
    const present = (k: string) => !HIDDEN_METRICS.has(k) && metrics[k] != null;

    // Show a PRF row if any of its three cells exists in the data.
    const prfRows = PRF_GROUPS.filter(
        (g) => present(g.precision) || present(g.recall) || present(g.f1),
    );

    const scalarKeys = SCALAR_METRICS.filter(present);
    const knownScalar = new Set(SCALAR_METRICS);
    const knownPrf = new Set(PRF_GROUPS.flatMap((g) => [g.precision, g.recall, g.f1]));
    const otherKeys = Object.keys(metrics).filter(
        (k) => present(k) && !knownScalar.has(k) && !knownPrf.has(k),
    );

    const duration =
        run.duration_s != null
            ? `${Math.floor(run.duration_s / 60)}m ${run.duration_s % 60}s`
            : null;

    return (
        <div className="ops-panel ops-best-model-panel">
            <div className="ops-best-model-head">
                <div>
                    <div className="ops-panel-title">Best model</div>
                    <div className="ops-best-model-id" title={run.model_id}>
                        {run.model_id}
                        {run.model_version && <Tag style={{ marginLeft: 8 }}>{run.model_version}</Tag>}
                    </div>
                </div>
                <div className="ops-best-model-meta">
                    {run.subset && <span>Subset: <Tag>{run.subset}</Tag></span>}
                    <span>Run: <strong>{run.run_id}</strong></span>
                    {run.started_at && <span>Started: <strong>{run.started_at.slice(0, 16).replace("T", " ")}</strong></span>}
                    {duration && <span>Duration: <strong>{duration}</strong></span>}
                </div>
            </div>

            {prfRows.length > 0 && (
                <table className="ops-best-model-matrix">
                    <thead>
                        <tr>
                            <th />
                            <th>Precision</th>
                            <th>Recall</th>
                            <th>F1</th>
                        </tr>
                    </thead>
                    <tbody>
                        {prfRows.map((g) => (
                            <tr key={g.title}>
                                <th scope="row">{g.title}</th>
                                <td>{formatMetric(g.precision, metrics[g.precision])}</td>
                                <td>{formatMetric(g.recall, metrics[g.recall])}</td>
                                <td>{formatMetric(g.f1, metrics[g.f1])}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            )}

            {(scalarKeys.length > 0 || otherKeys.length > 0) && (
                <div className="ops-best-model-scalars">
                    {[...scalarKeys, ...otherKeys].map((key) => (
                        <div key={key} className="ops-best-model-scalar">
                            <span className="ops-best-model-scalar-label">{SCALAR_LABELS[key] ?? key}</span>
                            <span className="ops-best-model-scalar-value">{formatMetric(key, metrics[key])}</span>
                        </div>
                    ))}
                </div>
            )}
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
