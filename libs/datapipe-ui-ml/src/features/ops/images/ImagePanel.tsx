import React from "react";

type Tone = "gt" | "prediction" | "neutral";

export function ImagePanel({
    title,
    tone = "neutral",
    imageUrl,
    label,
}: {
    title: string;
    tone?: Tone;
    imageUrl?: string | null;
    label?: string | null;
}) {
    const toneClass =
        tone === "gt" ? "ops-image-panel-gt" : tone === "prediction" ? "ops-image-panel-prediction" : "";

    return (
        <div className={`ops-image-panel ${toneClass}`.trim()}>
            <div className="ops-image-panel-header">{title}</div>
            <div className="ops-image-panel-body">
                {imageUrl ? <img src={imageUrl} alt={title} /> : <div className="ops-muted">No image</div>}
            </div>
            {label ? (
                <div className={`ops-image-label-badge ops-image-label-badge-${tone}`}>
                    <span className="ops-image-label-badge-caption">
                        {tone === "gt" ? "true label" : tone === "prediction" ? "pred label" : "label"}
                    </span>
                    <span className="ops-image-label-badge-value">{label}</span>
                </div>
            ) : null}
        </div>
    );
}
