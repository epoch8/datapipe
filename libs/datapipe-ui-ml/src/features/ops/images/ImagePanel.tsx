import React from "react";

type Tone = "gt" | "prediction" | "neutral";

export function ImagePanel({
    title,
    tone = "neutral",
    imageUrl,
}: {
    title: string;
    tone?: Tone;
    imageUrl?: string | null;
}) {
    const toneClass =
        tone === "gt" ? "ops-image-panel-gt" : tone === "prediction" ? "ops-image-panel-prediction" : "";

    return (
        <div className={`ops-image-panel ${toneClass}`.trim()}>
            <div className="ops-image-panel-header">{title}</div>
            <div className="ops-image-panel-body">
                {imageUrl ? <img src={imageUrl} alt={title} /> : <div className="ops-muted">No image</div>}
            </div>
        </div>
    );
}

