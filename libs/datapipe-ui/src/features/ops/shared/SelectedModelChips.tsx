import React from "react";

type Props = {
    modelIds: string[];
    onRemove: (modelId: string) => void;
};

export function SelectedModelChips({ modelIds, onRemove }: Props) {
    if (!modelIds.length) return null;

    return (
        <div className="ops-selected-model-chips" role="list" aria-label="Selected models">
            {modelIds.map((id) => (
                <span key={id} className="ops-model-chip" role="listitem" title={id}>
                    <span className="ops-model-chip-text">{id}</span>
                    <button
                        type="button"
                        className="ops-model-chip-close"
                        aria-label={`Remove ${id}`}
                        onClick={() => onRemove(id)}
                    >
                        ×
                    </button>
                </span>
            ))}
        </div>
    );
}
