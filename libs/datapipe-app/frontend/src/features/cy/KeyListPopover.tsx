import React from "react";
import type { KeyKind } from "./nodeKeyChips";

export type KeyPopoverState = {
    nodeId: string;
    kind: KeyKind;
    keys: string[];
    anchor: { x: number; y: number };
};

type KeyListPopoverProps = {
    state: KeyPopoverState;
    onClose: () => void;
};

export function KeyListPopover({ state, onClose }: KeyListPopoverProps) {
    const title =
        state.kind === "pk"
            ? `Primary keys · ${state.keys.length}`
            : `Transform Primary Keys · ${state.keys.length}`;

    return (
        <div
            className="node-key-popover"
            style={{
                left: state.anchor.x,
                top: state.anchor.y,
            }}
            role="dialog"
            aria-label={title}
        >
            <div className="node-key-popover-header">
                <strong>{title}</strong>
                <button type="button" onClick={onClose} aria-label="Close">
                    ×
                </button>
            </div>

            <div className="node-key-popover-list">
                {state.keys.map((key) => (
                    <span key={key} className={`node-key-chip ${state.kind}`}>
                        {key}
                    </span>
                ))}
            </div>
        </div>
    );
}
