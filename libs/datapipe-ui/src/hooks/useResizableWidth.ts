import { useCallback, useEffect, useRef, useState } from "react";

type Edge = "left" | "right";

type Options = {
    initial: number;
    min: number;
    max: number;
    storageKey?: string;
    /**
     * Which edge the drag handle lives on.
     * - "right": handle on the element's right edge → dragging right widens (left sidebar).
     * - "left":  handle on the element's left edge  → dragging left widens (right panel).
     */
    edge: Edge;
};

export function useResizableWidth({ initial, min, max, storageKey, edge }: Options) {
    const clamp = useCallback((w: number) => Math.min(max, Math.max(min, w)), [min, max]);

    const [width, setWidth] = useState<number>(() => {
        if (storageKey) {
            const raw = localStorage.getItem(storageKey);
            const saved = raw == null ? NaN : Number(raw);
            if (Number.isFinite(saved)) return Math.min(max, Math.max(min, saved));
        }
        return initial;
    });

    const [dragging, setDragging] = useState(false);
    const dragRef = useRef<{ startX: number; startWidth: number } | null>(null);

    useEffect(() => {
        if (storageKey) localStorage.setItem(storageKey, String(Math.round(width)));
    }, [width, storageKey]);

    useEffect(() => {
        if (!dragging) return undefined;

        const onMove = (event: MouseEvent) => {
            const drag = dragRef.current;
            if (!drag) return;
            const delta = event.clientX - drag.startX;
            const next = edge === "right" ? drag.startWidth + delta : drag.startWidth - delta;
            setWidth(clamp(next));
        };
        const onUp = () => setDragging(false);

        document.addEventListener("mousemove", onMove);
        document.addEventListener("mouseup", onUp);
        const prevCursor = document.body.style.cursor;
        const prevSelect = document.body.style.userSelect;
        document.body.style.cursor = "col-resize";
        document.body.style.userSelect = "none";

        return () => {
            document.removeEventListener("mousemove", onMove);
            document.removeEventListener("mouseup", onUp);
            document.body.style.cursor = prevCursor;
            document.body.style.userSelect = prevSelect;
        };
    }, [dragging, edge, clamp]);

    const onHandleMouseDown = useCallback(
        (event: React.MouseEvent) => {
            event.preventDefault();
            dragRef.current = { startX: event.clientX, startWidth: width };
            setDragging(true);
        },
        [width],
    );

    return { width, dragging, onHandleMouseDown, setWidth };
}
