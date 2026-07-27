import Cytoscape from "cytoscape";
import type { KeyPopoverState } from "./KeyListPopover";
import { formatNodeLabels, getTransformPrimaryKeys } from "./nodeKeyChips";
import {
    applyGraphVisualState,
    clearGraphFocus,
    clearSelectedNodeIds,
    getSelectedNodeIds,
    isNodeSelected,
    setSelectedNodeIds,
    toggleSelectedNodeId,
} from "./graphVisualState";

const BG_CLEAR_MOVE_PX = 5;
const NODE_PAN_MOVE_PX = 14;
const BG_CLEAR_MAX_MS = 280;

const MIN_ZOOM = 0.05;
const MAX_ZOOM = 6.0;
const WHEEL_ZOOM_SPEED = 3.3;
const FAST_WHEEL_ZOOM_SPEED = 6.9;
const WHEEL_DELTA_CLAMP = 36;
const WHEEL_ZOOM_COEFF = 0.0022;

export type GraphInteractionCallbacks = {
    onOpenInspector: (node: Cytoscape.NodeSingular) => void;
    onCloseInspector: () => void;
    onOpenKeyPopover: (state: KeyPopoverState) => void;
    onCloseKeyPopover: () => void;
    onToggleGroup: (groupName: string) => void;
    onUserInteracted: () => void;
    /** Last RMB target — used so menu actions don't rely on cytoscape-context-menus' scratch event. */
    onContextTarget?: (node: Cytoscape.NodeSingular) => void;
};

function canSelectNode(node: Cytoscape.NodeSingular): boolean {
    return (node.data("type") as string) !== "group-expanded";
}

function modelPosFromClient(
    cy: Cytoscape.Core,
    container: HTMLElement,
    clientX: number,
    clientY: number,
): { x: number; y: number; rendered: { x: number; y: number } } {
    const rect = container.getBoundingClientRect();
    const pan = cy.pan();
    const zoom = cy.zoom();
    const rendered = { x: clientX - rect.left, y: clientY - rect.top };
    return {
        x: (rendered.x - pan.x) / zoom,
        y: (rendered.y - pan.y) / zoom,
        rendered,
    };
}

/** Expanded blue frame containing a model-space point (events:no, so no native hits). */
function findExpandedFrameAt(
    cy: Cytoscape.Core,
    x: number,
    y: number,
): Cytoscape.NodeSingular | null {
    let frame: Cytoscape.NodeSingular | null = null;
    let bestArea = Infinity;
    cy.nodes('node[type = "group-expanded"]').forEach((ele) => {
        if (!ele.visible()) return;
        const node = ele as Cytoscape.NodeSingular;
        const bb = node.boundingBox({ includeLabels: false, includeOverlays: false });
        if (x < bb.x1 || x > bb.x2 || y < bb.y1 || y > bb.y2) return;
        // Prefer the tightest (nested) frame if several overlap.
        const area = bb.w * bb.h;
        if (area < bestArea) {
            bestArea = area;
            frame = node;
        }
    });
    return frame;
}

/** Content card under a point when the HTML label did not receive the event. */
function findContentNodeAt(
    cy: Cytoscape.Core,
    x: number,
    y: number,
): Cytoscape.NodeSingular | null {
    let hit: Cytoscape.NodeSingular | null = null;
    let bestArea = Infinity;
    cy.nodes().forEach((ele) => {
        if (!ele.visible()) return;
        const node = ele as Cytoscape.NodeSingular;
        const type = node.data("type") as string;
        if (type === "group-expanded") return;
        const bb = node.boundingBox({ includeLabels: false, includeOverlays: false });
        if (x < bb.x1 || x > bb.x2 || y < bb.y1 || y > bb.y2) return;
        const area = bb.w * bb.h;
        if (area < bestArea) {
            bestArea = area;
            hit = node;
        }
    });
    return hit;
}

/**
 * Resolve the context-menu/dblclick target for a container pointer event.
 * HTML cards win; then a content node BB; empty expanded-frame chrome last.
 * Preferring the frame over content made RMB on a step/table inside an expanded
 * meta open the group menu (Open details no-op for group-expanded).
 */
function resolvePointerTarget(
    cy: Cytoscape.Core,
    container: HTMLElement,
    event: MouseEvent,
): Cytoscape.NodeSingular | null {
    const fromLabel = (() => {
        const label = (event.target as Element | null)?.closest?.("[data-cy-node-id]");
        if (!label) return null;
        const nodeId = label.getAttribute("data-cy-node-id");
        if (!nodeId) return null;
        const node = cy.getElementById(nodeId);
        if (node.empty()) return null;
        return node as Cytoscape.NodeSingular;
    })();
    if (fromLabel) return fromLabel;

    const pos = modelPosFromClient(cy, container, event.clientX, event.clientY);
    const content = findContentNodeAt(cy, pos.x, pos.y);
    if (content) return content;
    return findExpandedFrameAt(cy, pos.x, pos.y);
}

function isContextMenuTarget(target: EventTarget | null): boolean {
    return Boolean(
        (target as Element | null)?.closest?.(
            ".cy-context-menus-cxt-menu, .cy-context-menus-cxt-menuitem",
        ),
    );
}

/**
 * Expanded frames use `events: no`, so native RMB ends as core `cxttap` on mouseup
 * when the cursor is over empty chrome (esp. the title padding band). That second
 * `cxttap` resets cytoscape-context-menus and hides the menu we opened via a
 * synthetic node emit. Mark the gesture as a cxt-drag so mouseup skips `cxttap`.
 */
function suppressUpcomingNativeCxttap(cy: Cytoscape.Core): void {
    // cytoscape Core typings omit renderer(); it's the public accessor used elsewhere.
    const renderer = (
        cy as Cytoscape.Core & {
            renderer?: () => { hoverData?: { cxtDragged?: boolean } };
        }
    ).renderer?.();
    if (renderer?.hoverData) {
        renderer.hoverData.cxtDragged = true;
    }
}

/**
 * Attach all pointer / wheel / tap handlers for pipeline graph selection.
 * Returns a cleanup function.
 */
export function attachGraphInteractions(
    cy: Cytoscape.Core,
    callbacks: GraphInteractionCallbacks,
): () => void {
    const container = cy.container();
    if (!container) return () => undefined;

    const chipScrollStore = new Map<string, number>();
    const chipScrollKey = (nodeId: string, kind: string) => `${nodeId}::${kind}`;
    let ignoreChipScrollMemory = false;

    const findChipScroller = (nodeId: string, kind: string): HTMLElement | null =>
        container.querySelector(
            `.node-key-chips[data-cy-node-id="${CSS.escape(nodeId)}"][data-key-kind="${kind}"]`,
        );

    const rememberChipScroll = (el: HTMLElement) => {
        if (ignoreChipScrollMemory) return;
        const nodeId = el.getAttribute("data-cy-node-id");
        const kind = el.getAttribute("data-key-kind");
        if (nodeId && kind) chipScrollStore.set(chipScrollKey(nodeId, kind), el.scrollLeft);
    };

    const onChipScroll = (event: Event) => {
        const el = event.target as HTMLElement | null;
        if (el?.classList?.contains("node-key-chips")) rememberChipScroll(el);
    };
    container.addEventListener("scroll", onChipScroll, true);

    let restoreFrame = 0;
    const restoreChipScrolls = () => {
        cancelAnimationFrame(restoreFrame);
        restoreFrame = requestAnimationFrame(() => {
            ignoreChipScrollMemory = true;
            chipScrollStore.forEach((left, key) => {
                const [nodeId, kind] = key.split("::");
                const el = findChipScroller(nodeId, kind);
                if (el && Math.abs(el.scrollLeft - left) > 0.5) el.scrollLeft = left;
            });
            requestAnimationFrame(() => {
                chipScrollStore.forEach((left, key) => {
                    const [nodeId, kind] = key.split("::");
                    const el = findChipScroller(nodeId, kind);
                    if (el && Math.abs(el.scrollLeft - left) > 0.5) el.scrollLeft = left;
                });
                ignoreChipScrollMemory = false;
            });
        });
    };
    cy.on("render", restoreChipScrolls);

    const resolveOverflowChip = (target: EventTarget | null): HTMLElement | null =>
        (target as Element | null)?.closest?.("[data-key-overflow='true']") as HTMLElement | null;

    const resolveNodeFromLabel = (target: EventTarget | null): Cytoscape.NodeSingular | null => {
        const label = (target as Element | null)?.closest?.("[data-cy-node-id]");
        if (!label) return null;
        const nodeId = label.getAttribute("data-cy-node-id");
        if (!nodeId) return null;
        const node = cy.getElementById(nodeId);
        if (node.empty()) return null;
        return node as Cytoscape.NodeSingular;
    };

    const openKeyPopoverFromChip = (overflow: HTMLElement) => {
        const nodeId = overflow.getAttribute("data-cy-node-id");
        const kind = overflow.getAttribute("data-key-kind") as "pk" | "tpk" | "label" | null;
        if (!nodeId || !kind) return;

        const node = cy.getElementById(nodeId);
        if (node.empty()) return;

        const data = node.data();
        const keys =
            kind === "pk"
                ? ((data.indexes as string[]) ?? [])
                : kind === "label"
                  ? formatNodeLabels((data.labels as string[][]) ?? [])
                  : getTransformPrimaryKeys(data);

        const containerRect = container.getBoundingClientRect();
        const chipRect = overflow.getBoundingClientRect();
        callbacks.onOpenKeyPopover({
            nodeId,
            kind,
            keys,
            anchor: {
                x: chipRect.left - containerRect.left,
                y: chipRect.bottom - containerRect.top + 8,
            },
        });
    };

    const handleNodeSelect = (node: Cytoscape.NodeSingular, multi: boolean) => {
        if (!canSelectNode(node)) return;
        if (multi) {
            if (isNodeSelected(cy, node.id())) {
                toggleSelectedNodeId(cy, node.id());
                callbacks.onCloseInspector();
            } else {
                toggleSelectedNodeId(cy, node.id());
                callbacks.onOpenInspector(node);
            }
        } else {
            const selected = getSelectedNodeIds(cy);
            const alreadySole = selected.length === 1 && selected[0] === node.id();
            if (!alreadySole) setSelectedNodeIds(cy, [node.id()]);
            else applyGraphVisualState(cy);
            callbacks.onOpenInspector(node);
        }
    };

    type BgPress = { x: number; y: number; moved: boolean; downAt: number };
    let bgPress: BgPress | null = null;
    let multiSelectSnapshot: string[] | null = null;
    let nodePress: { nodeId: string; x: number; y: number } | null = null;
    let nodePan: {
        nodeId: string;
        startX: number;
        startY: number;
        startPan: { x: number; y: number };
        moved: boolean;
        selectedAtStart: string[];
    } | null = null;
    let ignoreNextSelect = false;
    let selectionFreeze: string[] | null = null;
    let pressStartedOnOverflow = false;
    /**
     * One-shot per pointer gesture: mouseup/click/cy-tap can all try to select the
     * same press. A time window (TAP_DEDUPE_MS) was wrong — it also blocked the
     * *next* intentional select after a quick deselect. Reset on every mousedown.
     */
    let selectHandledThisGesture = false;
    let chipDrag: {
        kind: string;
        scrollNodeId: string;
        startX: number;
        startScrollLeft: number;
        moved: boolean;
        nodeId: string | null;
    } | null = null;

    /** One-shot: swallow the trailing click/tap after a drag-pan or scrollbar gesture. */
    const consumeSelectSuppress = (): boolean => {
        if (!ignoreNextSelect) return false;
        ignoreNextSelect = false;
        return true;
    };

    const armSelectSuppress = () => {
        ignoreNextSelect = true;
        nodePress = null;
    };

    const restoreFrozenSelection = () => {
        if (selectionFreeze == null) return;
        setSelectedNodeIds(cy, selectionFreeze);
    };

    const runSelectOnce = (node: Cytoscape.NodeSingular, multi: boolean) => {
        if (consumeSelectSuppress()) {
            restoreFrozenSelection();
            return;
        }
        if (selectHandledThisGesture) return;
        selectHandledThisGesture = true;
        handleNodeSelect(node, multi);
    };

    const onChipDragMove = (event: MouseEvent) => {
        if (!chipDrag) return;
        const dx = event.clientX - chipDrag.startX;
        if (Math.abs(dx) > NODE_PAN_MOVE_PX) chipDrag.moved = true;
        const el = findChipScroller(chipDrag.scrollNodeId, chipDrag.kind);
        if (el) {
            el.scrollLeft = chipDrag.startScrollLeft - dx;
            rememberChipScroll(el);
        }
        event.preventDefault();
        event.stopPropagation();
    };

    const onChipDragEnd = (event: MouseEvent) => {
        document.removeEventListener("mousemove", onChipDragMove, true);
        document.removeEventListener("mouseup", onChipDragEnd, true);
        const drag = chipDrag;
        chipDrag = null;
        if (!drag) return;
        findChipScroller(drag.scrollNodeId, drag.kind)?.classList.remove("is-grabbing");
        event.preventDefault();
        event.stopPropagation();
        if (!drag.moved && drag.nodeId && !ignoreNextSelect) {
            const node = cy.getElementById(drag.nodeId);
            if (!node.empty() && canSelectNode(node as Cytoscape.NodeSingular)) {
                runSelectOnce(node as Cytoscape.NodeSingular, event.ctrlKey || event.metaKey);
            }
        }
    };

    const onNodePanMove = (event: MouseEvent) => {
        if (!nodePan) return;
        const dx = event.clientX - nodePan.startX;
        const dy = event.clientY - nodePan.startY;
        if (!nodePan.moved) {
            if (dx * dx + dy * dy <= NODE_PAN_MOVE_PX * NODE_PAN_MOVE_PX) return;
            nodePan.moved = true;
            callbacks.onUserInteracted();
        }
        cy.pan({ x: nodePan.startPan.x + dx, y: nodePan.startPan.y + dy });
        event.preventDefault();
    };

    const onNodePanEnd = (event: MouseEvent) => {
        document.removeEventListener("mousemove", onNodePanMove, true);
        document.removeEventListener("mouseup", onNodePanEnd, true);
        const pan = nodePan;
        nodePan = null;
        if (!pan) return;
        event.preventDefault();
        event.stopPropagation();
        if (pan.moved) {
            selectionFreeze = pan.selectedAtStart;
            armSelectSuppress();
            restoreFrozenSelection();
            return;
        }
        const node = cy.getElementById(pan.nodeId);
        if (!node.empty() && canSelectNode(node as Cytoscape.NodeSingular)) {
            runSelectOnce(node as Cytoscape.NodeSingular, event.ctrlKey || event.metaKey);
        }
    };

    const onDocumentMouseMove = (event: MouseEvent) => {
        if (!bgPress) return;
        const dx = event.clientX - bgPress.x;
        const dy = event.clientY - bgPress.y;
        if (dx * dx + dy * dy > BG_CLEAR_MOVE_PX * BG_CLEAR_MOVE_PX) bgPress.moved = true;
    };

    const endBackgroundPress = (event: MouseEvent) => {
        document.removeEventListener("mousemove", onDocumentMouseMove, true);
        document.removeEventListener("mouseup", endBackgroundPress, true);

        if (event.button !== 0 || !bgPress) {
            bgPress = null;
            multiSelectSnapshot = null;
            return;
        }

        const press = bgPress;
        bgPress = null;
        const snapshot = multiSelectSnapshot;
        multiSelectSnapshot = null;

        const releasedOnNode = Boolean(resolveNodeFromLabel(event.target));
        const releasedOnCanvas = container.contains(event.target as Node);
        const duration = performance.now() - press.downAt;
        const isShortClick = !press.moved && duration <= BG_CLEAR_MAX_MS;
        const shouldClear =
            releasedOnCanvas &&
            !releasedOnNode &&
            isShortClick &&
            getSelectedNodeIds(cy).length > 0;

        if (shouldClear) {
            clearSelectedNodeIds(cy);
            clearGraphFocus(cy);
            callbacks.onCloseInspector();
            callbacks.onCloseKeyPopover();
            return;
        }

        if (snapshot && snapshot.length > 1) {
            setSelectedNodeIds(cy, snapshot);
        }
    };

    const onContainerMouseDown = (event: MouseEvent) => {
        if (event.button !== 0) return;
        // Context-menu items live inside the cy container; do not treat their
        // LMB as background/node press (that clears currentCyEvent / selection).
        if (isContextMenuTarget(event.target)) return;
        pressStartedOnOverflow = false;
        nodePress = null;
        selectionFreeze = null;
        // New intentional press — drop any leftover one-shot suppress from a
        // previous gesture. The trailing click after a drag has no new mousedown
        // before it, so it still sees ignoreNextSelect=true.
        ignoreNextSelect = false;
        selectHandledThisGesture = false;

        const scroller = (event.target as Element | null)?.closest?.(
            ".node-key-chips",
        ) as HTMLElement | null;
        if (scroller) {
            const canScroll = scroller.scrollWidth > scroller.clientWidth;
            const rect = scroller.getBoundingClientRect();
            const sbH = Math.max(scroller.offsetHeight - scroller.clientHeight, 10);
            const inScrollbarBand =
                canScroll &&
                event.clientY >= rect.bottom - sbH &&
                event.clientY <= rect.bottom + 2;

            // Only the real scrollbar thumb/track should freeze selection. Clicks on
            // chip gaps / scrollbar-gutter used to armSelectSuppress and keep the
            // *previous* node selected (orange stays selected when aiming at green).
            if (inScrollbarBand) {
                rememberChipScroll(scroller);
                selectionFreeze = getSelectedNodeIds(cy);
                armSelectSuppress();
                event.stopPropagation();
                return;
            }

            if (canScroll) {
                const scrollNodeId = scroller.getAttribute("data-cy-node-id");
                const kind = scroller.getAttribute("data-key-kind");
                if (scrollNodeId && kind) {
                    // Drag scrolls chips; a press without move selects this node
                    // (chip, gap, or gutter — all belong to the card).
                    chipDrag = {
                        kind,
                        scrollNodeId,
                        startX: event.clientX,
                        startScrollLeft: scroller.scrollLeft,
                        moved: false,
                        nodeId: scrollNodeId,
                    };
                    scroller.classList.add("is-grabbing");
                    event.preventDefault();
                    event.stopPropagation();
                    document.addEventListener("mousemove", onChipDragMove, true);
                    document.addEventListener("mouseup", onChipDragEnd, true);
                    return;
                }
            }
            // Non-scrollable chip row: fall through to normal node select via
            // data-cy-node-id on the scroller.
        }

        const overflow = resolveOverflowChip(event.target);
        if (overflow) {
            pressStartedOnOverflow = true;
            event.preventDefault();
            event.stopPropagation();
            openKeyPopoverFromChip(overflow);
            return;
        }

        const nodeAtDown = resolveNodeFromLabel(event.target);
        if (nodeAtDown) {
            event.preventDefault();
            event.stopPropagation();
            nodePress = { nodeId: nodeAtDown.id(), x: event.clientX, y: event.clientY };
            const pan = cy.pan();
            nodePan = {
                nodeId: nodeAtDown.id(),
                startX: event.clientX,
                startY: event.clientY,
                startPan: { x: pan.x, y: pan.y },
                moved: false,
                selectedAtStart: getSelectedNodeIds(cy),
            };
            document.addEventListener("mousemove", onNodePanMove, true);
            document.addEventListener("mouseup", onNodePanEnd, true);
            return;
        }

        if (!container.contains(event.target as Node)) return;

        const selected = getSelectedNodeIds(cy);
        multiSelectSnapshot = selected.length > 1 ? selected : null;
        bgPress = { x: event.clientX, y: event.clientY, moved: false, downAt: performance.now() };
        document.addEventListener("mousemove", onDocumentMouseMove, true);
        document.addEventListener("mouseup", endBackgroundPress, true);
    };

    const onCyNodeTap = (event: Cytoscape.EventObject) => {
        if (consumeSelectSuppress()) {
            restoreFrozenSelection();
            return;
        }
        const node = event.target as Cytoscape.NodeSingular;
        if (!canSelectNode(node)) return;
        const original = event.originalEvent as MouseEvent | undefined;
        if (original?.button != null && original.button !== 0) return;
        if (pressStartedOnOverflow || resolveOverflowChip(original?.target ?? null)) return;
        runSelectOnce(node, Boolean(original?.ctrlKey || original?.metaKey));
    };

    const onContainerClick = (event: MouseEvent) => {
        if (event.button !== 0) return;
        if (isContextMenuTarget(event.target)) return;
        if (consumeSelectSuppress()) {
            nodePress = null;
            restoreFrozenSelection();
            event.preventDefault();
            event.stopPropagation();
            return;
        }
        if (resolveOverflowChip(event.target)) {
            event.preventDefault();
            event.stopPropagation();
            nodePress = null;
            return;
        }
        // mouseup already selected this press — swallow the trailing click.
        if (selectHandledThisGesture) {
            nodePress = null;
            return;
        }
        let node = resolveNodeFromLabel(event.target);
        if (!node && nodePress) {
            const dx = event.clientX - nodePress.x;
            const dy = event.clientY - nodePress.y;
            if (dx * dx + dy * dy <= NODE_PAN_MOVE_PX * NODE_PAN_MOVE_PX) {
                const candidate = cy.getElementById(nodePress.nodeId);
                if (!candidate.empty()) node = candidate as Cytoscape.NodeSingular;
            }
        }
        nodePress = null;
        if (!node) return;
        event.preventDefault();
        event.stopPropagation();
        runSelectOnce(node, event.ctrlKey || event.metaKey);
    };

    const onContainerMouseOut = (event: MouseEvent) => {
        const related = event.relatedTarget as Node | null;
        if (related && container.contains(related)) return;
        if (getSelectedNodeIds(cy).length > 0) return;
        clearGraphFocus(cy);
    };

    const onDblTapGroup = (event: Cytoscape.EventObject) => {
        const node = event.target as Cytoscape.NodeSingular;
        const type = node.data("type") as string;
        if (type === "group" || type === "group-expanded") {
            callbacks.onToggleGroup(node.data("name") as string);
        }
    };

    // HTML labels sit above the canvas and swallow the browser contextmenu, so
    // cytoscape-context-menus never sees a native cxttap. Forward RMB on labels
    // and on empty areas of the (events:no) expanded blue frame.
    const onContainerContextMenu = (event: MouseEvent) => {
        const node = resolvePointerTarget(cy, container, event);
        if (!node) return;
        event.preventDefault();
        event.stopPropagation();
        // Skip native mouseup→core cxttap that would wipe this menu on empty
        // frame chrome (title band has no events:yes node under the cursor).
        suppressUpcomingNativeCxttap(cy);
        callbacks.onContextTarget?.(node);

        // Plugin only *shows* matching items; it does not hide the rest unless the
        // menu position object changes. Clear every item first so Collapse cannot
        // linger from a previous RMB on the blue frame.
        container.querySelectorAll(".cy-context-menus-cxt-menuitem").forEach((el) => {
            (el as HTMLElement).style.display = "none";
        });

        const pos = modelPosFromClient(cy, container, event.clientX, event.clientY);
        // Cytoscape emit accepts a plain event object with position fields.
        // @ts-expect-error cytoscape EventNames overload misses the object form
        node.emit({
            type: "cxttap",
            position: { x: pos.x, y: pos.y },
            renderedPosition: pos.rendered,
            originalEvent: event,
        });

        // Plugin corner-flip math often drifts for HTML-label hits (esp. on the
        // right half of the canvas). Pin the menu under the cursor instead.
        // Also enforce item visibility from the resolved target (not stale selectors).
        queueMicrotask(() => {
            const menu = container.querySelector(
                ".cy-context-menus-cxt-menu",
            ) as HTMLElement | null;
            if (!menu || getComputedStyle(menu).display === "none") return;
            const rect = container.getBoundingClientRect();
            menu.style.left = `${event.clientX - rect.left}px`;
            menu.style.top = `${event.clientY - rect.top}px`;
            menu.style.right = "auto";
            menu.style.bottom = "auto";

            const type = node.data("type") as string;
            const openDetails = menu.querySelector("#open-details") as HTMLElement | null;
            const expand = menu.querySelector("#expand-steps") as HTMLElement | null;
            const collapse = menu.querySelector("#collapse-steps") as HTMLElement | null;
            if (openDetails) openDetails.style.display = "";
            if (expand) expand.style.display = type === "group" ? "" : "none";
            if (collapse) collapse.style.display = type === "group-expanded" ? "" : "none";
        });
    };

    // Double-click: HTML group labels, or empty area of an expanded blue frame
    // (group-expanded uses events:no so dbltap does not fire there).
    const onContainerDblClick = (event: MouseEvent) => {
        const node = resolvePointerTarget(cy, container, event);
        if (!node) return;
        const type = node.data("type") as string;
        if (type === "group" || type === "group-expanded") {
            event.preventDefault();
            event.stopPropagation();
            callbacks.onToggleGroup(node.data("name") as string);
        }
    };

    const onContainerWheel = (event: WheelEvent) => {
        const scroller = (event.target as Element | null)?.closest?.(
            ".node-key-chips",
        ) as HTMLElement | null;
        if (scroller) {
            if (scroller.scrollWidth <= scroller.clientWidth) return;
            const delta =
                Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
            if (delta === 0) return;
            scroller.scrollLeft += delta;
            rememberChipScroll(scroller);
            event.preventDefault();
            event.stopPropagation();
            return;
        }

        event.preventDefault();
        callbacks.onUserInteracted();
        const raw = Math.max(-WHEEL_DELTA_CLAMP, Math.min(WHEEL_DELTA_CLAMP, event.deltaY));
        const normalizedDelta = Math.sign(raw) * Math.sqrt(Math.abs(raw));
        const speed = event.shiftKey ? FAST_WHEEL_ZOOM_SPEED : WHEEL_ZOOM_SPEED;
        const factor = Math.exp(-normalizedDelta * WHEEL_ZOOM_COEFF * speed);
        const rect = container.getBoundingClientRect();
        const renderedPosition = {
            x: event.clientX - rect.left,
            y: event.clientY - rect.top,
        };
        const nextZoom = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, cy.zoom() * factor));
        cy.zoom({ level: nextZoom, renderedPosition });
    };

    cy.boxSelectionEnabled(false);
    cy.on("tap", "node", onCyNodeTap);
    container.addEventListener("wheel", onContainerWheel, { capture: true, passive: false });
    container.addEventListener("mousedown", onContainerMouseDown, true);
    container.addEventListener("click", onContainerClick, true);
    container.addEventListener("contextmenu", onContainerContextMenu, true);
    container.addEventListener("dblclick", onContainerDblClick, true);
    container.addEventListener("mouseout", onContainerMouseOut, true);
    cy.on("dbltap", 'node[type = "group"], node[type = "group-expanded"]', onDblTapGroup);

    return () => {
        document.removeEventListener("mousemove", onDocumentMouseMove, true);
        document.removeEventListener("mouseup", endBackgroundPress, true);
        document.removeEventListener("mousemove", onChipDragMove, true);
        document.removeEventListener("mouseup", onChipDragEnd, true);
        document.removeEventListener("mousemove", onNodePanMove, true);
        document.removeEventListener("mouseup", onNodePanEnd, true);
        cancelAnimationFrame(restoreFrame);
        if (!cy.destroyed()) {
            cy.off("render", restoreChipScrolls);
            container.removeEventListener("scroll", onChipScroll, true);
            container.removeEventListener("wheel", onContainerWheel, true);
            container.removeEventListener("mousedown", onContainerMouseDown, true);
            container.removeEventListener("click", onContainerClick, true);
            container.removeEventListener("contextmenu", onContainerContextMenu, true);
            container.removeEventListener("dblclick", onContainerDblClick, true);
            container.removeEventListener("mouseout", onContainerMouseOut, true);
            cy.off("tap", "node", onCyNodeTap);
            cy.off("dbltap", 'node[type = "group"], node[type = "group-expanded"]', onDblTapGroup);
        }
    };
}

export { MIN_ZOOM, MAX_ZOOM };
