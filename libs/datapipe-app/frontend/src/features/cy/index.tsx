import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import Cytoscape from "cytoscape";
import CytoscapeComponent from "react-cytoscapejs";
import "cytoscape-context-menus/cytoscape-context-menus.css";

// @ts-ignore
import nodeHtmlLabel from "cytoscape-node-html-label";
import dagre from "cytoscape-dagre";
import contextMenus from "cytoscape-context-menus";

import "./style.css";
import { groupBoxSize, stepNodeSize, tableNodeSize } from "./graphNodeLayout";
import { groupIconSvg, tableIconSvg, transformIconSvg, slidersHorizontalIconSvg } from "./nodeIcons";
import { escapeHtml, getTransformPrimaryKeys, renderKeyChipList } from "./nodeKeyChips";
import { KeyListPopover, type KeyPopoverState } from "./KeyListPopover";
import { NodeInspectorPanel, type InspectorState } from "./NodeInspectorPanel";
import { useResizableWidth } from "../../hooks/useResizableWidth";
import { reprocessData } from "./process";
import { stylesheet } from "./stylesheet";
import { syncCyGraph } from "./syncCyGraph";
import { initHtmlLabelOpacitySync, setNodeVisualOpacity } from "./htmlLabelOpacity";
import { initInternalEdgeOverlay, refreshInternalEdgeOverlay } from "./internalEdgeOverlay";
import {
    applyFailedEdgeStyles,
    clearFocus,
    focusSelection,
} from "./graphFocus";
import { Alert, AlertProps, Spin } from "antd";
import { GraphData } from "../../types";
import type { PipelineGraphProps } from "../../types/pipelineGraph";

Cytoscape.use(nodeHtmlLabel);
Cytoscape.use(dagre);
Cytoscape.use(contextMenus);

function buildGraphUrl(stageFilter?: string | null): string {
    const base = (process.env["REACT_APP_GET_GRAPH_URL"] as string) || "/api/v1alpha2/graph";
    if (!stageFilter) return base;
    const joiner = base.includes("?") ? "&" : "?";
    return `${base}${joiner}stage=${encodeURIComponent(stageFilter)}`;
}

const labelsInitStore = new WeakMap<Cytoscape.Core, true>();

/** Multi-select: clear only on short LMB click on canvas without pointer movement. */
const BG_CLEAR_MOVE_PX = 5;
const BG_CLEAR_MAX_MS = 280;
/** Window during which a DOM click is treated as already handled by the cy tap. */
const TAP_DEDUPE_MS = 350;

function labelOpacityStyle(data: Cytoscape.NodeDataDefinition): string {
    const opacity = typeof data.htmlLabelOpacity === "number" ? data.htmlLabelOpacity : 1;
    return `opacity:${opacity};`;
}

function interactionStateClass(data: Cytoscape.NodeDataDefinition): string {
    return [
        data.uiFocused ? "is-focused" : "",
        data.uiSelected ? "is-selected" : "",
        data.uiDimmed ? "is-dimmed" : "",
    ]
        .filter(Boolean)
        .join(" ");
}

function buildNodeLabelTpl() {
    return (data: Cytoscape.NodeDataDefinition) => {
        if (data.type === "group-expanded") {
            return "";
        }

        const fullName = data.name as string;
        const nodeId = (data.id as string) || fullName;
        const metaGroup = data.metaGroup as string | undefined;
        const isSubgraph = Boolean(metaGroup);
        const stateClass = interactionStateClass(data);
        const renderName = (lines: string[]) => lines.join("<br>");

        if (data.type === "group") {
            const childCount = data.child_count ?? 0;
            const tpk = getTransformPrimaryKeys(data);
            const fallback = groupBoxSize(fullName, childCount, tpk);
            const w = (data.boxW as number) ?? fallback.w;
            const h = (data.boxH as number) ?? fallback.h;
            return `
              <div class="node-compound-label node-compound-group ${stateClass}" data-cy-node-id="${nodeId}" style="width:${w}px;height:${h}px;${labelOpacityStyle(data)}" title="${escapeHtml(fullName)}">
                  <div class="node-content">
                      <div class="node-icon">${groupIconSvg}</div>
                      <div class="node-body">
                          <div class="node-title">${renderName(fallback.lines)}</div>
                          <div class="node-subtitle">${childCount} steps</div>
                          ${renderKeyChipList(nodeId, "tpk", tpk)}
                      </div>
                  </div>
              </div>
            `;
        }

        const coreClass = isSubgraph ? "node-core-subgraph" : "";

        if (data.type === "table") {
            const primaryKeys = (data.indexes as string[]) || [];
            const tableType = data.store_class ? String(data.store_class) : "TableStoreDB";
            const { w, h, lines } = tableNodeSize(fullName, primaryKeys, isSubgraph);
            return `
              <div
                class="node-core node-core-table ${coreClass} ${stateClass}"
                style="width:${w}px;height:${h}px;${labelOpacityStyle(data)}"
                data-cy-node-id="${nodeId}"
                title="${escapeHtml(fullName)}"
              >
                <div class="node-content">
                  <div class="node-icon">${tableIconSvg}</div>
                  <div class="node-body">
                    <div class="node-title">${renderName(lines)}</div>
                    <div class="node-subtitle">${escapeHtml(tableType)}</div>
                    ${renderKeyChipList(nodeId, "pk", primaryKeys)}
                  </div>
                </div>
              </div>
            `;
        }

        const tpk = getTransformPrimaryKeys(data);
        const { w, h, lines } = stepNodeSize(fullName, isSubgraph, tpk);
        const transformType = data.transform_type ? String(data.transform_type) : "TransformStep";

        return `
          <div
            class="node-core node-core-step ${coreClass} ${stateClass}"
            style="width:${w}px;height:${h}px;${labelOpacityStyle(data)}"
            data-cy-node-id="${nodeId}"
            title="${escapeHtml(fullName)}"
          >
            <div class="node-content">
              <div class="node-icon">${transformIconSvg}</div>
              <div class="node-body">
                <div class="node-title">${renderName(lines)}</div>
                <div class="node-subtitle">${escapeHtml(transformType)}</div>
                ${renderKeyChipList(nodeId, "tpk", tpk)}
              </div>
            </div>
          </div>
        `;
    };
}

function initNodeLabels(cy: Cytoscape.Core) {
    if (labelsInitStore.has(cy)) return;
    labelsInitStore.set(cy, true);
    initHtmlLabelOpacitySync(cy);
    initInternalEdgeOverlay(cy);
    // @ts-ignore
    cy.nodeHtmlLabel([
        {
            query: "node",
            halign: "center",
            valign: "center",
            halignBox: "center",
            valignBox: "center",
            tpl: buildNodeLabelTpl(),
        },
    ]);
    cy.one("render", () => {
        cy.nodes().forEach((node) => setNodeVisualOpacity(cy, node, 1));
    });
}

function refreshNodeLabelPositions(cy: Cytoscape.Core) {
    if (cy.destroyed()) return;
    cy.nodes().forEach((node) => {
        node.trigger("position");
    });
}

function PipelineGraphView({
    stageFilter = null,
    runSteps = null,
    height = "100%",
    rankDir = "TB",
    refreshIntervalMs = 0,
    pipelineId: pipelineIdProp,
}: PipelineGraphProps) {
    const navigate = useNavigate();
    const [loading, setLoading] = useState(false);
    const [showInitialSpin, setShowInitialSpin] = useState(true);
    const initialLoadRef = useRef(true);
    const [cy, setCy] = useState<Cytoscape.Core>();
    const [alertMsg, setAlertMsg] = useState<AlertProps | null>(null);
    const [expandedGroups, setExpandedGroups] = useState<Set<string>>(() => new Set());
    const [rawGraph, setRawGraph] = useState<GraphData | null>(null);
    const [pipelineId, setPipelineId] = useState<string | null>(pipelineIdProp ?? null);

    const [keyPopover, setKeyPopover] = useState<KeyPopoverState | null>(null);
    const [inspector, setInspector] = useState<InspectorState>(null);

    const {
        width: panelWidth,
        dragging: panelDragging,
        onHandleMouseDown: onPanelResize,
    } = useResizableWidth({
        initial: 360,
        min: 260,
        max: 620,
        storageKey: "dp.graphInspectorWidth",
        edge: "left",
    });

    const needFitRef = useRef(true);
    // graphUrl whose data currently lives in `rawGraph`; guards the sync effect
    // from running against a stale graph during a stage switch (which would fit
    // the camera to the old graph and consume needFit before the new one loads).
    const loadedUrlRef = useRef<string | null>(null);
    const stageInitKeyRef = useRef<string | null>(null);
    const anchorGroupRef = useRef<string | null>(null);
    const expandingRef = useRef(false);
    const pipelineIdRef = useRef(pipelineId);
    pipelineIdRef.current = pipelineId;

    const runStatusByStep = useMemo(() => {
        const map = new Map<string, string>();
        runSteps?.forEach((s) => map.set(s.step_name, s.status));
        return map.size ? map : undefined;
    }, [runSteps]);

    const graphUrl = useMemo(() => buildGraphUrl(stageFilter), [stageFilter]);
    const runStatusRef = useRef(runStatusByStep);
    runStatusRef.current = runStatusByStep;

    const graphNodesById = useMemo(() => {
        if (!rawGraph) return new Map<string, Cytoscape.NodeDataDefinition>();
        const { nodes } = reprocessData(rawGraph, expandedGroups);
        return nodes;
    }, [rawGraph, expandedGroups]);

    const setKeyPopoverRef = useRef(setKeyPopover);
    const setInspectorRef = useRef(setInspector);
    setKeyPopoverRef.current = setKeyPopover;
    setInspectorRef.current = setInspector;

    useEffect(() => {
        if (pipelineIdProp) {
            setPipelineId(pipelineIdProp);
            return;
        }
        fetch("/api/v1alpha3/capabilities")
            .then((r) => r.json())
            .then((c) => setPipelineId(c.pipeline_id ?? null))
            .catch(() => setPipelineId(null));
    }, [pipelineIdProp]);

    const toggleGroupExpand = useCallback((groupName: string) => {
        if (!cy) return;
        anchorGroupRef.current = groupName;
        setExpandedGroups((prev) => {
            const next = new Set(prev);
            if (next.has(groupName)) {
                next.delete(groupName);
                expandingRef.current = false;
            } else {
                next.add(groupName);
                expandingRef.current = true;
            }
            return next;
        });
    }, [cy]);

    const openNodeDetails = useCallback((node: Cytoscape.NodeSingular) => {
        const pid = pipelineIdRef.current;
        if (!pid) {
            setAlertMsg({ type: "warning", message: "Pipeline ID not available" });
            return;
        }
        const nodeType = node.data("type") as string;
        const name = node.data("name") as string;
        const base = `/pipelines/${encodeURIComponent(pid)}`;
        if (nodeType === "group") {
            navigate(`${base}/meta-steps/${encodeURIComponent(name)}`);
        } else if (nodeType === "table") {
            navigate(`${base}/tables/${encodeURIComponent(name)}`);
        } else if (nodeType === "transform") {
            navigate(`${base}/transforms/${encodeURIComponent(name)}`);
        }
    }, [navigate]);

    const openNodeDetailsRef = useRef(openNodeDetails);
    const toggleGroupExpandRef = useRef(toggleGroupExpand);
    openNodeDetailsRef.current = openNodeDetails;
    toggleGroupExpandRef.current = toggleGroupExpand;

    useEffect(() => {
        stageInitKeyRef.current = null;
        needFitRef.current = true;
        initialLoadRef.current = true;
        anchorGroupRef.current = null;
        setShowInitialSpin(true);
    }, [graphUrl]);

    useEffect(() => {
        if (!rawGraph) return;

        const initKey = `${graphUrl}::${stageFilter ?? ""}`;
        if (stageInitKeyRef.current === initKey) return;
        stageInitKeyRef.current = initKey;

        setExpandedGroups(new Set());
        needFitRef.current = true;
    }, [rawGraph, graphUrl, stageFilter]);

    useEffect(() => {
        let cancelled = false;

        async function loadGraph() {
            if (initialLoadRef.current) {
                setLoading(true);
            }

            try {
                const response = await fetch(graphUrl);
                if (!response.ok) throw new Error(`Graph request failed: ${response.status}`);
                const data = await response.json();
                if (cancelled) return;
                loadedUrlRef.current = graphUrl;
                setRawGraph(data);
            } catch {
                if (!cancelled && cy) {
                    cy.elements().remove();
                }
            } finally {
                if (!cancelled) {
                    setLoading(false);
                    initialLoadRef.current = false;
                    setShowInitialSpin(false);
                }
            }
        }

        loadGraph();
        if (!refreshIntervalMs) return () => {
            cancelled = true;
        };

        const timer = setInterval(loadGraph, refreshIntervalMs);
        return () => {
            cancelled = true;
            clearInterval(timer);
        };
    }, [graphUrl, refreshIntervalMs, cy]);

    useEffect(() => {
        if (!cy || !rawGraph || loading) return;
        // Skip while rawGraph still belongs to the previous stage: wait until the
        // fetch for the current graphUrl lands so we animate to the right frame.
        if (loadedUrlRef.current !== graphUrl) return;

        syncCyGraph(cy, rawGraph, expandedGroups, {
            mode: needFitRef.current ? "fit" : "preserve",
            rankDir,
            anchorGroup: anchorGroupRef.current,
            expanding: expandingRef.current,
            onLayoutComplete: () => {
                refreshNodeLabelPositions(cy);
                applyFailedEdgeStyles(cy, runStatusRef.current);
                refreshInternalEdgeOverlay(cy);
            },
        });
        needFitRef.current = false;
        anchorGroupRef.current = null;
    }, [cy, rawGraph, expandedGroups, loading, rankDir, graphUrl]);

    useEffect(() => {
        if (!cy || cy.destroyed()) return;
        initNodeLabels(cy);
    }, [cy]);

    useEffect(() => {
        if (!cy || cy.destroyed() || !labelsInitStore.has(cy)) return;
        cy.nodes().forEach((node) => {
            node.data("labelRefresh", ((node.data("labelRefresh") as number) ?? 0) + 1);
        });
    }, [cy, runStatusByStep]);

    useEffect(() => {
        if (!cy || cy.destroyed()) return;
        applyFailedEdgeStyles(cy, runStatusByStep);
    }, [cy, runStatusByStep]);

    useEffect(() => {
        if (!cy || cy.destroyed()) return;
        const container = cy.container();
        if (!container) return;

        const canSelectNode = (node: Cytoscape.NodeSingular): boolean => {
            const type = node.data("type") as string;
            return type !== "group-expanded";
        };

        const openInspectorForNode = (node: Cytoscape.NodeSingular) => {
            setInspectorRef.current({
                nodeId: node.id(),
                data: node.data(),
            });
            setKeyPopoverRef.current(null);
        };

        const handleNodeSelect = (node: Cytoscape.NodeSingular, multi: boolean) => {
            if (!canSelectNode(node)) return;
            if (multi) {
                if (node.selected()) {
                    node.unselect();
                    setInspectorRef.current(null);
                } else {
                    node.select();
                    openInspectorForNode(node);
                }
            } else {
                cy.$(":selected").unselect();
                node.select();
                openInspectorForNode(node);
            }
            focusSelection(cy);
        };

        const resolveOverflowChip = (target: EventTarget | null): HTMLElement | null => {
            return (target as Element | null)?.closest?.("[data-key-overflow='true']") as HTMLElement | null;
        };

        const openKeyPopoverFromChip = (overflow: HTMLElement) => {
            const nodeId = overflow.getAttribute("data-cy-node-id");
            const kind = overflow.getAttribute("data-key-kind") as "pk" | "tpk" | null;
            if (!nodeId || !kind) return;

            const node = cy.getElementById(nodeId);
            if (node.empty()) return;

            const data = node.data();
            const keys =
                kind === "pk"
                    ? ((data.indexes as string[]) ?? [])
                    : getTransformPrimaryKeys(data);

            const containerRect = container.getBoundingClientRect();
            const chipRect = overflow.getBoundingClientRect();

            setKeyPopoverRef.current({
                nodeId,
                kind,
                keys,
                anchor: {
                    x: chipRect.left - containerRect.left,
                    y: chipRect.bottom - containerRect.top + 8,
                },
            });
        };

        const resolveNodeFromLabel = (target: EventTarget | null): Cytoscape.NodeSingular | null => {
            const label = (target as Element | null)?.closest?.("[data-cy-node-id]");
            if (!label) return null;
            const nodeId = label.getAttribute("data-cy-node-id");
            if (!nodeId) return null;
            const node = cy.getElementById(nodeId);
            if (node.empty()) return null;
            return node as Cytoscape.NodeSingular;
        };

        type BgPress = { x: number; y: number; moved: boolean; downAt: number };
        let bgPress: BgPress | null = null;
        /** Snapshot while multi-select background gesture is active (pan / long-press). */
        let multiSelectSnapshot: string[] | null = null;
        /**
         * Node captured at mousedown. The html-label plugin can rebuild a node's
         * DOM label between mousedown and mouseup (e.g. on hover/selection focus),
         * which makes the browser dispatch `click` on the container instead of the
         * label. We fall back to this captured node so fast clicks still select.
         */
        let nodePress: { nodeId: string; x: number; y: number } | null = null;
        /** True while the current press started on an overflow "+N more" chip. */
        let pressStartedOnOverflow = false;
        /** Timestamp of the last node selection handled via the cytoscape tap. */
        let nodeTapHandledAt = 0;

        const onDocumentMouseMove = (event: MouseEvent) => {
            if (!bgPress) return;
            const dx = event.clientX - bgPress.x;
            const dy = event.clientY - bgPress.y;
            if (dx * dx + dy * dy > BG_CLEAR_MOVE_PX * BG_CLEAR_MOVE_PX) {
                bgPress.moved = true;
            }
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
                cy.nodes(":selected").length > 0;

            if (shouldClear) {
                cy.$(":selected").unselect();
                clearFocus(cy);
                setInspectorRef.current(null);
                setKeyPopoverRef.current(null);
                return;
            }

            // Cytoscape may unselect on pan/tap despite autounselectify — restore multi-select.
            if (snapshot && snapshot.length > 1) {
                const restore = () => {
                    cy.batch(() => {
                        snapshot.forEach((id) => {
                            const node = cy.getElementById(id);
                            if (!node.empty()) node.select();
                        });
                    });
                    focusSelection(cy);
                };
                restore();
                requestAnimationFrame(restore);
            }
        };

        const onContainerMouseDown = (event: MouseEvent) => {
            if (event.button !== 0) return;
            pressStartedOnOverflow = false;
            nodePress = null;
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
                nodePress = { nodeId: nodeAtDown.id(), x: event.clientX, y: event.clientY };
                return;
            }
            if (!container.contains(event.target as Node)) return;

            const selected = cy.nodes(":selected");
            multiSelectSnapshot =
                selected.length > 1 ? selected.map((node) => node.id()) : null;

            bgPress = {
                x: event.clientX,
                y: event.clientY,
                moved: false,
                downAt: performance.now(),
            };

            document.addEventListener("mousemove", onDocumentMouseMove, true);
            document.addEventListener("mouseup", endBackgroundPress, true);
        };

        // Primary node-selection path: the cytoscape tap reliably fires (pointer
        // events bubble to the container) even when the html-label DOM is rebuilt
        // mid-click, which can otherwise suppress the browser "click" event.
        const onCyNodeTap = (event: Cytoscape.EventObject) => {
            const node = event.target as Cytoscape.NodeSingular;
            if (!canSelectNode(node)) return;
            const original = event.originalEvent as MouseEvent | undefined;
            if (original?.button != null && original.button !== 0) return;
            if (pressStartedOnOverflow || resolveOverflowChip(original?.target ?? null)) return;
            nodeTapHandledAt = performance.now();
            handleNodeSelect(node, Boolean(original?.ctrlKey || original?.metaKey));
        };

        const onContainerClick = (event: MouseEvent) => {
            if (event.button !== 0) return;
            const overflow = resolveOverflowChip(event.target);
            if (overflow) {
                event.preventDefault();
                event.stopPropagation();
                nodePress = null;
                return;
            }
            // Already handled by the cytoscape tap for this gesture — avoid a
            // second (double-toggling) selection call.
            if (performance.now() - nodeTapHandledAt < TAP_DEDUPE_MS) {
                nodePress = null;
                return;
            }
            let node = resolveNodeFromLabel(event.target);
            if (!node && nodePress) {
                const dx = event.clientX - nodePress.x;
                const dy = event.clientY - nodePress.y;
                if (dx * dx + dy * dy <= BG_CLEAR_MOVE_PX * BG_CLEAR_MOVE_PX) {
                    const candidate = cy.getElementById(nodePress.nodeId);
                    if (!candidate.empty()) node = candidate as Cytoscape.NodeSingular;
                }
            }
            nodePress = null;
            if (!node) return;
            event.preventDefault();
            event.stopPropagation();
            handleNodeSelect(node, event.ctrlKey || event.metaKey);
        };

        const onContainerMouseOver = (event: MouseEvent) => {
            const node = resolveNodeFromLabel(event.target);
            if (!node) return;
            if (cy.nodes(":selected").length > 0) {
                focusSelection(cy);
            }
        };

        const onContainerMouseOut = (event: MouseEvent) => {
            const related = event.relatedTarget as Node | null;
            if (related && container.contains(related)) {
                const stillOnNode = resolveNodeFromLabel(related);
                if (stillOnNode) return;
            }
            if (cy.nodes(":selected").length > 0) {
                focusSelection(cy);
                return;
            }
            clearFocus(cy);
        };

        const onDblTapGroup = (event: Cytoscape.EventObject) => {
            const node = event.target as Cytoscape.NodeSingular;
            const type = node.data("type") as string;
            if (type === "group" || type === "group-expanded") {
                toggleGroupExpandRef.current(node.data("name") as string);
            }
        };

        cy.boxSelectionEnabled(false);
        cy.on("tap", "node", onCyNodeTap);

        container.addEventListener("mousedown", onContainerMouseDown, true);
        container.addEventListener("click", onContainerClick, true);
        container.addEventListener("mouseover", onContainerMouseOver, true);
        container.addEventListener("mouseout", onContainerMouseOut, true);
        cy.on("dbltap", 'node[type = "group"], node[type = "group-expanded"]', onDblTapGroup);

        return () => {
            try {
                document.removeEventListener("mousemove", onDocumentMouseMove, true);
                document.removeEventListener("mouseup", endBackgroundPress, true);
                if (!cy.destroyed()) {
                    container.removeEventListener("mousedown", onContainerMouseDown, true);
                    container.removeEventListener("click", onContainerClick, true);
                    container.removeEventListener("mouseover", onContainerMouseOver, true);
                    container.removeEventListener("mouseout", onContainerMouseOut, true);
                    cy.off("tap", "node", onCyNodeTap);
                    cy.off("dbltap", 'node[type = "group"], node[type = "group-expanded"]', onDblTapGroup);
                }
            } catch {
                /* cytoscape may already be torn down */
            }
        };
    }, [cy]);

    useEffect(() => {
        if (!cy || cy.destroyed()) return;

        // @ts-ignore cytoscape-context-menus
        cy.contextMenus({
            menuItems: [
                {
                    id: "open-details",
                    content: "Open details page…",
                    selector: "node",
                    onClickFunction: (event: { target?: Cytoscape.NodeSingular; cyTarget?: Cytoscape.NodeSingular }) => {
                        const node = event.target || event.cyTarget;
                        if (node) openNodeDetailsRef.current(node);
                    },
                },
                {
                    id: "expand-steps",
                    content: "Expand into sub-steps",
                    selector: 'node[type = "group"]',
                    onClickFunction: (event: { target?: Cytoscape.NodeSingular; cyTarget?: Cytoscape.NodeSingular }) => {
                        const node = event.target || event.cyTarget;
                        if (node?.data("type") === "group") {
                            toggleGroupExpandRef.current(node.data("name") as string);
                        }
                    },
                },
                {
                    id: "collapse-steps",
                    content: "Collapse into single step",
                    selector: 'node[type = "group-expanded"], node[?metaGroup]',
                    onClickFunction: (event: { target?: Cytoscape.NodeSingular; cyTarget?: Cytoscape.NodeSingular }) => {
                        const node = event.target || event.cyTarget;
                        const groupName =
                            (node?.data("type") === "group-expanded"
                                ? (node?.data("name") as string)
                                : (node?.data("metaGroup") as string)) || undefined;
                        if (groupName) toggleGroupExpandRef.current(groupName);
                    },
                },
            ],
        });

        return () => {
            try {
                if (!cy.destroyed()) {
                    // @ts-ignore
                    cy.contextMenus("destroy");
                    labelsInitStore.delete(cy);
                }
            } catch {
                /* cytoscape may already be torn down */
            }
        };
    }, [cy]);

    useEffect(() => {
        const onKeyDown = (event: KeyboardEvent) => {
            if (event.key !== "Escape") return;
            if (keyPopover) {
                setKeyPopover(null);
                return;
            }
            if (inspector) {
                setInspector(null);
            }
        };
        document.addEventListener("keydown", onKeyDown);
        return () => document.removeEventListener("keydown", onKeyDown);
    }, [keyPopover, inspector]);

    useEffect(() => {
        if (!keyPopover) return undefined;
        const onPointerDown = (event: MouseEvent) => {
            const target = event.target as Element | null;
            if (target?.closest?.(".node-key-popover") || target?.closest?.("[data-key-overflow='true']")) {
                return;
            }
            setKeyPopover(null);
        };
        document.addEventListener("mousedown", onPointerDown, true);
        return () => document.removeEventListener("mousedown", onPointerDown, true);
    }, [keyPopover]);

    const closeAlert = () => setAlertMsg(null);

    const handleZoomIn = useCallback(() => {
        if (!cy || cy.destroyed()) return;
        cy.zoom(cy.zoom() * 1.2);
    }, [cy]);

    const handleZoomOut = useCallback(() => {
        if (!cy || cy.destroyed()) return;
        cy.zoom(cy.zoom() / 1.2);
    }, [cy]);

    const handleFit = useCallback(() => {
        if (!cy || cy.destroyed()) return;
        cy.fit(undefined, 48);
    }, [cy]);

    const navigateToInspectorNode = useCallback(
        (nodeId: string) => {
            const fallbackData = graphNodesById.get(nodeId);
            if (!cy || cy.destroyed()) {
                if (fallbackData) {
                    setInspector({ nodeId, data: fallbackData });
                    setKeyPopover(null);
                }
                return;
            }
            const node = cy.getElementById(nodeId);
            if (!node.empty()) {
                const type = node.data("type") as string;
                if (type !== "group-expanded") {
                    cy.$(":selected").unselect();
                    node.select();
                    setInspector({ nodeId: node.id(), data: node.data() });
                    setKeyPopover(null);
                    focusSelection(cy);
                    return;
                }
            }
            if (fallbackData) {
                setInspector({ nodeId, data: fallbackData });
                setKeyPopover(null);
                if (!node.empty()) {
                    cy.$(":selected").unselect();
                    focusSelection(cy);
                }
            }
        },
        [cy, graphNodesById],
    );

    return (
        <div className="pipeline-graph-shell" style={{ height }}>
            <div className="pipeline-graph-embedded" style={{ position: "relative" }}>
            {alertMsg && (
                <Alert
                    message={alertMsg.message}
                    type={alertMsg.type}
                    closable
                    afterClose={closeAlert}
                />
            )}
            {showInitialSpin && loading && <Spin className="spin" spinning={true} />}
            <div className="graph-toolbar graph-toolbar-floating">
                <button type="button" className="graph-toolbar-button" onClick={handleZoomOut} title="Zoom out">
                    −
                </button>
                <button type="button" className="graph-toolbar-button" onClick={handleZoomIn} title="Zoom in">
                    +
                </button>
                <button type="button" className="graph-toolbar-segment" onClick={handleFit} title="Fit graph">
                    Fit
                </button>
                <button
                    type="button"
                    className="graph-toolbar-button graph-toolbar-icon"
                    title="Graph settings"
                    aria-label="Graph settings"
                    dangerouslySetInnerHTML={{ __html: slidersHorizontalIconSvg }}
                />
            </div>
            <CytoscapeComponent
                stylesheet={stylesheet}
                cy={setCy}
                autoungrabify
                maxZoom={3}
                minZoom={0.08}
                wheelSensitivity={0.2}
                elements={[]}
                className="cy-container cy-container-embedded"
            />
            {keyPopover && (
                <KeyListPopover state={keyPopover} onClose={() => setKeyPopover(null)} />
            )}
            </div>
            <NodeInspectorPanel
                inspector={inspector}
                graphNodesById={graphNodesById}
                runStatusByStep={runStatusByStep}
                width={panelWidth}
                dragging={panelDragging}
                onHandleMouseDown={onPanelResize}
                onClose={() => setInspector(null)}
                onNavigateToNode={navigateToInspectorNode}
            />
        </div>
    );
}

export default PipelineGraphView;
