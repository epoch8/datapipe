import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import Cytoscape from "cytoscape";
import CytoscapeComponent from "react-cytoscapejs";

import dagre from "cytoscape-dagre";

import "./style.css";
import { groupBoxSize, stepNodeSize, tableNodeSize } from "./graphNodeLayout";
import { groupIconSvg, tableIconSvg, transformIconSvg, slidersHorizontalIconSvg } from "./nodeIcons";
import { escapeHtml, getTransformPrimaryKeys, renderKeyChipList, renderLabelChipList } from "./nodeKeyChips";
import { KeyListPopover, type KeyPopoverState } from "./KeyListPopover";
import { NodeInspectorPanel, type InspectorState } from "./NodeInspectorPanel";
import { useResizableWidth } from "../../hooks/useResizableWidth";
import { reprocessData } from "./process";
import { stylesheet } from "./stylesheet";
import { syncCyGraph } from "./syncCyGraph";
import { initHtmlLabelOpacitySync, setNodeVisualOpacity } from "./htmlLabelOpacity";
import { initInternalEdgeOverlay, refreshInternalEdgeOverlay } from "./internalEdgeOverlay";
import { initSnakeRowOverlay, refreshSnakeRowOverlay } from "./snakeRowOverlay";
import {
    applyFailedEdgeStyles,
    clearSelectedNodeIds,
    initHtmlLabelInteractionStateSync,
    setGraphLabelFocus,
    setSelectedNodeIds,
} from "./graphVisualState";
import { attachGraphInteractions, MAX_ZOOM, MIN_ZOOM } from "./graphInteractions";
import { fitGraphViewport } from "./incrementalLayout";
import { initHtmlNodeLabels } from "./htmlNodeLabels";
import { Alert, AlertProps, Spin } from "antd";
import { apiFetch, getApiErrorMessage } from "../../api/http";
import {
    captureGraphSessionState,
    loadGraphSessionState,
    saveGraphSessionState,
    type GraphSessionState,
} from "./graphSessionState";
import { GraphData } from "../../types";
import type { PipelineGraphProps } from "../../types/pipelineGraph";
import { resolveFlowLayout } from "../../types/pipelineGraph";

Cytoscape.use(dagre);

function buildGraphUrl(stageFilter?: string | null, labelKey?: string | null): string {
    const base = (process.env["REACT_APP_GET_GRAPH_URL"] as string) || "/api/v1alpha3/graph";
    const params = new URLSearchParams();
    if (stageFilter) params.set("stage", stageFilter);
    if (stageFilter && labelKey && labelKey !== "stage") params.set("label_key", labelKey);
    const query = params.toString();
    if (!query) return base;
    const joiner = base.includes("?") ? "&" : "?";
    return `${base}${joiner}${query}`;
}

const labelsInitStore = new WeakMap<Cytoscape.Core, true>();

function labelOpacityStyle(data: Cytoscape.NodeDataDefinition): string {
    const opacity = typeof data.htmlLabelOpacity === "number" ? data.htmlLabelOpacity : 1;
    return `opacity:${opacity};`;
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
        const renderName = (lines: string[]) => lines.join("<br>");

        if (data.type === "group") {
            const childCount = data.child_count ?? 0;
            const tpk = getTransformPrimaryKeys(data);
            const fallback = groupBoxSize(fullName, childCount, tpk);
            const w = (data.boxW as number) ?? fallback.w;
            const h = (data.boxH as number) ?? fallback.h;
            return `
              <div class="node-compound-label node-compound-group" data-cy-node-id="${nodeId}" style="width:${w}px;height:${h}px;${labelOpacityStyle(data)}" title="${escapeHtml(fullName)}">
                  <div class="node-content">
                      <div class="node-icon">${groupIconSvg}</div>
                      <div class="node-body">
                          <div class="node-title">${renderName(fallback.lines)}</div>
                          <div class="node-subtitle">${childCount} steps</div>
                          ${renderKeyChipList(nodeId, "tpk", tpk)}
                          ${renderLabelChipList(nodeId, (data.labels as string[][]) ?? [])}
                      </div>
                  </div>
              </div>
            `;
        }

        const coreClass = isSubgraph ? "node-core-subgraph" : "";

        if (data.type === "table") {
            const primaryKeys = (data.indexes as string[]) || [];
            const tableType = data.store_class ? String(data.store_class) : "TableStoreDB";
            const { w, h, lines } = tableNodeSize(fullName, primaryKeys, false);
            return `
              <div
                class="node-core node-core-table ${coreClass}"
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
        const { w, h, lines } = stepNodeSize(fullName, false, tpk);
        const transformType = data.transform_type ? String(data.transform_type) : "TransformStep";

        return `
          <div
            class="node-core node-core-step ${coreClass}"
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
                ${renderLabelChipList(nodeId, (data.labels as string[][]) ?? [])}
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
    initSnakeRowOverlay(cy);
    initHtmlNodeLabels(cy, [
        {
            query: "node",
            halign: "center",
            valign: "center",
            halignBox: "center",
            valignBox: "center",
            tpl: buildNodeLabelTpl(),
        },
    ]);
    initHtmlLabelInteractionStateSync(cy);
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
    labelKey = null,
    labelFilter = null,
    labelOrder = [],
    labelColumnMap = {},
    layoutMode = "dag",
    runSteps = null,
    height = "100%",
    rankDir = "TB",
    flowLayout: flowLayoutProp,
    refreshIntervalMs = 0,
    pipelineId: pipelineIdProp,
    graphRefreshToken = 0,
}: PipelineGraphProps) {
    const navigate = useNavigate();
    const [loading, setLoading] = useState(false);
    const [showInitialSpin, setShowInitialSpin] = useState(true);
    const initialLoadRef = useRef(true);
    const [cy, setCy] = useState<Cytoscape.Core>();
    const [alertMsg, setAlertMsg] = useState<AlertProps | null>(null);
    const [expandedGroups, setExpandedGroups] = useState<Set<string>>(() => {
        const saved = loadGraphSessionState(buildGraphUrl(stageFilter, labelKey));
        return saved ? new Set(saved.expandedGroups) : new Set();
    });
    const [rawGraph, setRawGraph] = useState<GraphData | null>(null);
    const [pipelineId, setPipelineId] = useState<string | null>(pipelineIdProp ?? null);

    const [keyPopover, setKeyPopover] = useState<KeyPopoverState | null>(null);
    const [inspector, setInspector] = useState<InspectorState>(null);

    // Allow the inspector panel to grow up to ~50% of the viewport. The panel
    // lives inside the zoomed #root (--dp-ui-scale), so its CSS-px space is the
    // viewport width divided by that scale.
    const inspectorMaxWidth = useMemo(() => {
        if (typeof window === "undefined") return 900;
        const scaleRaw = getComputedStyle(document.documentElement).getPropertyValue(
            "--dp-ui-scale",
        );
        const scale = parseFloat(scaleRaw) || 0.8;
        return Math.round((window.innerWidth / scale) * 0.5);
    }, []);

    const {
        width: panelWidth,
        dragging: panelDragging,
        onHandleMouseDown: onPanelResize,
    } = useResizableWidth({
        initial: 360,
        min: 260,
        max: inspectorMaxWidth,
        storageKey: "dp.graphInspectorWidth",
        edge: "left",
    });

    const savedSessionRef = useRef<GraphSessionState | null>(
        loadGraphSessionState(buildGraphUrl(stageFilter, labelKey)),
    );
    const sessionRestoredRef = useRef(false);
    // Persisted zoom/pan is only restored on the very first mount (page reload /
    // deep-link). In-app navigation between stages and the full graph must not
    // snap the camera to a stale manual viewport — it animates a clean fit
    // instead, so the transition stays smooth and consistent.
    const firstMountRef = useRef(true);

    const flowLayout = useMemo(
        () => resolveFlowLayout(flowLayoutProp, rankDir),
        [flowLayoutProp, rankDir],
    );

    const needFitRef = useRef(true);
    const flowLayoutRef = useRef(flowLayout);
    // Whether the user has manually panned/zoomed. Until they do, the camera is
    // considered "auto": a container resize (flex settle, inspector panel resize,
    // sidebar toggle, window resize) re-fits so the graph stays centered instead
    // of drifting to an edge because cytoscape cached a stale container width.
    const userInteractedRef = useRef(false);
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

    const graphUrl = useMemo(
        () => buildGraphUrl(stageFilter, labelKey),
        [stageFilter, labelKey],
    );
    const runStatusRef = useRef(runStatusByStep);
    runStatusRef.current = runStatusByStep;

    const graphNodesById = useMemo(() => {
        if (!rawGraph) return new Map<string, Cytoscape.NodeDataDefinition>();
        const { nodes } = reprocessData(rawGraph, expandedGroups);
        return nodes;
    }, [rawGraph, expandedGroups]);

    const applySessionRestore = useCallback(
        (cyInstance: Cytoscape.Core) => {
            const saved = savedSessionRef.current;
            if (!saved || sessionRestoredRef.current || cyInstance.destroyed()) return;
            sessionRestoredRef.current = true;

            const hasExpanded = saved.expandedGroups.length > 0;

            // With expanded blues the layout was rebuilt from scratch — never reuse a
            // stale pan/zoom from before navigation (blues end up off-canvas).
            if (saved.userInteracted && !hasExpanded) {
                cyInstance.zoom(saved.zoom);
                cyInstance.pan(saved.pan);
            } else {
                cyInstance.resize();
                fitGraphViewport(cyInstance);
            }

            const nodeIds =
                saved.selectedNodeIds.length > 0
                    ? saved.selectedNodeIds
                    : saved.inspectorNodeId
                      ? [saved.inspectorNodeId]
                      : [];

            if (nodeIds.length > 0) {
                setSelectedNodeIds(cyInstance, nodeIds);
            }

            if (saved.inspectorNodeId) {
                const node = cyInstance.getElementById(saved.inspectorNodeId);
                if (!node.empty()) {
                    setInspector({
                        nodeId: saved.inspectorNodeId,
                        data: node.data(),
                    });
                } else {
                    const fallback = graphNodesById.get(saved.inspectorNodeId);
                    if (fallback) {
                        setInspector({
                            nodeId: saved.inspectorNodeId,
                            data: fallback,
                        });
                    }
                }
            }
        },
        [graphNodesById],
    );

    const setKeyPopoverRef = useRef(setKeyPopover);
    const setInspectorRef = useRef(setInspector);
    setKeyPopoverRef.current = setKeyPopover;
    setInspectorRef.current = setInspector;

    useEffect(() => {
        if (pipelineIdProp) {
            setPipelineId(pipelineIdProp);
            return;
        }
        // Cloud API has no pipeline_id; detail routes are global (/tables, /transforms).
        setPipelineId(null);
    }, [pipelineIdProp]);

    const persistGraphSession = useCallback(() => {
        saveGraphSessionState(
            captureGraphSessionState(
                graphUrl,
                expandedGroups,
                inspector,
                cy,
                userInteractedRef.current,
            ),
            stageFilter,
        );
    }, [graphUrl, expandedGroups, inspector, cy, stageFilter]);

    const toggleGroupExpand = useCallback((groupName: string) => {
        if (!cy) return;
        // Expand/collapse must not autofit the camera (ResizeObserver / session).
        userInteractedRef.current = true;
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
        persistGraphSession();
        const nodeType = node.data("type") as string;
        const name = node.data("name") as string;
        if (nodeType === "group" || nodeType === "group-expanded") {
            navigate(`/meta-steps/${encodeURIComponent(name)}`);
        } else if (nodeType === "table") {
            navigate(`/tables/${encodeURIComponent(name)}`);
        } else if (nodeType === "transform") {
            navigate(`/transforms/${encodeURIComponent(name)}`);
        }
    }, [navigate, persistGraphSession]);

    const openNodeDetailsRef = useRef(openNodeDetails);
    const toggleGroupExpandRef = useRef(toggleGroupExpand);
    const contextTargetRef = useRef<Cytoscape.NodeSingular | null>(null);
    openNodeDetailsRef.current = openNodeDetails;
    toggleGroupExpandRef.current = toggleGroupExpand;

    useEffect(() => {
        // Only the first mount restores a persisted camera/selection (reload or
        // deep-link). Later graphUrl changes are in-app navigation: drop the saved
        // session so the graph animates a fresh fit instead of snapping to a stale
        // manual zoom/pan.
        const isFirstMount = firstMountRef.current;
        firstMountRef.current = false;
        const saved = isFirstMount ? loadGraphSessionState(graphUrl) : null;
        savedSessionRef.current = saved;
        sessionRestoredRef.current = false;
        stageInitKeyRef.current = null;
        // Expanded metas must be re-laid out from scratch on remount; restoring a
        // stale pan/zoom (often from a narrower inspector viewport) hides the graph
        // and can leave the blue frame visually detached after Fit.
        const hasExpanded = Boolean(saved?.expandedGroups?.length);
        needFitRef.current = saved ? hasExpanded || !saved.userInteracted : true;
        userInteractedRef.current = saved && !hasExpanded ? saved.userInteracted : false;
        if (saved) {
            setExpandedGroups(new Set(saved.expandedGroups));
        } else {
            setExpandedGroups(new Set());
        }
        initialLoadRef.current = true;
        anchorGroupRef.current = null;
        setShowInitialSpin(true);
    }, [graphUrl]);

    useEffect(() => {
        if (!rawGraph) return;

        const initKey = `${graphUrl}::${stageFilter ?? ""}::${labelKey ?? ""}`;
        if (stageInitKeyRef.current === initKey) return;
        stageInitKeyRef.current = initKey;

        if (savedSessionRef.current) {
            // Never clear expandedGroups while a session is being restored — a
            // sync that finishes first used to flip sessionRestored and then this
            // effect wiped the expand set, leaving a broken layout.
            const saved = savedSessionRef.current;
            if (!sessionRestoredRef.current) {
                const hasExpanded = saved.expandedGroups.length > 0;
                needFitRef.current = hasExpanded || !saved.userInteracted;
            }
            return;
        }

        setExpandedGroups(new Set());
        needFitRef.current = true;
    }, [rawGraph, graphUrl, stageFilter, labelKey]);

    useEffect(() => {
        let cancelled = false;

        async function loadGraph() {
            if (initialLoadRef.current) {
                setLoading(true);
            }

            try {
                const response = await apiFetch(graphUrl);
                if (!response.ok) throw new Error(`Graph request failed: ${response.status}`);
                const data = await response.json();
                if (cancelled) return;
                loadedUrlRef.current = graphUrl;
                setRawGraph(data);
            } catch (error) {
                if (!cancelled) {
                    if (cy) {
                        cy.elements().remove();
                    }
                    setAlertMsg({ type: "error", message: getApiErrorMessage(error) });
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
    }, [graphUrl, refreshIntervalMs, cy, graphRefreshToken]);

    useEffect(() => {
        if (!cy || !rawGraph || loading) return;
        // Skip while rawGraph still belongs to the previous stage: wait until the
        // fetch for the current graphUrl lands so we animate to the right frame.
        if (loadedUrlRef.current !== graphUrl) return;

        if (flowLayoutRef.current !== flowLayout) {
            flowLayoutRef.current = flowLayout;
            needFitRef.current = true;
        }

        syncCyGraph(cy, rawGraph, expandedGroups, {
            mode: needFitRef.current ? "fit" : "preserve",
            rankDir,
            flowLayout,
            layoutMode,
            labelKey: labelKey ?? "stage",
            labelOrder,
            labelColumnMap: new Map(Object.entries(labelColumnMap)),
            anchorGroup: anchorGroupRef.current,
            expanding: expandingRef.current,
            onLayoutComplete: () => {
                anchorGroupRef.current = null;
                expandingRef.current = false;
                refreshNodeLabelPositions(cy);
                applyFailedEdgeStyles(cy, runStatusRef.current);
                setGraphLabelFocus(cy, labelKey ?? "stage", labelFilter);
                refreshInternalEdgeOverlay(cy);
                refreshSnakeRowOverlay(cy);
                applySessionRestore(cy);
            },
        });
        needFitRef.current = false;
    }, [
        cy,
        rawGraph,
        expandedGroups,
        loading,
        rankDir,
        flowLayout,
        layoutMode,
        labelKey,
        labelOrder,
        labelColumnMap,
        labelFilter,
        graphUrl,
        applySessionRestore,
    ]);

    useEffect(() => {
        if (!cy || cy.destroyed()) return;
        setGraphLabelFocus(cy, labelKey ?? "stage", labelFilter);
    }, [cy, labelKey, labelFilter]);

    useEffect(() => {
        return () => {
            persistGraphSession();
        };
    }, [persistGraphSession]);

    useEffect(() => {
        if (!cy || cy.destroyed()) return;
        initNodeLabels(cy);
    }, [cy]);

    // Keep cytoscape's cached viewport size in sync with the container.
    // Only call cy.resize() on real dimension changes — never pan/fit on
    // selection (that must not move the camera).
    useEffect(() => {
        if (!cy || cy.destroyed()) return;
        const container = cy.container();
        if (!container || typeof ResizeObserver === "undefined") return;

        let frame = 0;
        let lastW = container.clientWidth;
        let lastH = container.clientHeight;
        const observer = new ResizeObserver(() => {
            cancelAnimationFrame(frame);
            frame = requestAnimationFrame(() => {
                if (cy.destroyed()) return;
                const w = container.clientWidth;
                const h = container.clientHeight;
                if (Math.abs(w - lastW) < 1 && Math.abs(h - lastH) < 1) return;
                lastW = w;
                lastH = h;
                cy.resize();
                refreshInternalEdgeOverlay(cy);
                // Autofit only while camera is still in auto mode (first load).
                // Overview fit (no readable clamp) — matches the Fit button so a
                // resize does not jump back to a clipped middle slice.
                if (!userInteractedRef.current && cy.nodes().nonempty()) {
                    fitGraphViewport(cy);
                }
            });
        });
        observer.observe(container);
        return () => {
            cancelAnimationFrame(frame);
            observer.disconnect();
        };
    }, [cy]);

    useEffect(() => {
        if (!cy || cy.destroyed()) return;

        return attachGraphInteractions(cy, {
            onOpenInspector: (node) => {
                setInspectorRef.current({
                    nodeId: node.id(),
                    data: node.data(),
                });
                setKeyPopoverRef.current(null);
            },
            onCloseInspector: () => setInspectorRef.current(null),
            onOpenKeyPopover: (state) => setKeyPopoverRef.current(state),
            onCloseKeyPopover: () => setKeyPopoverRef.current(null),
            onToggleGroup: (groupName) => toggleGroupExpandRef.current(groupName),
            onUserInteracted: () => {
                userInteractedRef.current = true;
            },
            onContextTarget: (node) => {
                contextTargetRef.current = node;
            },
            onContextMenuAction: (itemId) => {
                const node = contextTargetRef.current;
                if (!node || typeof node.data !== "function") return;
                if (itemId === "open-details") {
                    openNodeDetailsRef.current(node);
                    return;
                }
                const type = node.data("type") as string;
                if (itemId === "expand-steps" && type === "group") {
                    toggleGroupExpandRef.current(node.id());
                } else if (itemId === "collapse-steps" && type === "group-expanded") {
                    toggleGroupExpandRef.current(node.id());
                }
            },
        });
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
        userInteractedRef.current = true;
        const next = Math.min(MAX_ZOOM, cy.zoom() * 1.25);
        cy.zoom(next);
    }, [cy]);

    const handleZoomOut = useCallback(() => {
        if (!cy || cy.destroyed()) return;
        userInteractedRef.current = true;
        const next = Math.max(MIN_ZOOM, cy.zoom() / 1.25);
        cy.zoom(next);
    }, [cy]);

    const handleResetZoom = useCallback(() => {
        if (!cy || cy.destroyed()) return;
        userInteractedRef.current = true;
        cy.zoom(1);
        cy.center();
    }, [cy]);

    const handleFit = useCallback(() => {
        if (!cy || cy.destroyed()) return;
        userInteractedRef.current = false;
        cy.resize();
        fitGraphViewport(cy);
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
                setSelectedNodeIds(cy, [node.id()]);
                setInspector({ nodeId: node.id(), data: node.data() });
                setKeyPopover(null);
                return;
            }
            if (fallbackData) {
                setInspector({ nodeId, data: fallbackData });
                setKeyPopover(null);
            }
        },
        [cy, graphNodesById],
    );

    const fillParent = height === "100%" || height === "100vh";

    return (
        <div
            className="pipeline-graph-shell"
            style={
                fillParent
                    ? { flex: "1 1 auto", minHeight: 0, height: "auto" }
                    : { height }
            }
        >
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
                    className="graph-toolbar-segment"
                    onClick={handleResetZoom}
                    title="Reset zoom to 100%"
                >
                    100%
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
                maxZoom={MAX_ZOOM}
                minZoom={MIN_ZOOM}
                wheelSensitivity={0}
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
                onOpenDetails={
                    inspector
                        ? () => {
                              const node = cy?.getElementById(inspector.nodeId);
                              if (node && !node.empty()) openNodeDetails(node);
                          }
                        : undefined
                }
            />
        </div>
    );
}

export default PipelineGraphView;
