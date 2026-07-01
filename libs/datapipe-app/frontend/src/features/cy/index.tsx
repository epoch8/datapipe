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
import { displayNodeName, groupBoxSize, stepNodeSize, tableNodeSize } from "./graphNodeLayout";
import { groupIconSvg, tableIconSvg, transformIconSvg, slidersHorizontalIconSvg } from "./nodeIcons";
import { stylesheet } from "./stylesheet";
import { syncCyGraph } from "./syncCyGraph";
import { initHtmlLabelOpacitySync, setNodeVisualOpacity } from "./htmlLabelOpacity";
import {
    applyFailedEdgeStyles,
    clearFocus,
    focusNode,
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

function resolveNodeStatus(
    name: string,
    data: Cytoscape.NodeDataDefinition,
    runStatusByStep?: Map<string, string>,
): string {
    const fromRun = runStatusByStep?.get(name);
    if (fromRun) return fromRun;
    if ((data.changed_idx_count ?? 0) > 0) return "pending";
    return "completed";
}

function statusClass(status: string): string {
    if (status === "failed" || status === "error") return "failed";
    if (status === "running") return "running";
    if (status === "pending") return "pending";
    if (status === "completed" || status === "finish") return "completed";
    return "unknown";
}

const labelsInitStore = new WeakMap<Cytoscape.Core, true>();

function buildNodeLabelTpl(runStatusRef: React.MutableRefObject<Map<string, string> | undefined>) {
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
            const fallback = groupBoxSize(fullName, childCount);
            const w = (data.boxW as number) ?? fallback.w;
            const h = (data.boxH as number) ?? fallback.h;
            return `
              <div class="node-compound-label node-compound-group" data-cy-node-id="${nodeId}" style="width:${w}px;height:${h}px" title="${fullName}">
                  <div class="node-content">
                      <div class="node-icon">${groupIconSvg}</div>
                      <div class="node-body">
                          <div class="node-title">${renderName(fallback.lines)}</div>
                          <div class="node-group-steps">${childCount} steps</div>
                      </div>
                  </div>
              </div>
            `;
        }

        const status = resolveNodeStatus(fullName, data, runStatusRef.current);
        const coreClass = isSubgraph ? "node-core-subgraph" : "";

        if (data.type === "table") {
            const indexes = (data.indexes as string[]) || [];
            const { w, h, lines } = tableNodeSize(fullName, indexes, isSubgraph);
            const tip = [
                fullName,
                indexes.length ? `PK: ${indexes.join(", ")}` : "",
                data.size != null ? `size: ${data.size}` : "",
                data.store_class ? String(data.store_class) : "",
                metaGroup ? `in ${metaGroup}` : "",
            ]
                .filter(Boolean)
                .join("\n");
            return `
              <div class="node-core node-core-table ${coreClass}" data-cy-node-id="${nodeId}" style="width:${w}px;height:${h}px" title="${tip}">
                  <div class="node-content">
                      <div class="node-icon">${tableIconSvg}</div>
                      <div class="node-body">
                          <div class="node-title">${renderName(lines)}</div>
                          ${
                              !isSubgraph && indexes.length
                                  ? `<div class="node-meta">PK: ${displayNodeName(indexes.join(", "), 44)}</div>`
                                  : ""
                          }
                          ${
                              !isSubgraph && data.store_class
                                  ? `<div class="node-subtitle">${displayNodeName(String(data.store_class), 36)}</div>`
                                  : ""
                          }
                      </div>
                  </div>
              </div>
            `;
        }

        const { w, h, lines } = stepNodeSize(fullName, isSubgraph);
        const tip = [
            fullName,
            data.transform_type ? String(data.transform_type) : "",
            data.total_idx_count != null || data.changed_idx_count != null
                ? `changed ${data.changed_idx_count ?? 0} / ${data.total_idx_count ?? 0}`
                : "",
            metaGroup ? `in ${metaGroup}` : "",
        ]
            .filter(Boolean)
            .join("\n");
        const changed = data.changed_idx_count ?? 0;
        const total = data.total_idx_count ?? 0;
        const idxLine = total > 0 || changed > 0 ? `changed ${changed} / ${total}` : "";
        const transformType = data.transform_type ? String(data.transform_type) : "";

        return `
              <div class="node-core node-core-step ${coreClass}" data-cy-node-id="${nodeId}" style="width:${w}px;height:${h}px" title="${tip}">
                  <div class="node-content">
                      <div class="node-icon">${transformIconSvg}</div>
                      <div class="node-body">
                          <div class="node-title">${renderName(lines)}</div>
                          ${!isSubgraph && transformType ? `<div class="node-subtitle">${displayNodeName(transformType, 36)}</div>` : ""}
                          ${idxLine ? `<div class="node-meta">${idxLine}</div>` : ""}
                          <div class="step-status ${statusClass(status)}">${status}</div>
                      </div>
                  </div>
              </div>
            `;
    };
}

function initNodeLabels(
    cy: Cytoscape.Core,
    runStatusRef: React.MutableRefObject<Map<string, string> | undefined>,
) {
    if (labelsInitStore.has(cy)) return;
    labelsInitStore.set(cy, true);
    initHtmlLabelOpacitySync(cy);
    // @ts-ignore
    cy.nodeHtmlLabel([
        {
            query: "node",
            halign: "center",
            valign: "center",
            halignBox: "center",
            valignBox: "center",
            tpl: buildNodeLabelTpl(runStatusRef),
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

    const needFitRef = useRef(true);
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

        if (stageFilter) {
            const metaNames = rawGraph.pipeline
                .filter((pipe) => pipe.type === "meta")
                .map((pipe) => pipe.name);
            setExpandedGroups(new Set(metaNames));
        } else {
            setExpandedGroups(new Set());
        }
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

        syncCyGraph(cy, rawGraph, expandedGroups, {
            mode: needFitRef.current ? "fit" : "preserve",
            rankDir,
            anchorGroup: anchorGroupRef.current,
            expanding: expandingRef.current,
            onLayoutComplete: () => {
                refreshNodeLabelPositions(cy);
                applyFailedEdgeStyles(cy, runStatusRef.current);
            },
        });
        needFitRef.current = false;
        anchorGroupRef.current = null;
    }, [cy, rawGraph, expandedGroups, loading, rankDir, graphUrl]);

    useEffect(() => {
        if (!cy || cy.destroyed()) return;
        initNodeLabels(cy, runStatusRef);
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

        const handleNodeSelect = (node: Cytoscape.NodeSingular, multi: boolean) => {
            if (!node.selectable()) return;
            if (multi) {
                if (node.selected()) {
                    node.unselect();
                } else {
                    node.select();
                }
            } else {
                cy.$(":selected").unselect();
                node.select();
            }
            focusSelection(cy);
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

        const onContainerClick = (event: MouseEvent) => {
            if (event.button !== 0) return;
            const node = resolveNodeFromLabel(event.target);
            if (node) {
                event.preventDefault();
                event.stopPropagation();
                handleNodeSelect(node, event.ctrlKey || event.metaKey);
                return;
            }
            if (container.contains(event.target as Node)) {
                cy.$(":selected").unselect();
                clearFocus(cy);
            }
        };

        const onContainerMouseOver = (event: MouseEvent) => {
            const node = resolveNodeFromLabel(event.target);
            if (!node) return;
            if (cy.nodes(":selected").length > 0) {
                focusSelection(cy);
                return;
            }
            focusNode(cy, node);
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

        container.addEventListener("click", onContainerClick, true);
        container.addEventListener("mouseover", onContainerMouseOver, true);
        container.addEventListener("mouseout", onContainerMouseOut, true);
        cy.on("dbltap", 'node[type = "group"], node[type = "group-expanded"]', onDblTapGroup);

        return () => {
            try {
                if (!cy.destroyed()) {
                    container.removeEventListener("click", onContainerClick, true);
                    container.removeEventListener("mouseover", onContainerMouseOver, true);
                    container.removeEventListener("mouseout", onContainerMouseOut, true);
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
                    content: "Open details…",
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

    return (
        <div className="pipeline-graph-embedded" style={{ height, position: "relative" }}>
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
        </div>
    );
}

export default PipelineGraphView;
