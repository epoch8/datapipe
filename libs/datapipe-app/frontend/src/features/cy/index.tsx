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
import {
    NODE_STEP_HEIGHT,
    SUBGRAPH_STEP_HEIGHT,
    SUBGRAPH_STEP_WIDTH,
    displayNodeName,
    groupNodeHeight,
    groupNodeWidth,
    nodeWidthFromLabel,
    subgraphNameMaxLen,
    tableDisplayName,
    tableNodeHeight,
    tableNodeWidth,
} from "./graphNodeLayout";
import { captureViewport, type LayoutMode } from "./compound";
import { stylesheet } from "./stylesheet";
import { syncCyGraph } from "./syncCyGraph";
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
    if (status === "failed" || status === "error") return "step-status-failed";
    if (status === "running" || status === "pending") return "step-status-pending";
    if (status === "completed" || status === "finish") return "step-status-completed";
    return "step-status-unknown";
}

function refreshNodeLabels(
    cy: Cytoscape.Core,
    expandedGroupsRef: React.MutableRefObject<Set<string>>,
    runStatusRef: React.MutableRefObject<Map<string, string> | undefined>,
) {
    // @ts-ignore
    cy.nodeHtmlLabel([
        {
            query: "node",
            halign: "center",
            valign: "center",
            halignBox: "center",
            valignBox: "center",
            tpl(data: Cytoscape.NodeDataDefinition) {
                if (data.type === "group") {
                    const childCount = data.child_count ?? 0;
                    const width = groupNodeWidth(childCount);
                    const height = groupNodeHeight(childCount);
                    return `
              <div class="node-compound-label node-compound-group" style="width:${width}px;height:${height}px" title="${data.name}">
                  <div class="compound-title">${displayNodeName(data.name as string, 28)}</div>
                  <div class="compound-hint">${childCount} steps</div>
                  <div class="compound-action">right-click for options</div>
              </div>
            `;
                }

                const metaGroup = data.metaGroup as string | undefined;
                const isSubgraph = Boolean(metaGroup);
                const expanded = metaGroup
                    ? expandedGroupsRef.current.has(metaGroup)
                    : false;
                const metaBadge = metaGroup
                    ? `<div class="meta-group-badge ${expanded ? "meta-group-badge-expanded" : ""}">
                  <span class="meta-group-badge-name">${displayNodeName(metaGroup, 14)}</span>
                  ${expanded ? `<span class="meta-group-collapse-hint">zoom in · right-click</span>` : ""}
               </div>`
                    : "";

                const status = resolveNodeStatus(
                    data.name as string,
                    data,
                    runStatusRef.current,
                );
                const fullName = data.name as string;
                if (data.type === "table") {
                    const indexes = (data.indexes as string[]) || [];
                    const width = tableNodeWidth(fullName, indexes, isSubgraph);
                    const height = tableNodeHeight(fullName, isSubgraph);
                    const coreClass = isSubgraph ? "node-core-subgraph" : "";
                    const nameClass = isSubgraph ? "name" : "name name-table-full";
                    return `
              <div class="node-core node-core-table ${coreClass}" style="width: ${width}px; height: ${height}px" title="${fullName}">
                  ${metaBadge}
                  <div class="icon icon-table"></div>
                  <div class="${nameClass}">${tableDisplayName(fullName, isSubgraph)}</div>
                  ${
                      !isSubgraph && indexes.length
                          ? `<div class="indexes">${displayNodeName(indexes.join(", "), 40)}</div>`
                          : ""
                  }
                  ${!isSubgraph && data.size ? `<div class="indexes">size: ${data.size}</div>` : ""}
                  ${
                      !isSubgraph && data.store_class
                          ? `<div class="store">${data.store_class}</div>`
                          : ""
                  }
              </div>
            `;
                }

                const width = isSubgraph ? SUBGRAPH_STEP_WIDTH : nodeWidthFromLabel(fullName);
                const height = isSubgraph ? SUBGRAPH_STEP_HEIGHT : NODE_STEP_HEIGHT;
                const coreClass = isSubgraph ? "node-core-subgraph" : "";
                const nameMax = subgraphNameMaxLen(isSubgraph);
                return `
              <div class="node-core node-core-step ${coreClass}" style="width: ${width}px; height: ${height}px" title="${fullName}">
                  ${metaBadge}
                  <div class="name">${displayNodeName(fullName, nameMax)}</div>
                  <div class="step-status ${statusClass(status)}">${status}</div>
                  ${
                      !isSubgraph &&
                      (data.total_idx_count != null || data.changed_idx_count != null)
                          ? `<div class="indexes">${data.total_idx_count ?? 0}/${data.changed_idx_count ?? 0}</div>`
                          : ""
                  }
                  ${!isSubgraph ? `<div class="transform-type">${data.transform_type ?? ""}</div>` : ""}
              </div>
            `;
            },
        },
    ]);
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

    const layoutModeRef = useRef<LayoutMode>("fit");
    const viewportRef = useRef<ReturnType<typeof captureViewport> | null>(null);
    const anchorGroupRef = useRef<string | null>(null);
    const stageInitKeyRef = useRef<string | null>(null);
    const expandedGroupsRef = useRef(expandedGroups);
    expandedGroupsRef.current = expandedGroups;
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
        viewportRef.current = captureViewport(cy);
        layoutModeRef.current = "local";
        anchorGroupRef.current = groupName;
        setExpandedGroups((prev) => {
            const next = new Set(prev);
            if (next.has(groupName)) {
                next.delete(groupName);
            } else {
                next.add(groupName);
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
        layoutModeRef.current = "fit";
        anchorGroupRef.current = null;
        initialLoadRef.current = true;
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
        layoutModeRef.current = "fit";
        anchorGroupRef.current = null;
    }, [rawGraph, graphUrl, stageFilter]);

    useEffect(() => {
        let cancelled = false;

        async function loadGraph() {
            if (initialLoadRef.current) {
                setLoading(true);
            } else if (cy) {
                viewportRef.current = captureViewport(cy);
                layoutModeRef.current = "preserve";
                anchorGroupRef.current = null;
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
            mode: layoutModeRef.current,
            rankDir,
            viewport: layoutModeRef.current !== "fit" ? viewportRef.current : null,
            anchorGroup: anchorGroupRef.current,
        });
        refreshNodeLabels(cy, expandedGroupsRef, runStatusRef);
        layoutModeRef.current = "preserve";
        anchorGroupRef.current = null;
    }, [cy, rawGraph, expandedGroups, loading, rankDir]);

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
                    selector: "node[?metaGroup]",
                    onClickFunction: (event: { target?: Cytoscape.NodeSingular; cyTarget?: Cytoscape.NodeSingular }) => {
                        const node = event.target || event.cyTarget;
                        const groupName = node?.data("metaGroup") as string | undefined;
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
                }
            } catch {
                /* cytoscape may already be torn down */
            }
        };
    }, [cy]);

    const closeAlert = () => setAlertMsg(null);

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
