import React from "react";
import {
    BarChartOutlined,
    DatabaseOutlined,
    ExperimentOutlined,
    PictureOutlined,
    TableOutlined,
} from "@ant-design/icons";
import { Link } from "react-router-dom";
import type { Capabilities } from "@datapipe/ui/types/ops";
import type { OpsSpecSummary } from "@datapipe/ui/plugins/types";

function matchNav(pathname: string, href: string): boolean {
    const hrefPath = href.split("?")[0] ?? href;
    return pathname === hrefPath || pathname.startsWith(`${hrefPath}/`);
}

function renderSpecGroup(
    href: string,
    label: string,
    icon: React.ReactNode,
    specs: OpsSpecSummary[],
    enabled: (spec: OpsSpecSummary) => boolean,
    collapsed: boolean,
    pathname: string,
) {
    const filtered = specs.filter(enabled);
    if (!filtered.length) return null;
    return (
        <div className="ops-sidebar-section" key={href}>
            <Link
                to={href}
                className={`datapipe-sidebar-item${matchNav(pathname, href) ? " active" : ""}`}
            >
                <span className="sidebar-icon">{icon}</span>
                {!collapsed && label}
            </Link>
            {!collapsed && filtered.map((spec) => (
                <Link
                    key={`${href}/${spec.id}`}
                    to={`${href}/${spec.id}`}
                    className={`datapipe-sidebar-item ops-sidebar-nested${pathname === `${href}/${spec.id}` ? " active" : ""}`}
                >
                    {spec.title}
                </Link>
            ))}
        </div>
    );
}

export function renderMlNavSections(ctx: {
    specs: OpsSpecSummary[];
    collapsed: boolean;
    pathname: string;
    capabilities: Capabilities | null;
}) {
    if (!ctx.capabilities?.ml_metrics && !ctx.specs.length) return null;
    return (
        <>
            {renderSpecGroup("/image", "Image", <PictureOutlined />, ctx.specs, (s) => !!s.has_image, ctx.collapsed, ctx.pathname)}
            {renderSpecGroup("/frozen-datasets", "Frozen Datasets", <DatabaseOutlined />, ctx.specs, (s) => !!s.has_frozen_datasets, ctx.collapsed, ctx.pathname)}
            {renderSpecGroup("/training", "Training", <ExperimentOutlined />, ctx.specs, (s) => !!s.has_training, ctx.collapsed, ctx.pathname)}
            {renderSpecGroup("/metrics", "Metrics", <BarChartOutlined />, ctx.specs, (s) => (s.metric_tables_count ?? 0) > 0, ctx.collapsed, ctx.pathname)}
            {renderSpecGroup("/class-metrics", "Class Metrics", <TableOutlined />, ctx.specs, (s) => (s.class_metric_tables_count ?? 0) > 0, ctx.collapsed, ctx.pathname)}
        </>
    );
}
