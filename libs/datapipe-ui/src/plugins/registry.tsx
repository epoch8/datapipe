import React from "react";
import type { Capabilities, Enrichment } from "../types/ops";
import type { EntityLinkProps, OpsSpecSummary } from "./types";

export type UiPlugin = {
    id: string;
    routes?: () => React.ReactNode;
    renderNavSections?: (ctx: {
        specs: OpsSpecSummary[];
        collapsed: boolean;
        pathname: string;
        capabilities: Capabilities | null;
    }) => React.ReactNode;
    renderLegacyNavItems?: (ctx: {
        hasExplicitSpecs: boolean;
        capabilities: Capabilities | null;
    }) => Array<{ key: string; href: string; label: string }>;
    PluginSection?: React.ComponentType<{ enrichments?: Enrichment[] }>;
    obsPagePrefixes?: string[];
    opsApiExtensions?: Record<string, unknown>;
    EntityLink?: React.ComponentType<EntityLinkProps>;
};

const plugins: UiPlugin[] = [];

export function registerUiPlugin(plugin: UiPlugin): void {
    if (plugins.some((entry) => entry.id === plugin.id)) {
        throw new Error(`UI plugin "${plugin.id}" is already registered`);
    }
    plugins.push(plugin);
}

export function getUiPlugins(): readonly UiPlugin[] {
    return plugins;
}

export function getUiPlugin(id: string): UiPlugin | undefined {
    return plugins.find((entry) => entry.id === id);
}

export function renderPluginRoutes(): React.ReactNode {
    return (
        <>
            {plugins.map((plugin) => (
                <React.Fragment key={plugin.id}>{plugin.routes?.()}</React.Fragment>
            ))}
        </>
    );
}

export function renderPluginNavSections(ctx: {
    specs: OpsSpecSummary[];
    collapsed: boolean;
    pathname: string;
    capabilities: Capabilities | null;
}): React.ReactNode {
    return (
        <>
            {plugins.map((plugin) => (
                <React.Fragment key={plugin.id}>{plugin.renderNavSections?.(ctx)}</React.Fragment>
            ))}
        </>
    );
}

export function collectLegacyNavItems(ctx: {
    hasExplicitSpecs: boolean;
    capabilities: Capabilities | null;
}): Array<{ key: string; href: string; label: string }> {
    return plugins.flatMap((plugin) => plugin.renderLegacyNavItems?.(ctx) ?? []);
}

export function getObsPagePrefixes(): string[] {
    return plugins.flatMap((plugin) => plugin.obsPagePrefixes ?? []);
}

export function mergeOpsApiExtensions(): Record<string, unknown> {
    return Object.assign({}, ...plugins.map((plugin) => plugin.opsApiExtensions ?? {}));
}

export function invokeOpsApiExtension<T>(method: string, ...args: unknown[]): Promise<T> | undefined {
    const fn = mergeOpsApiExtensions()[method];
    if (typeof fn !== "function") return undefined;
    return (fn as (...callArgs: unknown[]) => Promise<T>)(...args);
}

export function renderPluginSections(props: { enrichments?: Enrichment[] }): React.ReactNode {
    return (
        <>
            {plugins.map((plugin) => {
                const Section = plugin.PluginSection;
                return Section ? <Section key={plugin.id} {...props} /> : null;
            })}
        </>
    );
}

export function renderEntityLink(props: EntityLinkProps): React.ReactNode {
    for (const plugin of plugins) {
        if (plugin.EntityLink) {
            const Link = plugin.EntityLink;
            return <Link {...props} />;
        }
    }
    return <>{props.children ?? props.id}</>;
}

export type { EntityLinkProps, OpsSpecSummary } from "./types";
