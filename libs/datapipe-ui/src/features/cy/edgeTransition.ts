import Cytoscape from "cytoscape";
import { ANIMATION_EASING, ANIMATION_MS } from "./animationConstants";

type CyElementLike = { data: Record<string, unknown> };

export function makeEdgeKey(source: string, target: string): string {
    return `${source}->${target}`;
}

export function parseEdgeKey(key: string): { source: string; target: string } {
    const idx = key.indexOf("->");
    return { source: key.slice(0, idx), target: key.slice(idx + 2) };
}

export function findEdgeByKey(cy: Cytoscape.Core, key: string): Cytoscape.EdgeSingular | null {
    const { source, target } = parseEdgeKey(key);
    const found = cy.edges().filter(
        (edge) => edge.source().id() === source && edge.target().id() === target,
    );
    if (found.empty()) return null;
    return found.first() as Cytoscape.EdgeSingular;
}

export type EdgeDiff = {
    toAdd: string[];
    toRemove: string[];
};

export function computeEdgeDiff(cy: Cytoscape.Core, target: CyElementLike[]): EdgeDiff {
    const targetEdgeKeys = new Set(
        target
            .filter((el) => el.data.source && el.data.target)
            .map((el) => makeEdgeKey(el.data.source as string, el.data.target as string)),
    );
    const currentKeys = new Set<string>();
    cy.edges().forEach((edge) => {
        currentKeys.add(makeEdgeKey(edge.source().id(), edge.target().id()));
    });

    const toAdd = Array.from(targetEdgeKeys).filter((key) => !currentKeys.has(key));
    const toRemove = Array.from(currentKeys).filter((key) => !targetEdgeKeys.has(key));
    return { toAdd, toRemove };
}

export function addEdgesFromTarget(
    cy: Cytoscape.Core,
    target: CyElementLike[],
    keys: Set<string>,
    opacity = 1,
): void {
    if (!keys.size) return;
    cy.batch(() => {
        target
            .filter((el) => el.data.source && el.data.target)
            .forEach((el) => {
                const key = makeEdgeKey(el.data.source as string, el.data.target as string);
                if (!keys.has(key) || findEdgeByKey(cy, key)) return;
                cy.add(el);
                const edge = findEdgeByKey(cy, key);
                if (edge) {
                    edge.style("opacity", opacity);
                }
            });
    });
}

export function resetEdgeOpacities(cy: Cytoscape.Core): void {
    cy.edges().forEach((edge) => {
        (edge as Cytoscape.EdgeSingular).removeStyle("opacity");
    });
}

export function animateEdgeOpacityTransitions(
    cy: Cytoscape.Core,
    fadeIn: Set<string>,
    fadeOut: Set<string>,
    onEdgeComplete: () => void,
    duration = ANIMATION_MS,
    easing = ANIMATION_EASING,
): void {
    fadeOut.forEach((key) => {
        const edge = findEdgeByKey(cy, key);
        if (!edge) {
            onEdgeComplete();
            return;
        }
        edge.animate(
            { style: { opacity: 0 } },
            {
                duration,
                easing: easing as Cytoscape.Css.TransitionTimingFunction,
                complete: () => {
                    edge.remove();
                    onEdgeComplete();
                },
            },
        );
    });

    fadeIn.forEach((key) => {
        const edge = findEdgeByKey(cy, key);
        if (!edge) {
            onEdgeComplete();
            return;
        }
        edge.style("opacity", 0);
        edge.animate(
            { style: { opacity: 1 } },
            {
                duration,
                easing: easing as Cytoscape.Css.TransitionTimingFunction,
                complete: () => {
                    edge.removeStyle("opacity");
                    onEdgeComplete();
                },
            },
        );
    });
}
