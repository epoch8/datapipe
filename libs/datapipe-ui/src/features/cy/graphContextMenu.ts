export type GraphContextMenuAction = "open-details" | "expand-steps" | "collapse-steps";

export type GraphContextMenuItem = {
    id: GraphContextMenuAction;
    label: string;
};

type ShowOpts = {
    clientX: number;
    clientY: number;
    items: GraphContextMenuItem[];
    onAction: (id: GraphContextMenuAction) => void;
};

let activeRoot: HTMLDivElement | null = null;

/**
 * Own RMB menu on document.body (outside `#root { zoom }`). A full-screen
 * backdrop captures outside clicks so the zoomed app cannot steal the gesture.
 */
export function isGraphContextMenuTarget(target: EventTarget | null): boolean {
    return Boolean((target as Element | null)?.closest?.("[data-dp-graph-ctx-menu]"));
}

export function hideGraphContextMenu(): void {
    if (activeRoot) {
        activeRoot.remove();
        activeRoot = null;
    }
}

export function showGraphContextMenu(opts: ShowOpts): void {
    hideGraphContextMenu();
    if (!opts.items.length) return;

    const root = document.createElement("div");
    root.setAttribute("data-dp-graph-ctx-menu", "1");
    root.style.cssText =
        "position:fixed;inset:0;z-index:2147483646;pointer-events:auto;";

    const backdrop = document.createElement("div");
    backdrop.setAttribute("data-dp-graph-ctx-menu", "1");
    backdrop.style.cssText = "position:absolute;inset:0;";
    backdrop.addEventListener("pointerdown", (event) => {
        if (event.button !== 0 && event.button !== 2) return;
        event.preventDefault();
        event.stopPropagation();
        hideGraphContextMenu();
    });
    // Prevent the browser context menu on the backdrop.
    backdrop.addEventListener("contextmenu", (event) => {
        event.preventDefault();
        hideGraphContextMenu();
    });

    const menu = document.createElement("div");
    menu.className = "dp-graph-context-menu";
    menu.setAttribute("data-dp-graph-ctx-menu", "1");
    menu.setAttribute("role", "menu");

    for (const item of opts.items) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "dp-graph-context-menu-item";
        btn.setAttribute("role", "menuitem");
        btn.setAttribute("data-dp-graph-ctx-menu", "1");
        btn.textContent = item.label;
        const run = (event: Event) => {
            event.preventDefault();
            event.stopPropagation();
            (event as Event & { stopImmediatePropagation?: () => void }).stopImmediatePropagation?.();
            hideGraphContextMenu();
            opts.onAction(item.id);
        };
        // pointerdown is more reliable than click under CSS zoom / capture races.
        btn.addEventListener("pointerdown", (event) => {
            if (event.button !== 0) return;
            run(event);
        });
        btn.addEventListener("click", run);
        menu.appendChild(btn);
    }

    root.appendChild(backdrop);
    root.appendChild(menu);
    document.body.appendChild(root);

    const pad = 4;
    const menuW = menu.offsetWidth || 200;
    const menuH = menu.offsetHeight || 40;
    let left = opts.clientX;
    let top = opts.clientY;
    left = Math.max(pad, Math.min(left, window.innerWidth - menuW - pad));
    top = Math.max(pad, Math.min(top, window.innerHeight - menuH - pad));
    menu.style.left = `${left}px`;
    menu.style.top = `${top}px`;

    activeRoot = root;
}
