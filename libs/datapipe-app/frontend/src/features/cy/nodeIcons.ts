const svgAttrs = 'viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"';
const toolbarSvgAttrs = 'viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"';

export const tableIconSvg = `<svg ${svgAttrs} class="node-icon-svg"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18M3 15h18M9 3v18"/></svg>`;

export const transformIconSvg = `<svg ${svgAttrs} class="node-icon-svg"><path d="M12 3v3M12 18v3M3 12h3M18 12h3"/><path d="M5.6 5.6l2.1 2.1M16.3 16.3l2.1 2.1M5.6 18.4l2.1-2.1M16.3 7.7l2.1-2.1"/><circle cx="12" cy="12" r="2.5"/></svg>`;

export const groupIconSvg = `<svg ${svgAttrs} class="node-icon-svg"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><path d="M3.3 7.7 12 12.5l8.7-4.8M12 22V12.5"/></svg>`;

/** Pipeline graph card title — workflow / pipeline semantics. */
export const workflowIconSvg = `<svg ${toolbarSvgAttrs} class="toolbar-icon-svg" width="22" height="22"><rect x="3" y="3" width="6" height="6" rx="1"/><rect x="15" y="3" width="6" height="6" rx="1"/><rect x="9" y="15" width="6" height="6" rx="1"/><path d="M6 9v3a1 1 0 0 0 1 1h4M18 9v3a1 1 0 0 1-1 1h-4"/></svg>`;

/** @deprecated use workflowIconSvg */
export const graphCardIconSvg = workflowIconSvg;

export const expandGraphIconSvg = `<svg ${toolbarSvgAttrs} class="toolbar-icon-svg" width="18" height="18"><path d="M15 3h6v6"/><path d="M9 21H3v-6"/><path d="M21 3l-7 7"/><path d="M3 21l7-7"/></svg>`;

export const slidersHorizontalIconSvg = `<svg ${toolbarSvgAttrs} class="toolbar-icon-svg" width="18" height="18"><line x1="21" x2="14" y1="4" y2="4"/><line x1="10" x2="3" y1="4" y2="4"/><line x1="21" x2="12" y1="12" y2="12"/><line x1="8" x2="3" y1="12" y2="12"/><line x1="21" x2="16" y1="20" y2="20"/><line x1="12" x2="3" y1="20" y2="20"/><line x1="14" x2="14" y1="2" y2="6"/><line x1="8" x2="8" y1="10" y2="14"/><line x1="16" x2="16" y1="18" y2="22"/></svg>`;
