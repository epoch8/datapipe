const svgAttrs = 'viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"';

export const tableIconSvg = `<svg ${svgAttrs} class="node-icon-svg"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18M3 15h18M9 3v18"/></svg>`;

export const transformIconSvg = `<svg ${svgAttrs} class="node-icon-svg"><path d="M12 3v3M12 18v3M3 12h3M18 12h3"/><path d="M5.6 5.6l2.1 2.1M16.3 16.3l2.1 2.1M5.6 18.4l2.1-2.1M16.3 7.7l2.1-2.1"/><circle cx="12" cy="12" r="2.5"/></svg>`;

export const groupIconSvg = `<svg ${svgAttrs} class="node-icon-svg"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><path d="M3.3 7.7 12 12.5l8.7-4.8M12 22V12.5"/></svg>`;

export const graphCardIconSvg = `<svg ${svgAttrs.replace('2.2', '2')} width="22" height="22"><circle cx="12" cy="12" r="3"/><path d="M12 2v4M12 18v4M2 12h4M18 12h4"/></svg>`;
