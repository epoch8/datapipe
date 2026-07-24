// Polyfills for antd components under jsdom (matchMedia, ResizeObserver).
// Runs before the test framework is installed (jest `setupFiles`).
export {};

if (typeof window !== "undefined" && typeof window.matchMedia !== "function") {
    Object.defineProperty(window, "matchMedia", {
        writable: true,
        value: (query: string) => ({
            matches: false,
            media: query,
            onchange: null,
            addListener: () => undefined,
            removeListener: () => undefined,
            addEventListener: () => undefined,
            removeEventListener: () => undefined,
            dispatchEvent: () => false,
        }),
    });
}

if (typeof globalThis !== "undefined" && typeof (globalThis as { ResizeObserver?: unknown }).ResizeObserver === "undefined") {
    class ResizeObserverStub {
        observe(): void {
            /* no-op */
        }
        unobserve(): void {
            /* no-op */
        }
        disconnect(): void {
            /* no-op */
        }
    }
    (globalThis as { ResizeObserver?: unknown }).ResizeObserver = ResizeObserverStub;
}
