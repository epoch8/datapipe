export type ApiErrorKind = "offline" | "network" | "http" | "unknown";

export class ApiError extends Error {
    readonly kind: ApiErrorKind;
    readonly status?: number;
    readonly url?: string;
    /** Machine-readable error code from the API envelope `{error:{code}}`. */
    readonly code: string | null;
    /** Structured error details from the API envelope `{error:{details}}`. */
    readonly details: unknown;

    constructor(
        kind: ApiErrorKind,
        message: string,
        options?: {
            status?: number;
            url?: string;
            cause?: unknown;
            code?: string | null;
            details?: unknown;
        },
    ) {
        super(message);
        this.name = "ApiError";
        this.kind = kind;
        this.status = options?.status;
        this.url = options?.url;
        this.code = options?.code ?? null;
        this.details = options?.details;
    }

    toString(): string {
        return this.message;
    }
}

export function isBrowserOffline(): boolean {
    const nav = typeof globalThis !== "undefined" ? globalThis.navigator : undefined;
    return nav !== undefined && nav.onLine === false;
}

function isNetworkFailureMessage(message: string): boolean {
    return /failed to fetch|load failed|networkerror|network request failed/i.test(message);
}

export function toApiError(error: unknown, url?: string): ApiError {
    if (error instanceof ApiError) {
        return error;
    }

    if (isBrowserOffline()) {
        return new ApiError("offline", offlineMessage(), { url, cause: error });
    }

    const rawMessage = error instanceof Error ? error.message : String(error);

    if (
        (error instanceof TypeError && isNetworkFailureMessage(rawMessage)) ||
        isNetworkFailureMessage(rawMessage)
    ) {
        return new ApiError("network", networkMessage(url), { url, cause: error });
    }

    return new ApiError("unknown", rawMessage || "Request failed", { url, cause: error });
}

function offlineMessage(): string {
    return "You are offline. Check your network connection and try again.";
}

function networkMessage(url?: string): string {
    const target = url ? ` (${url})` : "";
    return `Cannot reach the Datapipe API${target}. The server may be stopped or unreachable. Start it with: uv run datapipe --pipeline app:app api`;
}

export function getApiErrorMessage(error: unknown): string {
    return toApiError(error).message;
}

export function getApiErrorDescription(error: unknown): string | undefined {
    const apiError = error instanceof ApiError ? error : toApiError(error);
    if (apiError.kind === "offline") {
        return "Reconnect to the network, then reload the page.";
    }
    if (apiError.kind === "network") {
        return "Open the Ops UI from the same host/port as the API (for example http://localhost:8001) and confirm the process is still running.";
    }
    if (apiError.kind === "http" && apiError.status === 502) {
        return "The API proxy received a bad response. The Datapipe process may have crashed.";
    }
    return undefined;
}

export async function apiFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const url =
        typeof input === "string"
            ? input
            : input instanceof URL
              ? input.href
              : input instanceof Request
                ? input.url
                : undefined;

    if (isBrowserOffline()) {
        throw new ApiError("offline", offlineMessage(), { url });
    }

    try {
        return await fetch(input, init);
    } catch (error) {
        throw toApiError(error, url);
    }
}

export async function readApiErrorBody(res: Response): Promise<string> {
    const text = await res.text();
    if (!text) {
        return res.statusText || `HTTP ${res.status}`;
    }
    try {
        const json = JSON.parse(text) as { detail?: unknown; message?: unknown };
        if (typeof json.detail === "string") return json.detail;
        if (Array.isArray(json.detail)) {
            return json.detail
                .map((item) => (typeof item === "string" ? item : JSON.stringify(item)))
                .join("; ");
        }
        if (typeof json.message === "string") return json.message;
    } catch {
        // plain text body
    }
    return text;
}
