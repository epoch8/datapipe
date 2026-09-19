/** Run statuses that mean the run may still produce updates and logs. */
export function isRunActive(status?: string | null): boolean {
    return status === "pending" || status === "running";
}
