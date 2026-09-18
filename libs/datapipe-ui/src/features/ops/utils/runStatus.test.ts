import { isRunActive } from "./runStatus";

describe("isRunActive", () => {
    it.each(["pending", "running"])("treats %s as active", (status) => {
        expect(isRunActive(status)).toBe(true);
    });

    it.each(["completed", "failed", "interrupted", "", "Running", undefined, null])(
        "treats %s as not active",
        (status) => {
            expect(isRunActive(status)).toBe(false);
        },
    );
});
