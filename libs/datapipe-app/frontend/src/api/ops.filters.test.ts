describe("opsApi filter query serialization", () => {
    it("serializes filters as JSON query param", async () => {
        const originalFetch = global.fetch;
        const fetchMock = jest.fn().mockResolvedValue({
            ok: true,
            json: async () => ({ rows: [], total: 0 }),
        });
        global.fetch = fetchMock as typeof fetch;

        const { opsApi } = await import("./ops");
        await opsApi.getOpsMetricRows("pipeline-1", "spec-1", "table-1", {
            filter_mode: "or",
            filters: [
                { column_id: "model", operator: "contains", value: "cat_dog" },
                { column_id: "subset", operator: "equal", value: "val" },
            ],
        });

        expect(fetchMock).toHaveBeenCalled();
        const url = String(fetchMock.mock.calls[0][0]);
        expect(url).toContain("filter_mode=or");
        expect(url).toContain(
            `filters=${encodeURIComponent(
                JSON.stringify([
                    { column_id: "model", operator: "contains", value: "cat_dog" },
                    { column_id: "subset", operator: "equal", value: "val" },
                ]),
            )}`,
        );

        global.fetch = originalFetch;
    });
});
