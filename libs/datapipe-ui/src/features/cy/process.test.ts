import { reprocessData } from "./process";
import type { GraphData, PipeTable } from "../../types";

function table(id: string): PipeTable {
    return { id, indexes: ["id"], store_class: "db", type: "table" };
}

function makeMetaGraph(): GraphData {
    const catalog = {
        in_a: table("in_a"),
        in_b: table("in_b"),
        mid: table("mid"),
        out_x: table("out_x"),
    };
    return {
        catalog,
        pipeline: [
            {
                id: "G",
                name: "G",
                type: "meta",
                inputs: ["in_a", "in_b"],
                outputs: ["out_x"],
                graph: {
                    catalog,
                    pipeline: [
                        {
                            id: "t1",
                            name: "t1",
                            type: "transform",
                            inputs: ["in_a", "in_b"],
                            outputs: ["mid"],
                        },
                        {
                            id: "t2",
                            name: "t2",
                            type: "transform",
                            inputs: ["mid"],
                            outputs: ["out_x"],
                        },
                    ],
                },
            },
        ],
    };
}

describe("reprocessData expanded boundary tables", () => {
    it("keeps declared meta inputs/outputs outside the blue frame", () => {
        const { nodes } = reprocessData(makeMetaGraph(), new Set(["G"]));

        expect(nodes.get("G")?.type).toBe("group-expanded");
        expect(nodes.get("t1")?.metaGroup).toBe("G");
        expect(nodes.get("t2")?.metaGroup).toBe("G");
        expect(nodes.get("mid")?.metaGroup).toBe("G");

        // Declared meta inputs must not be framed as inner steps.
        expect(nodes.get("in_a")?.metaGroup).toBeUndefined();
        expect(nodes.get("in_b")?.metaGroup).toBeUndefined();
        // Declared meta outputs stay outside too.
        expect(nodes.get("out_x")?.metaGroup).toBeUndefined();
    });
});
