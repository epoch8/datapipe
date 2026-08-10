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

    it("preserves stage labels on the expanded blue frame", () => {
        const data = makeMetaGraph();
        (data.pipeline[0] as { labels?: string[][] }).labels = [["stage", "annotation"]];
        const { nodes } = reprocessData(data, new Set(["G"]));
        expect(nodes.get("G")?.type).toBe("group-expanded");
        expect(nodes.get("G")?.labels).toEqual([["stage", "annotation"]]);
    });
});

describe("reprocessData duplicate meta names", () => {
    it("keeps two metas with the same display name as distinct graph nodes", () => {
        const catalog = {
            a: table("a"),
            b: table("b"),
            c: table("c"),
            d: table("d"),
        };
        const data: GraphData = {
            catalog,
            pipeline: [
                {
                    id: "inf1",
                    name: "Inference_DetectionModel",
                    type: "meta",
                    inputs: ["a"],
                    outputs: ["b"],
                    graph: {
                        catalog,
                        pipeline: [
                            {
                                id: "t_early",
                                name: "detection_model_inference_early",
                                type: "transform",
                                inputs: ["a"],
                                outputs: ["b"],
                            },
                        ],
                    },
                },
                {
                    id: "mid",
                    name: "mid_step",
                    type: "transform",
                    inputs: ["b"],
                    outputs: ["c"],
                },
                {
                    id: "inf2",
                    name: "Inference_DetectionModel",
                    type: "meta",
                    inputs: ["c"],
                    outputs: ["d"],
                    graph: {
                        catalog,
                        pipeline: [
                            {
                                id: "t_late",
                                name: "detection_model_inference_late",
                                type: "transform",
                                inputs: ["c"],
                                outputs: ["d"],
                            },
                        ],
                    },
                },
            ],
        };

        const collapsed = reprocessData(data, new Set());
        expect(collapsed.nodes.get("Inference_DetectionModel__0000")?.type).toBe("group");
        expect(collapsed.nodes.get("Inference_DetectionModel__0002")?.type).toBe("group");
        expect(collapsed.nodes.get("Inference_DetectionModel")).toBeUndefined();

        const expandedLate = reprocessData(data, new Set(["Inference_DetectionModel__0002"]));
        expect(expandedLate.nodes.get("Inference_DetectionModel__0002")?.type).toBe(
            "group-expanded",
        );
        expect(expandedLate.nodes.get("detection_model_inference_late")?.metaGroup).toBe(
            "Inference_DetectionModel__0002",
        );
        expect(expandedLate.nodes.get("detection_model_inference_early")).toBeUndefined();
        expect(expandedLate.nodes.get("Inference_DetectionModel__0000")?.type).toBe("group");
    });
});

describe("reprocessData sequential next-step edges", () => {
    it("adds sequential hops between consecutive top-level steps", () => {
        const catalog = {
            a_in: table("a_in"),
            a_out: table("a_out"),
            b_in: table("b_in"),
            b_out: table("b_out"),
        };
        const data: GraphData = {
            catalog,
            pipeline: [
                {
                    id: "t1",
                    name: "t1",
                    type: "transform",
                    inputs: ["a_in"],
                    outputs: ["a_out"],
                },
                {
                    id: "t2",
                    name: "t2",
                    type: "transform",
                    inputs: ["b_in"],
                    outputs: ["b_out"],
                },
            ],
        };

        const { edges } = reprocessData(data, new Set());
        const sequential = Array.from(edges).filter((e) => e.sequential);
        expect(sequential).toEqual(
            expect.arrayContaining([
                expect.objectContaining({ source: "t1", target: "t2", sequential: true }),
            ]),
        );
    });

    it("adds sequential hops between consecutive transforms inside an expanded meta", () => {
        const { edges } = reprocessData(makeMetaGraph(), new Set(["G"]));
        const sequential = Array.from(edges).filter((e) => e.sequential);
        expect(sequential).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    source: "t1",
                    target: "t2",
                    sequential: true,
                    internalMeta: "G",
                }),
            ]),
        );
    });
});

describe("reprocessData table first-use chronology", () => {
    it("keeps a table at its earliest pipeline use even if a later step produces it", () => {
        const catalog = {
            image__ground_truth: table("image__ground_truth"),
            s3_images: table("s3_images"),
            sec__image_without_ground_truth: table("sec__image_without_ground_truth"),
            annotations_raw: table("annotations_raw"),
        };
        const data: GraphData = {
            catalog,
            pipeline: [
                {
                    id: "get_images_without_ground_truth",
                    name: "get_images_without_ground_truth",
                    type: "transform",
                    inputs: ["s3_images", "image__ground_truth"],
                    outputs: ["sec__image_without_ground_truth"],
                },
                {
                    id: "mid_step",
                    name: "mid_step",
                    type: "transform",
                    inputs: ["sec__image_without_ground_truth"],
                    outputs: ["annotations_raw"],
                },
                {
                    id: "parse_annotations",
                    name: "parse_annotations",
                    type: "transform",
                    inputs: ["annotations_raw"],
                    outputs: ["image__ground_truth"],
                },
            ],
        };

        const { nodes } = reprocessData(data, new Set());
        const gt = nodes.get("image__ground_truth");
        const early = nodes.get("get_images_without_ground_truth");
        expect(early?.pipelineOrderKey).toBe("0000");
        // First use is as input to step 0000 — must not jump to late producer 0002.out.
        expect(gt?.pipelineOrderKey).toBe("0000.in.0001");
        expect(gt?.tableOrderSource).toBe("consumer");
        expect(nodes.get("parse_annotations")?.pipelineOrderKey).toBe("0002");
    });
});
