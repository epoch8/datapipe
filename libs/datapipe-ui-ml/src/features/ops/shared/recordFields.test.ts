import { recordFieldOrder } from "./recordFields";
import { frozenDatasetHighlightFields } from "../frozen-datasets/datasetRecordFields";
import { modelHighlightFields } from "../models/modelRecordFields";
import type { OpsSpecDetail } from "../../../types/opsSpecs";

describe("recordFields", () => {
    const detectionSpec: OpsSpecDetail = {
        id: "cat_dog_smoke_yolo",
        title: "Cat / Dog Smoke YOLO",
        description: "",
        icon: "shield",
        color: "blue",
        has_frozen_datasets: true,
        has_training: true,
        metric_tables_count: 1,
        class_metric_tables_count: 1,
        metrics: [],
        class_metrics: [],
        frozen_dataset: {
            table: "detection_frozen_dataset",
            id_column: "detection_frozen_dataset_id",
            created_at_column: "detection_frozen_dataset__created_at",
            split_columns: {
                train: "detection_frozen_dataset__train_images_count",
                val: "detection_frozen_dataset__val_images_count",
                test: "detection_frozen_dataset__test_images_count",
            },
        },
        model: {
            table: "detection_model",
            id_column: "detection_model_id",
            artifact_uri_column: "detection_model__model_path",
        },
        training: {
            status_table: "detection_training_status",
            columns: [
                {
                    id: "run_id",
                    label: "Run ID",
                    source: "training_status__run_key",
                    link_to: "training_run",
                },
                {
                    id: "detection_model_id",
                    label: "Model",
                    source: "detection_model_id",
                    link_to: "model",
                },
                {
                    id: "detection_frozen_dataset_id",
                    label: "Frozen dataset",
                    source: "detection_frozen_dataset_id",
                    link_to: "frozen_dataset",
                },
            ],
        },
        relations: [
            {
                id: "model_trained_on_frozen_dataset",
                table: "detection_model_is_trained_on_detection_frozen_dataset",
                from_entity: "model",
                from_column: "detection_model_id",
                to_entity: "frozen_dataset",
                to_column: "detection_frozen_dataset_id",
            },
        ],
    };

    it("orders pk first then spec highlights then remaining fields", () => {
        const record = {
            detection_frozen_dataset_id: "fd_1",
            detection_frozen_dataset__created_at: "2025-07-01",
            detection_frozen_dataset__train_images_count: 80,
            extra_field: "x",
        };
        const fields = recordFieldOrder({
            record,
            sourcePk: { detection_frozen_dataset_id: "fd_1" },
            highlightFields: frozenDatasetHighlightFields(detectionSpec),
        });
        expect(fields[0]).toBe("detection_frozen_dataset_id");
        expect(fields).toContain("detection_frozen_dataset__train_images_count");
        expect(fields).toContain("extra_field");
    });

    it("derives model highlight fields from spec and relations", () => {
        expect(modelHighlightFields(detectionSpec)).toEqual(
            expect.arrayContaining([
                "detection_model_id",
                "detection_model__model_path",
                "detection_frozen_dataset_id",
            ]),
        );
    });
});
