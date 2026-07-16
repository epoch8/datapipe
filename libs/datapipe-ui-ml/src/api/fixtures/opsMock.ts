import type { RunsListParams, RunsListResponse, RunListRow } from "@datapipe/ui/types/ops";
import type {
    ClassMetricDetailResponse,
    ClassMetricsResponse,
    FrozenDatasetDetailResponse,
    FrozenDatasetsResponse,
    MetricsModelDetailResponse,
    MetricsModelRow,
    MetricsRunRow,
    MetricsRunsResponse,
    MetricsSummaryResponse,
    MetricsTableSchema,
    TrainingRunsResponse,
} from "../../types/opsMl";

const PIPELINE = "image_detection_e2e";
const MODELS = ["yolo11x.pt", "yolov8l", "yolov8m", "yolov8n"];
const SUBSETS = ["train", "val", "test"];
const CLASSES = ["cat", "dog", "person", "car", "bicycle", "bird", "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe", "backpack", "umbrella"];

function seededRandom(seed: number) {
    let s = seed;
    return () => {
        s = (s * 16807) % 2147483647;
        return (s - 1) / 2147483646;
    };
}

function makeLegacyRuns(): MetricsRunRow[] {
    const rand = seededRandom(42);
    const rows: MetricsRunRow[] = [];
    for (let i = 0; i < 42; i++) {
        const day = new Date();
        day.setDate(day.getDate() - Math.floor(rand() * 30));
        const mAP50 = 0.75 + rand() * 0.12;
        const recall = 0.78 + rand() * 0.08 - (i === 0 ? 0.01 : 0);
        rows.push({
            run_id: `24070${Math.floor(rand() * 9)}-${String(900 + i).slice(-4)}`,
            pipeline_id: PIPELINE,
            model_id: MODELS[i % MODELS.length],
            model_version: `v${18 - (i % 5)}`,
            task_type: "detection",
            subset: SUBSETS[i % 3],
            started_at: day.toISOString(),
            duration_s: 300 + Math.floor(rand() * 600),
            status: i === 5 ? "failed" : "success",
            metrics: {
                mAP50,
                mAP50_95: mAP50 * 0.65,
                precision: 0.8 + rand() * 0.08,
                recall,
                f1_score: 0.79 + rand() * 0.06,
                iou_mean: 0.68 + rand() * 0.08,
                weighted_f1_score: 0.79 + rand() * 0.06,
                weighted_precision: 0.8 + rand() * 0.08,
                weighted_recall: recall,
                macro_f1_score: 0.72 + rand() * 0.06,
                macro_precision: 0.7 + rand() * 0.08,
                macro_recall: 0.75 + rand() * 0.08,
                images_support: 15000 + Math.floor(rand() * 5000),
                support: 8000 + Math.floor(rand() * 3000),
            },
            delta_pct: { mAP50: (rand() - 0.3) * 8, recall: i === 0 ? -1.0 : (rand() - 0.4) * 4 },
            tags: i % 3 === 0 ? ["baseline"] : i % 4 === 0 ? ["exp"] : [],
        });
    }
    return rows.sort((a, b) => (b.started_at ?? "").localeCompare(a.started_at ?? ""));
}

function toModelRows(runs: MetricsRunRow[]): MetricsModelRow[] {
    const byKey = new Map<string, MetricsModelRow>();
    for (const run of runs) {
        const datasetId = run.dataset_id ?? "";
        const key = `${run.model_id}|${datasetId}|${run.subset}`;
        const existing = byKey.get(key);
        const frozen = MOCK_FROZEN_DATASETS.find((d) => d.dataset_id === datasetId);
        const mergedMetrics = { ...(existing?.metrics ?? {}), ...run.metrics };
        byKey.set(key, {
            id: key.replace(/[^a-zA-Z0-9]/g, "").slice(0, 16) || run.run_id,
            pipeline_id: run.pipeline_id,
            model_id: run.model_id,
            model_display_name: run.model_id,
            model_version: run.model_version,
            task_type: run.task_type,
            dataset_id: run.dataset_id,
            frozen_at: frozen?.frozen_at,
            subset: run.subset,
            split_counts: {
                train: frozen?.train_count ?? run.train_items,
                val: frozen?.val_count ?? run.val_items,
                test: frozen?.test_count,
            },
            has_metrics: Object.values(mergedMetrics).some((v) => v != null),
            metrics_state: "computed",
            metrics: mergedMetrics,
            run_id: run.run_id,
            started_at: run.started_at,
            duration_s: run.duration_s,
            status: run.status,
            tags: run.tags,
        });
    }
    return Array.from(byKey.values());
}

const MOCK_SCHEMA: MetricsTableSchema = {
    task_type: "detection",
    primary_metric: "mAP50_95",
    groups: [
        {
            key: "core",
            label: "Core",
            priority: 1,
            metrics: [
                { key: "mAP50_95", label: "mAP50-95", short_label: "mAP", group: "core", task_types: ["detection"], format: "float", higher_is_better: true, visible_by_default: true, primary: true },
                { key: "mAP50", label: "mAP50", short_label: "mAP50", group: "core", task_types: ["detection"], format: "float", higher_is_better: true, visible_by_default: true },
                { key: "support", label: "Support", short_label: "Sup", group: "core", task_types: ["*"], format: "integer", higher_is_better: true, visible_by_default: true },
            ],
        },
        {
            key: "weighted",
            label: "Weighted",
            priority: 2,
            metrics: [
                { key: "weighted_f1_score", label: "F1", short_label: "F1", group: "weighted", task_types: ["*"], format: "float", higher_is_better: true, visible_by_default: true },
                { key: "weighted_precision", label: "Precision", short_label: "P", group: "weighted", task_types: ["*"], format: "float", higher_is_better: true, visible_by_default: true },
                { key: "weighted_recall", label: "Recall", short_label: "R", group: "weighted", task_types: ["*"], format: "float", higher_is_better: true, visible_by_default: true },
            ],
        },
        {
            key: "macro",
            label: "Macro",
            priority: 3,
            metrics: [
                { key: "macro_f1_score", label: "F1", short_label: "F1", group: "macro", task_types: ["*"], format: "float", higher_is_better: true, visible_by_default: true },
                { key: "macro_precision", label: "Precision", short_label: "P", group: "macro", task_types: ["*"], format: "float", higher_is_better: true, visible_by_default: true },
                { key: "macro_recall", label: "Recall", short_label: "R", group: "macro", task_types: ["*"], format: "float", higher_is_better: true, visible_by_default: true },
            ],
        },
    ],
};

const MOCK_FROZEN_DATASETS: FrozenDatasetsResponse["rows"] = [
    {
        dataset_id: "20260706_1515_yolov8n-320-batch16-epochs30-cfg-edc8a3153_e2e",
        frozen_at: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
        train_count: 120,
        val_count: 30,
        test_count: 10,
        models_count: 3,
    },
    {
        dataset_id: "20260701_0900_yolov8n-smoke",
        frozen_at: new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString(),
        train_count: 80,
        val_count: 20,
        test_count: 0,
        models_count: 1,
    },
];

const MOCK_LEGACY_RUNS = makeLegacyRuns().map((row, i) => ({
    ...row,
    dataset_id: MOCK_FROZEN_DATASETS[i % MOCK_FROZEN_DATASETS.length].dataset_id,
    train_items: MOCK_FROZEN_DATASETS[i % MOCK_FROZEN_DATASETS.length].train_count,
    val_items: MOCK_FROZEN_DATASETS[i % MOCK_FROZEN_DATASETS.length].val_count,
}));

const MOCK_MODEL_ROWS: MetricsModelRow[] = [
    ...toModelRows(MOCK_LEGACY_RUNS),
    {
        id: "pending-new-yolo",
        pipeline_id: PIPELINE,
        model_id: "new_yolo_model",
        model_source: "manual",
        task_type: "detection",
        dataset_id: MOCK_FROZEN_DATASETS[0].dataset_id,
        frozen_at: MOCK_FROZEN_DATASETS[0].frozen_at,
        subset: "val",
        split_counts: {
            train: MOCK_FROZEN_DATASETS[0].train_count,
            val: MOCK_FROZEN_DATASETS[0].val_count,
            test: MOCK_FROZEN_DATASETS[0].test_count,
        },
        has_metrics: false,
        metrics_state: "not_computed",
        metrics: {},
    },
];

const MOCK_PIPELINE_RUNS: RunListRow[] = [
    { run_id: "99d91102-a1b2", pipeline_id: PIPELINE, status: "completed", scope: "stage_run", target_label: "count-metrics", started_at: new Date(Date.now() - 2 * 60_000).toISOString(), duration_s: 120, trigger: "api:stage:count-metrics" },
    { run_id: "33e2e1ef-c3d4", pipeline_id: PIPELINE, status: "completed", scope: "stage_run", target_label: "train", started_at: new Date(Date.now() - 28 * 60_000).toISOString(), duration_s: 1680, trigger: "api:stage:train" },
    { run_id: "aa00b7f8-e5f6", pipeline_id: PIPELINE, status: "running", scope: "stage_run", target_label: "train", started_at: new Date(Date.now() - 60 * 60_000).toISOString(), trigger: "api:stage:train" },
    { run_id: "7b2d3c11-a7b8", pipeline_id: PIPELINE, status: "failed", scope: "stage_run", target_label: "inference", started_at: new Date(Date.now() - 3 * 60 * 60_000).toISOString(), duration_s: 900, trigger: "api:stage:inference" },
    ...Array.from({ length: 124 }, (_, i) => ({
        run_id: `run-${String(i).padStart(4, "0")}`,
        pipeline_id: PIPELINE,
        status: i % 17 === 0 ? "failed" : "completed",
        scope: "full_pipeline" as const,
        target_label: "all labels",
        started_at: new Date(Date.now() - (i + 4) * 3600_000).toISOString(),
        duration_s: 300 + i * 10,
        trigger: "api:pipeline",
    })),
];

export const opsMock = {
    getFrozenDatasets(): FrozenDatasetsResponse {
        return { rows: MOCK_FROZEN_DATASETS, total: MOCK_FROZEN_DATASETS.length };
    },

    getModelDetail(pipelineId: string, modelId: string, params?: { dataset_id?: string; subset?: string }): MetricsModelDetailResponse {
        let rows = MOCK_MODEL_ROWS.filter((r) => r.model_id === modelId);
        if (params?.dataset_id) rows = rows.filter((r) => r.dataset_id === params.dataset_id);
        if (params?.subset) rows = rows.filter((r) => r.subset === params.subset);
        const primary = rows.find((r) => r.has_metrics) ?? rows[0];
        const datasetId = params?.dataset_id ?? primary?.dataset_id ?? MOCK_FROZEN_DATASETS[0].dataset_id;
        const frozen = MOCK_FROZEN_DATASETS.find((d) => d.dataset_id === datasetId);
        return {
            pipeline_id: pipelineId,
            model_id: modelId,
            title: modelId,
            source_table: "ml_models",
            source_record: {
                model_id: modelId,
                task_type: "detection",
                run_key: primary?.run_key ?? "240701-0921",
                frozen_dataset_id: datasetId,
            },
            source_table_url: `/pipelines/${pipelineId}/tables/ml_models?focus_col=model_id&focus_value=${encodeURIComponent(modelId)}`,
            model_row: primary,
            frozen_dataset: frozen,
            frozen_dataset_source_table: "frozen_datasets",
            metrics_rows: rows.length ? rows : MOCK_MODEL_ROWS.slice(0, 1).map((r) => ({ ...r, model_id: modelId, has_metrics: false, metrics: {} })),
            kpis: primary?.has_metrics
                ? [
                      { key: "weighted_f1_score", label: "W-F1", value: primary.metrics?.weighted_f1_score ?? null, format: "float" },
                      { key: "macro_f1_score", label: "M-F1", value: primary.metrics?.macro_f1_score ?? null, format: "float" },
                      { key: "support", label: "Support", value: primary.metrics?.support ?? null, format: "integer" },
                  ]
                : [],
            related: { dataset_id: datasetId, run_id: primary?.run_id, run_key: primary?.run_key },
        };
    },

    getFrozenDatasetDetail(pipelineId: string, datasetId: string, params?: { subset?: string }): FrozenDatasetDetailResponse {
        const dataset = MOCK_FROZEN_DATASETS.find((d) => d.dataset_id === datasetId) ?? {
            dataset_id: datasetId,
            train_count: 0,
            val_count: 0,
            test_count: 0,
        };
        let models = MOCK_MODEL_ROWS.filter((r) => r.dataset_id === datasetId);
        if (params?.subset) models = models.filter((r) => r.subset === params.subset);
        const withMetrics = models.filter((r) => r.has_metrics);
        const best = withMetrics.sort((a, b) => (b.metrics?.weighted_f1_score ?? 0) - (a.metrics?.weighted_f1_score ?? 0))[0];
        const linkedByModel = new Map<string, (typeof models)[number]>();
        for (const row of models) {
            if (!linkedByModel.has(row.model_id)) linkedByModel.set(row.model_id, row);
        }
        const linked_models = Array.from(linkedByModel.entries()).map(([model_id, row]) => ({
            model_id,
            created_at: row.started_at ?? null,
            run_key: row.run_key ?? null,
            run_id: row.run_id ?? null,
            link_table: "detection_model_is_trained_on_detection_frozen_dataset",
            link_record: {
                detection_model_id: model_id,
                detection_frozen_dataset_id: datasetId,
            },
        }));
        return {
            pipeline_id: pipelineId,
            dataset_id: datasetId,
            title: datasetId,
            dataset: { ...dataset, models_count: linked_models.length },
            source_table: "frozen_datasets",
            source_record: {
                frozen_dataset_id: datasetId,
                train_images_count: dataset.train_count,
                val_images_count: dataset.val_count,
                test_images_count: dataset.test_count,
            },
            source_table_url: `/pipelines/${pipelineId}/tables/frozen_datasets?focus_col=frozen_dataset_id&focus_value=${encodeURIComponent(datasetId)}`,
            linked_models,
            coverage: {
                models_total: linked_models.length,
                models_with_metrics: withMetrics.length,
                subsets: Array.from(new Set(models.map((m) => m.subset))),
                best_metric_key: "weighted_f1_score",
                best_metric_value: best?.metrics?.weighted_f1_score ?? null,
                best_model_id: best?.model_id ?? null,
            },
        };
    },

    getRuns(params: RunsListParams = {}): RunsListResponse {
        let rows = [...MOCK_PIPELINE_RUNS];
        if (params.status) rows = rows.filter((r) => r.status === params.status);
        if (params.stage) {
            rows = rows.filter((r) =>
                params.stage === "all labels"
                    ? r.scope === "full_pipeline"
                    : r.target_label === params.stage,
            );
        }
        if (params.search) {
            const q = params.search.toLowerCase();
            rows = rows.filter((r) => r.run_id.toLowerCase().includes(q));
        }
        const counts_by_status = MOCK_PIPELINE_RUNS.reduce<Record<string, number>>((acc, row) => {
            acc[row.status] = (acc[row.status] ?? 0) + 1;
            return acc;
        }, {});
        const offset = params.offset ?? 0;
        const limit = params.limit ?? 25;
        return {
            rows: rows.slice(offset, offset + limit),
            total: rows.length,
            filters: {
                statuses: ["completed", "running", "failed"],
                stages: ["all labels", "train", "inference", "count-metrics", "fiftyone"],
                triggers: ["api:pipeline", "api:stage:train", "api:stage:inference"],
            },
            counts_by_status,
        };
    },

    getMetricsRuns(_pipelineId: string, params?: { limit?: number; offset?: number }): MetricsRunsResponse {
        const offset = params?.offset ?? 0;
        const limit = params?.limit ?? 25;
        return {
            rows: MOCK_MODEL_ROWS.slice(offset, offset + limit),
            total: MOCK_MODEL_ROWS.length,
            available_filters: {
                subsets: SUBSETS,
                models: MODELS,
                tags: ["baseline", "exp"],
                metrics: ["mAP50", "mAP50_95", "precision", "recall", "f1_score", "iou_mean"],
            },
            schema: MOCK_SCHEMA,
        };
    },

    getMetricsSummary(pipelineId: string): MetricsSummaryResponse {
        const best = [...MOCK_LEGACY_RUNS].sort((a, b) => (b.metrics.mAP50_95 ?? 0) - (a.metrics.mAP50_95 ?? 0))[0];
        const latest = MOCK_LEGACY_RUNS[0];
        const previous = MOCK_LEGACY_RUNS[1];
        return {
            pipeline_id: pipelineId,
            primary_metric: "mAP50_95",
            latest_run: latest,
            best_run: best,
            previous_run: previous,
            has_metrics: true,
            kpis: [
                { key: "mAP50", label: "mAP50", value: latest.metrics.mAP50, delta_pct: latest.delta_pct?.mAP50, format: "float", higher_is_better: true, trend: MOCK_LEGACY_RUNS.slice(0, 10).reverse().map((r) => ({ x: r.run_id, y: r.metrics.mAP50 })) },
                { key: "mAP50_95", label: "mAP50-95", value: latest.metrics.mAP50_95, delta_pct: 3.6, format: "float", higher_is_better: true, trend: MOCK_LEGACY_RUNS.slice(0, 10).reverse().map((r) => ({ x: r.run_id, y: r.metrics.mAP50_95 })) },
                { key: "precision", label: "Precision", value: latest.metrics.precision, delta_pct: 2.5, format: "float", higher_is_better: true },
                { key: "recall", label: "Recall", value: latest.metrics.recall, delta_pct: -1.0, format: "float", higher_is_better: true },
                { key: "f1_score", label: "F1 Score", value: latest.metrics.f1_score, delta_pct: 1.2, format: "float", higher_is_better: true },
                { key: "iou_mean", label: "IoU mean", value: latest.metrics.iou_mean, delta_pct: 2.4, format: "float", higher_is_better: true },
                { key: "images_support", label: "Images support", value: latest.metrics.images_support, delta_pct: 5.0, format: "integer", higher_is_better: true },
                { key: "best_model", label: "Best model", value: best.model_id, format: "string", subtitle: `Run #${best.run_id}` },
            ],
            anomalies: [
                { severity: "warning", metric: "recall", title: "Recall dropped -1.0%", description: "Latest test run recall below previous run", run_id: latest.run_id, delta: -0.01 },
                { severity: "info", metric: "iou_mean", title: "IoU mean below trend", description: "IoU slightly below 7-day moving average", run_id: latest.run_id },
            ],
        };
    },

    getClassMetrics(): ClassMetricsResponse {
        const rand = seededRandom(99);
        const rows = CLASSES.map((label, i) => ({
            label,
            subset: "val",
            model_id: "yolov8n-demo",
            class_id: i,
            images_support: 500 + Math.floor(rand() * 2000),
            support: 200 + Math.floor(rand() * 800),
            TP: 100 + Math.floor(rand() * 400),
            FP: Math.floor(rand() * 50),
            FN: Math.floor(rand() * 60),
            precision: 0.7 + rand() * 0.25,
            recall: 0.65 + rand() * 0.3,
            f1_score: 0.68 + rand() * 0.25,
            iou_mean: 0.6 + rand() * 0.25,
            mAP50: 0.72 + rand() * 0.2,
            delta: { f1_score: (rand() - 0.5) * 0.08 },
        }));
        const sorted = [...rows].sort((a, b) => (b.f1_score ?? 0) - (a.f1_score ?? 0));
        const macroF1 = rows.reduce((s, r) => s + (r.f1_score ?? 0), 0) / rows.length;
        return {
            rows,
            total: rows.length,
            summary: {
                total_classes: rows.length,
                macro_f1: macroF1,
                weighted_f1: macroF1 * 0.98,
                best_classes: sorted.slice(0, 3),
                worst_classes: sorted.slice(-3).reverse(),
            },
        };
    },

    getClassDetail(label: string): ClassMetricDetailResponse {
        const classes = opsMock.getClassMetrics();
        const latest = classes.rows.find((r) => r.label === label) ?? classes.rows[0];
        return {
            label,
            class_id: latest.class_id,
            latest,
            previous: { ...latest, f1_score: (latest.f1_score ?? 0) - 0.03 },
            trends: [{ metric: "f1_score", points: MOCK_LEGACY_RUNS.slice(0, 8).reverse().map((r) => ({ x: r.run_id, y: (latest.f1_score ?? 0.8) + (Math.random() - 0.5) * 0.05, run_id: r.run_id })) }],
            error_breakdown: { false_negatives: latest.FN, false_positives: latest.FP, localization_errors: 12, confusions: 8 },
            confusion_top: [{ actual_label: label, predicted_label: "dog", count: 5 }],
        };
    },

    getTrainingRuns(): TrainingRunsResponse {
        const keys = ["240701-0921", "240630-0810", "240628-1542", "240625-1103"];
        const rows = keys.map((run_key, i) => ({
            run_key,
            run_id: run_key,
            model_id: ["yolov8l", "yolov8m", "yolov8l", "yolov8n"][i],
            task_type: "detection",
            framework: i % 2 === 0 ? "YOLOv8" : "YOLOv5",
            dataset: "Coco 2017",
            started_at: new Date(Date.now() - i * 86400000 * 3).toISOString(),
            duration_s: 1800 + i * 300,
            status: i === 3 ? "failed" : "success",
            tags: i === 0 ? ["baseline"] : ["exp"],
            best_metric_name: "mAP50_95",
            best_metric_value: 0.654 - i * 0.02,
            is_best: i === 0,
            params: { epochs: 250, batch: 16, imgsz: 640 },
            artifacts: { "model.pt": "824.7 MB", "config.yaml": "2.1 KB", "train.log": "156 KB" },
        }));
        return {
            rows,
            total: 42,
            filters: { task_types: ["detection", "keypoints"], frameworks: ["YOLOv8", "YOLOv5"], datasets: ["Coco 2017"], statuses: ["success", "failed", "running"], tags: ["baseline", "exp"] },
        };
    },
};
