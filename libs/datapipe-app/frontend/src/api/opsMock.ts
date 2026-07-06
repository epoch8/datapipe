import type {
    ClassMetricDetailResponse,
    ClassMetricsResponse,
    FrozenDatasetsResponse,
    MetricsRunsResponse,
    MetricsSummaryResponse,
    MetricsTimeseriesResponse,
    RunsListParams,
    RunsListResponse,
    RunListRow,
    SqlQueryResponse,
    SqlSchemaResponse,
    TrainingCompareResponse,
    TrainingRunsResponse,
} from "../types/ops";

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

function makeRuns(): MetricsRunsResponse["rows"] {
    const rand = seededRandom(42);
    const rows: MetricsRunsResponse["rows"] = [];
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
                images_support: 15000 + Math.floor(rand() * 5000),
                support: 8000 + Math.floor(rand() * 3000),
            },
            delta_pct: { mAP50: (rand() - 0.3) * 8, recall: i === 0 ? -1.0 : (rand() - 0.4) * 4 },
            tags: i % 3 === 0 ? ["baseline"] : i % 4 === 0 ? ["exp"] : [],
        });
    }
    return rows.sort((a, b) => (b.started_at ?? "").localeCompare(a.started_at ?? ""));
}

const MOCK_FROZEN_DATASETS: FrozenDatasetsResponse["rows"] = [
    {
        dataset_id: "20260706_1515_yolov8n-320-batch16-epochs30-cfg-edc8a3153_e2e",
        frozen_at: new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
        train_count: 120,
        val_count: 30,
        test_count: 10,
    },
    {
        dataset_id: "20260701_0900_yolov8n-smoke",
        frozen_at: new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString(),
        train_count: 80,
        val_count: 20,
        test_count: 0,
    },
];

const MOCK_RUNS = makeRuns().map((row, i) => ({
    ...row,
    dataset_id: MOCK_FROZEN_DATASETS[i % MOCK_FROZEN_DATASETS.length].dataset_id,
    train_items: MOCK_FROZEN_DATASETS[i % MOCK_FROZEN_DATASETS.length].train_count,
    val_items: MOCK_FROZEN_DATASETS[i % MOCK_FROZEN_DATASETS.length].val_count,
}));

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
            rows: MOCK_RUNS.slice(offset, offset + limit),
            total: MOCK_RUNS.length,
            available_filters: {
                subsets: SUBSETS,
                models: MODELS,
                tags: ["baseline", "exp"],
                metrics: ["mAP50", "mAP50_95", "precision", "recall", "f1_score", "iou_mean"],
            },
        };
    },

    getMetricsSummary(pipelineId: string): MetricsSummaryResponse {
        const best = [...MOCK_RUNS].sort((a, b) => (b.metrics.mAP50_95 ?? 0) - (a.metrics.mAP50_95 ?? 0))[0];
        const latest = MOCK_RUNS[0];
        const previous = MOCK_RUNS[1];
        return {
            pipeline_id: pipelineId,
            primary_metric: "mAP50_95",
            latest_run: latest,
            best_run: best,
            previous_run: previous,
            has_metrics: true,
            kpis: [
                { key: "mAP50", label: "mAP50", value: latest.metrics.mAP50, delta_pct: latest.delta_pct?.mAP50, format: "float", higher_is_better: true, trend: MOCK_RUNS.slice(0, 10).reverse().map((r) => ({ x: r.run_id, y: r.metrics.mAP50 })) },
                { key: "mAP50_95", label: "mAP50-95", value: latest.metrics.mAP50_95, delta_pct: 3.6, format: "float", higher_is_better: true, trend: MOCK_RUNS.slice(0, 10).reverse().map((r) => ({ x: r.run_id, y: r.metrics.mAP50_95 })) },
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

    getMetricsTimeseries(): MetricsTimeseriesResponse {
        return {
            series: [
                { key: "mAP50-test", label: "mAP50 (test)", metric: "mAP50", subset: "test", points: MOCK_RUNS.filter((r) => r.subset === "test").slice(0, 15).reverse().map((r) => ({ x: r.started_at?.slice(0, 10) ?? r.run_id, y: r.metrics.mAP50, run_id: r.run_id })) },
                { key: "f1-test", label: "F1 (test)", metric: "f1_score", subset: "test", points: MOCK_RUNS.filter((r) => r.subset === "test").slice(0, 15).reverse().map((r) => ({ x: r.started_at?.slice(0, 10) ?? r.run_id, y: r.metrics.f1_score, run_id: r.run_id })) },
            ],
        };
    },

    getClassMetrics(): ClassMetricsResponse {
        const rand = seededRandom(99);
        const rows = CLASSES.map((label, i) => ({
            label,
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
            trends: [{ metric: "f1_score", points: MOCK_RUNS.slice(0, 8).reverse().map((r) => ({ x: r.run_id, y: (latest.f1_score ?? 0.8) + (Math.random() - 0.5) * 0.05, run_id: r.run_id })) }],
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

    compareTraining(runKeys: string[]): TrainingCompareResponse {
        const metrics = [
            { key: "train_box_loss", label: "train/box_loss", group: "loss" },
            { key: "metrics_mAP_0_5", label: "metrics/mAP50", group: "metrics", higher_is_better: true },
            { key: "metrics_mAP_0_5_to_0_95", label: "metrics/mAP50-95", group: "metrics", higher_is_better: true },
            { key: "lr_pg0", label: "lr/learning_rate", group: "learning_rate" },
        ];
        const charts = metrics.map((m) => ({
            metric: m.key,
            title: m.label,
            x_label: "epoch",
            series: runKeys.map((run_key, ri) => ({
                run_key,
                label: run_key,
                color_key: String(ri),
                points: Array.from({ length: 50 }, (_, e) => ({
                    x: e,
                    y: m.group === "loss" ? 2 - e * 0.03 + ri * 0.1 + Math.random() * 0.1 : 0.3 + e * 0.01 + ri * 0.02,
                })),
            })),
        }));
        return { runs: opsMock.getTrainingRuns().rows.filter((r) => runKeys.includes(r.run_key)), available_metrics: metrics, charts, run_keys: runKeys };
    },

    getSqlSchema(): SqlSchemaResponse {
        return {
            datasource: "datapipe_analytics",
            tables: [
                { name: "metrics_on_subset", row_count: 28, columns: [{ name: "run_id" }, { name: "model_id" }, { name: "subset" }, { name: "mAP50" }, { name: "precision" }, { name: "recall" }, { name: "f1_score" }] },
                { name: "metrics_by_class", row_count: 34, columns: [{ name: "label" }, { name: "support" }, { name: "precision" }, { name: "recall" }, { name: "f1_score" }] },
                { name: "training_runs", row_count: 18, columns: [{ name: "run_key" }, { name: "model_id" }, { name: "status" }] },
                { name: "predictions", row_count: 46, columns: [{ name: "run_id" }, { name: "image_id" }] },
                { name: "artifacts", row_count: 16, columns: [{ name: "run_key" }, { name: "path" }, { name: "size_bytes" }] },
            ],
        };
    },

    runSqlQuery(): SqlQueryResponse {
        return {
            columns: [{ name: "run_id" }, { name: "model_id" }, { name: "subset" }, { name: "run_date" }, { name: "mAP50" }, { name: "precision" }, { name: "recall" }, { name: "f1" }],
            rows: MOCK_RUNS.slice(0, 25).map((r) => ({
                run_id: r.run_id,
                model_id: r.model_id,
                subset: r.subset,
                run_date: r.started_at?.slice(0, 10),
                mAP50: r.metrics.mAP50,
                precision: r.metrics.precision,
                recall: r.metrics.recall,
                f1: r.metrics.f1_score,
            })),
            total: 1000,
            execution_ms: 123,
        };
    },
};
