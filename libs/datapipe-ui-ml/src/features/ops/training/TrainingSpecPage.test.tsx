import React from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import "@testing-library/jest-dom";

jest.mock("@datapipe/ui/hooks/usePipelineId", () => ({
    usePipelineId: () => ({ pipelineId: "p1", loading: false }),
}));

jest.mock("@datapipe/ui-ml/api/client", () => ({
    opsApi: {
        getOpsSpec: jest.fn(),
        getTrainingRequests: jest.fn(),
        getTrainingExperiments: jest.fn(),
    },
}));

import { opsApi } from "@datapipe/ui-ml/api/client";
import { TrainingSpecPage } from "./TrainingSpecPage";

const api = opsApi as unknown as {
    getOpsSpec: jest.Mock;
    getTrainingRequests: jest.Mock;
    getTrainingExperiments: jest.Mock;
};

beforeEach(() => {
    api.getOpsSpec.mockResolvedValue({
        id: "spec-1",
        title: "Detector",
        description: "",
        icon: "",
        color: "blue",
        has_image: false,
        has_model_predictions: false,
        has_frozen_datasets: true,
        has_training: true,
        metric_tables_count: 0,
        class_metric_tables_count: 0,
        metrics: [],
        class_metrics: [],
        training: {
            status_table: "runs",
            columns: [],
        },
    });
    api.getTrainingRequests.mockResolvedValue({
        rows: [
            {
                id: "req-1",
                kind: "manual",
                state: "completed",
                train_config_id: "cfg-1",
                config_name: "Baseline",
                frozen_dataset_id: "ds-1",
                run_key: "r1",
                model_id: "model-1",
                status: "completed",
            },
        ],
        total: 1,
    });
    api.getTrainingExperiments.mockResolvedValue({
        rows: [],
        total: 0,
        summary: { total: 0, builtin: 0, custom: 0, editable: 0, locked: 0, archived: 0 },
    });
});

afterEach(() => jest.clearAllMocks());

function renderPage(initialPath = "/training/spec-1") {
    return render(
        <MemoryRouter initialEntries={[initialPath]}>
            <Routes>
                <Route path="/training/:specId" element={<TrainingSpecPage />} />
            </Routes>
        </MemoryRouter>,
    );
}

test("defaults to the experiments tab", async () => {
    renderPage();
    await waitFor(() => expect(api.getOpsSpec).toHaveBeenCalled());

    const experimentsTab = await screen.findByRole("tab", { name: "Experiments" });
    expect(experimentsTab).toHaveAttribute("aria-selected", "true");
    await waitFor(() => expect(api.getTrainingExperiments).toHaveBeenCalled());
    expect(api.getTrainingRequests).not.toHaveBeenCalled();
});

test("switches to the requests tab and loads requests", async () => {
    const user = userEvent.setup();
    renderPage();
    await screen.findByRole("tab", { name: "Requests" });

    await user.click(screen.getByRole("tab", { name: "Requests" }));

    await waitFor(() => expect(api.getTrainingRequests).toHaveBeenCalled());
    expect(await screen.findByText("req-1")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "r1" })).toHaveAttribute(
        "href",
        "/training/spec-1/runs/r1",
    );
    expect(screen.getByRole("link", { name: "model-1" })).toHaveAttribute(
        "href",
        expect.stringContaining("/metrics/spec-1/models/model-1"),
    );
});

test("honors the ?tab=requests URL parameter", async () => {
    renderPage("/training/spec-1?tab=requests");
    await waitFor(() => expect(api.getOpsSpec).toHaveBeenCalled());
    const requestsTab = await screen.findByRole("tab", { name: "Requests" });
    expect(requestsTab).toHaveAttribute("aria-selected", "true");
    await waitFor(() => expect(api.getTrainingRequests).toHaveBeenCalled());
});

test("accepts legacy ?tab=runs as requests", async () => {
    renderPage("/training/spec-1?tab=runs");
    await waitFor(() => expect(api.getOpsSpec).toHaveBeenCalled());
    const requestsTab = await screen.findByRole("tab", { name: "Requests" });
    expect(requestsTab).toHaveAttribute("aria-selected", "true");
    await waitFor(() => expect(api.getTrainingRequests).toHaveBeenCalled());
});
