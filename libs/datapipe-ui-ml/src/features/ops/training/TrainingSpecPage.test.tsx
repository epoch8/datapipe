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
        getOpsTrainingRows: jest.fn(),
        getTrainingExperiments: jest.fn(),
    },
}));

import { opsApi } from "@datapipe/ui-ml/api/client";
import { TrainingSpecPage } from "./TrainingSpecPage";

const api = opsApi as unknown as {
    getOpsSpec: jest.Mock;
    getOpsTrainingRows: jest.Mock;
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
            columns: [
                { id: "run", label: "Run", source: "run_key", link_to: "training_run" },
                { id: "status", label: "Status", source: "status", kind: "status" },
            ],
        },
    });
    api.getOpsTrainingRows.mockResolvedValue({
        rows: [{ run_key: "r1", status: "success" }],
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
    expect(api.getOpsTrainingRows).not.toHaveBeenCalled();
});

test("switches to the runs tab and loads runs", async () => {
    const user = userEvent.setup();
    renderPage();
    await screen.findByRole("tab", { name: "Runs" });

    await user.click(screen.getByRole("tab", { name: "Runs" }));

    await waitFor(() => expect(api.getOpsTrainingRows).toHaveBeenCalled());
});

test("honors the ?tab=runs URL parameter", async () => {
    renderPage("/training/spec-1?tab=runs");
    await waitFor(() => expect(api.getOpsSpec).toHaveBeenCalled());
    const runsTab = await screen.findByRole("tab", { name: "Runs" });
    expect(runsTab).toHaveAttribute("aria-selected", "true");
    await waitFor(() => expect(api.getOpsTrainingRows).toHaveBeenCalled());
});
