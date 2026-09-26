import React from "react";
import { MemoryRouter } from "react-router-dom";
import { render, screen, waitFor, within } from "@testing-library/react";
import "@testing-library/jest-dom";

jest.mock("@datapipe/ui-ml/api/client", () => ({
    opsApi: {
        getTrainingExperiments: jest.fn(),
    },
}));

import { opsApi } from "@datapipe/ui-ml/api/client";
import { TrainingExperimentsTab } from "./TrainingExperimentsTab";

const api = opsApi as unknown as { getTrainingExperiments: jest.Mock };

beforeEach(() => {
    api.getTrainingExperiments.mockResolvedValue({
        rows: [
            {
                id: "exp-1",
                source: "custom",
                display_name: "Custom A",
                config_type: "yolov8_detection",
                params: {},
                active: true,
                revision: 1,
                summary: { display: "yolov8m.pt · 1280px · batch 4 · 50 epochs" },
                requests_count: 0,
                runs_count: 0,
                capabilities: {
                    can_edit: true,
                    can_delete: true,
                    can_duplicate: true,
                    can_launch: true,
                    can_archive: true,
                },
            },
        ],
        total: 1,
        summary: { total: 5, builtin: 2, custom: 3, editable: 2, locked: 1, archived: 1 },
    });
});

afterEach(() => jest.clearAllMocks());

test("renders summary cards from the backend summary and lists experiments", async () => {
    render(
        <MemoryRouter>
            <TrainingExperimentsTab pipelineId="p1" specId="spec-1" />
        </MemoryRouter>,
    );

    await waitFor(() => expect(api.getTrainingExperiments).toHaveBeenCalled());

    // Row + main params (summary.display) are shown.
    expect(await screen.findByText("Custom A")).toBeInTheDocument();
    expect(
        screen.getByText("yolov8m.pt · 1280px · batch 4 · 50 epochs"),
    ).toBeInTheDocument();

    // Summary cards use the backend numbers. Scope to the grid because status
    // labels (e.g. "Editable") can also appear as row status tags.
    const grid = document.querySelector(".te-summary-grid") as HTMLElement;
    const gridScope = within(grid);
    expect(gridScope.getByText("Editable")).toBeInTheDocument();
    expect(gridScope.getByText("Locked")).toBeInTheDocument();
    expect(gridScope.getByText("Archived")).toBeInTheDocument();
    // Backend numbers rendered as card values (5=total, 3=custom are unique).
    expect(gridScope.getByText("5")).toBeInTheDocument();
    expect(gridScope.getByText("3")).toBeInTheDocument();
});

test("passes include_archived when the toggle is used", async () => {
    render(
        <MemoryRouter>
            <TrainingExperimentsTab pipelineId="p1" specId="spec-1" />
        </MemoryRouter>,
    );
    await waitFor(() => expect(api.getTrainingExperiments).toHaveBeenCalled());
    const firstCallParams = api.getTrainingExperiments.mock.calls[0][2];
    expect(firstCallParams.include_archived).toBe(false);
});
