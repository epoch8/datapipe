import React from "react";
import { MemoryRouter } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import "@testing-library/jest-dom";

jest.mock("@datapipe/ui-ml/api/client", () => ({
    opsApi: {
        getOpsSpec: jest.fn(),
        getTrainingExperiments: jest.fn(),
        getFrozenDatasets: jest.fn(),
        getTrainingExperimentModels: jest.fn(),
        createTrainingRequest: jest.fn(),
    },
}));

import { opsApi } from "@datapipe/ui-ml/api/client";
import { isFrozenDatasetStale, NewTrainingRunDrawer, parseWithinTimeMs } from "./NewTrainingRunDrawer";

const api = opsApi as unknown as {
    getOpsSpec: jest.Mock;
    getTrainingExperiments: jest.Mock;
    getFrozenDatasets: jest.Mock;
    getTrainingExperimentModels: jest.Mock;
    createTrainingRequest: jest.Mock;
};

function experimentRow(id: string) {
    return {
        id,
        source: "custom",
        display_name: `Experiment ${id}`,
        config_type: "yolov8_detection",
        params: {},
        active: true,
        revision: 1,
        summary: {},
        requests_count: 0,
        runs_count: 0,
        capabilities: {
            can_edit: true,
            can_delete: true,
            can_duplicate: true,
            can_launch: true,
            can_archive: true,
        },
    };
}

function renderDrawer(props: Partial<React.ComponentProps<typeof NewTrainingRunDrawer>> = {}) {
    return render(
        <MemoryRouter>
            <NewTrainingRunDrawer
                open
                pipelineId="p1"
                specId="spec-1"
                preselectExperimentId="exp-1"
                initialDatasetId="ds-fresh"
                onClose={jest.fn()}
                {...props}
            />
        </MemoryRouter>,
    );
}

beforeEach(() => {
    api.getOpsSpec.mockResolvedValue({
        training: {
            requests: {
                run_labels: [["stage", "train-without-freeze"]],
                max_within_time: "1w",
            },
        },
    });
    api.getTrainingExperiments.mockResolvedValue({
        rows: [experimentRow("exp-1")],
        total: 1,
        summary: { total: 1, builtin: 0, custom: 1, editable: 1, locked: 0, archived: 0 },
    });
    api.getFrozenDatasets.mockResolvedValue({
        rows: [
            { dataset_id: "ds-fresh", frozen_at: "2026-07-20T12:00:00Z", train_count: 200, val_count: 150, test_count: 0 },
            { dataset_id: "ds-stale", frozen_at: "2026-06-01T12:00:00Z", train_count: 100, val_count: 50, test_count: 0 },
        ],
        total: 2,
    });
    api.getTrainingExperimentModels.mockResolvedValue({
        experiment: experimentRow("exp-1"),
        models: [],
        total: 0,
    });
    api.createTrainingRequest.mockResolvedValue({
        request: { id: "req-1", kind: "manual", state: "queued", train_config_id: "exp-1" },
        launch: { started: true, run_id: "run-1" },
    });
});

afterEach(() => jest.clearAllMocks());

test("parseWithinTimeMs understands week durations", () => {
    expect(parseWithinTimeMs("1w")).toBe(7 * 86_400_000);
    expect(isFrozenDatasetStale(
        { frozen_at: "2026-06-01T00:00:00Z" },
        "2026-07-20T00:00:00Z",
        "1w",
    )).toBe(true);
    expect(isFrozenDatasetStale(
        { frozen_at: "2026-07-18T00:00:00Z" },
        "2026-07-20T00:00:00Z",
        "1w",
    )).toBe(false);
});

test("launch checkbox is off by default and shows the spec stage label", async () => {
    renderDrawer();
    const checkbox = await screen.findByRole("checkbox", {
        name: /launch pipeline stage\s+train-without-freeze/i,
    });
    expect(checkbox).not.toBeChecked();
    expect(screen.getByRole("button", { name: /create request/i })).toBeInTheDocument();
});

test("hides stale datasets until bypass is checked", async () => {
    const user = userEvent.setup();
    renderDrawer();

    const bypass = await screen.findByRole("checkbox", {
        name: /bypass max_within_time/i,
    });
    expect(bypass).not.toBeChecked();
    expect(screen.getByText(/hiding 1 dataset/i)).toBeInTheDocument();

    await user.click(bypass);
    await waitFor(() => expect(screen.queryByText(/hiding 1 dataset/i)).not.toBeInTheDocument());
});

test("create request sends force=false by default", async () => {
    const user = userEvent.setup();
    renderDrawer();

    await waitFor(() => expect(api.getFrozenDatasets).toHaveBeenCalled());
    await user.click(await screen.findByRole("button", { name: /create request/i }));
    await waitFor(() => expect(api.createTrainingRequest).toHaveBeenCalled());
    expect(api.createTrainingRequest).toHaveBeenCalledWith("p1", "spec-1", {
        train_config_id: "exp-1",
        frozen_dataset_id: "ds-fresh",
        client_request_id: expect.any(String),
        launch: false,
        force: false,
    });
});

test("bypass sends force=true", async () => {
    const user = userEvent.setup();
    renderDrawer();

    await user.click(await screen.findByRole("checkbox", { name: /bypass max_within_time/i }));
    await user.click(screen.getByRole("button", { name: /create request/i }));
    await waitFor(() => expect(api.createTrainingRequest).toHaveBeenCalled());
    expect(api.createTrainingRequest.mock.calls[0][2].force).toBe(true);
});

test("reuses a single client_request_id across repeated submits (double-click)", async () => {
    const user = userEvent.setup();
    renderDrawer();

    await waitFor(() => expect(api.getFrozenDatasets).toHaveBeenCalled());

    const button = await screen.findByRole("button", { name: /create request/i });
    await user.click(button);
    await waitFor(() => expect(api.createTrainingRequest).toHaveBeenCalledTimes(1));
    await user.click(button);
    await waitFor(() => expect(api.createTrainingRequest).toHaveBeenCalledTimes(2));

    const firstId = api.createTrainingRequest.mock.calls[0][2].client_request_id;
    const secondId = api.createTrainingRequest.mock.calls[1][2].client_request_id;
    expect(firstId).toBeTruthy();
    expect(secondId).toBe(firstId);
    expect(api.createTrainingRequest.mock.calls[0][2].launch).toBe(false);
});

test("checking launch starts training with launch=true", async () => {
    const user = userEvent.setup();
    const onClose = jest.fn();
    renderDrawer({ onClose });

    const checkbox = await screen.findByRole("checkbox", {
        name: /launch pipeline stage\s+train-without-freeze/i,
    });
    await user.click(checkbox);
    await user.click(screen.getByRole("button", { name: /start training/i }));

    await waitFor(() => expect(api.createTrainingRequest).toHaveBeenCalled());
    expect(api.createTrainingRequest).toHaveBeenCalledWith("p1", "spec-1", {
        train_config_id: "exp-1",
        frozen_dataset_id: "ds-fresh",
        client_request_id: expect.any(String),
        launch: true,
        force: false,
    });
});

test("hides launch checkbox when run_labels are not configured", async () => {
    api.getOpsSpec.mockResolvedValue({
        training: { requests: { run_labels: [], max_within_time: "1w" } },
    });
    const user = userEvent.setup();
    renderDrawer();

    await screen.findByRole("button", { name: /create request/i });
    expect(screen.queryByRole("checkbox", { name: /launch pipeline stage/i })).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /create request/i }));
    await waitFor(() => expect(api.createTrainingRequest).toHaveBeenCalled());
    expect(api.createTrainingRequest).toHaveBeenCalledWith("p1", "spec-1", {
        train_config_id: "exp-1",
        frozen_dataset_id: "ds-fresh",
        client_request_id: expect.any(String),
        launch: false,
        force: false,
    });
});

test("warns when a model already exists for the experiment and dataset", async () => {
    api.getTrainingExperimentModels.mockResolvedValue({
        experiment: experimentRow("exp-1"),
        models: [
            {
                model_id: "model-already-there",
                frozen_dataset_id: "ds-fresh",
                created_at: "2026-07-01T00:00:00Z",
            },
            {
                model_id: "model-other-dataset",
                frozen_dataset_id: "ds-other",
            },
        ],
        total: 2,
    });
    renderDrawer();

    expect(await screen.findByText(/model already trained for this pair/i)).toBeInTheDocument();
    const link = screen.getByRole("link", { name: "model-already-there" });
    expect(link).toHaveAttribute("href", "/metrics/spec-1/models/model-already-there?dataset_id=ds-fresh");
    expect(screen.queryByRole("link", { name: "model-other-dataset" })).not.toBeInTheDocument();
    expect(screen.getByText(/will not train a new model/i)).toBeInTheDocument();
});
