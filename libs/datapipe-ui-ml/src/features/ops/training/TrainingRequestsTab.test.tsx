import React from "react";
import { MemoryRouter } from "react-router-dom";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Modal } from "antd";
import "@testing-library/jest-dom";

jest.mock("@datapipe/ui-ml/api/client", () => ({
    opsApi: {
        getTrainingRequests: jest.fn(),
        launchTrainingRequest: jest.fn(),
        deleteTrainingRequest: jest.fn(),
    },
}));

import { opsApi } from "@datapipe/ui-ml/api/client";
import { TrainingRequestsTab } from "./TrainingRequestsTab";

const api = opsApi as unknown as {
    getTrainingRequests: jest.Mock;
    launchTrainingRequest: jest.Mock;
    deleteTrainingRequest: jest.Mock;
};

beforeEach(() => {
    api.getTrainingRequests.mockResolvedValue({
        rows: [
            {
                id: "req-idle",
                kind: "manual",
                state: "queued",
                train_config_id: "cfg-1",
                config_name: "Baseline",
                frozen_dataset_id: "ds-1",
                can_delete: true,
                can_launch: true,
            },
            {
                id: "req-done",
                kind: "manual",
                state: "completed",
                train_config_id: "cfg-1",
                config_name: "Baseline",
                frozen_dataset_id: "ds-1",
                run_key: "r1",
                model_id: "m1",
                status: "completed",
                can_delete: false,
                can_launch: false,
            },
        ],
        total: 2,
    });
    api.launchTrainingRequest.mockResolvedValue({
        training_request_id: "req-idle",
        started: true,
        run_id: "run-9",
    });
    jest.spyOn(Modal, "confirm").mockImplementation((config: any) => {
        config.onOk?.();
        return { destroy: jest.fn(), update: jest.fn() } as any;
    });
});

afterEach(() => {
    jest.clearAllMocks();
    jest.restoreAllMocks();
});

function renderTab() {
    return render(
        <MemoryRouter>
            <TrainingRequestsTab pipelineId="p1" specId="spec-1" />
        </MemoryRouter>,
    );
}

test("shows Run request next to Delete for idle launchable rows only", async () => {
    renderTab();
    await waitFor(() => expect(api.getTrainingRequests).toHaveBeenCalled());

    expect(await screen.findByText("req-idle")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Run request" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument();

    // Completed row has neither action.
    expect(screen.getAllByRole("button", { name: "Run request" })).toHaveLength(1);
    expect(screen.getAllByRole("button", { name: "Delete" })).toHaveLength(1);
});

test("Run request calls launchTrainingRequest", async () => {
    const user = userEvent.setup();
    renderTab();
    await screen.findByText("req-idle");

    await user.click(screen.getByRole("button", { name: "Run request" }));

    await waitFor(() =>
        expect(api.launchTrainingRequest).toHaveBeenCalledWith("p1", "spec-1", "req-idle"),
    );
});

test("hides Run request when can_launch is false", async () => {
    api.getTrainingRequests.mockResolvedValue({
        rows: [
            {
                id: "req-no-labels",
                kind: "manual",
                state: "queued",
                train_config_id: "cfg-1",
                can_delete: true,
                can_launch: false,
            },
        ],
        total: 1,
    });
    renderTab();
    await screen.findByText("req-no-labels");
    expect(screen.queryByRole("button", { name: "Run request" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument();
});
