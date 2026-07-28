import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import "@testing-library/jest-dom";

jest.mock("@datapipe/ui-ml/api/client", () => ({
    opsApi: {
        getTrainConfigSchema: jest.fn(),
        createTrainingExperiment: jest.fn(),
        updateTrainingExperiment: jest.fn(),
    },
}));

import { opsApi } from "@datapipe/ui-ml/api/client";
import { TrainingExperimentDrawer } from "./TrainingExperimentDrawer";
import type { TrainingExperimentRow } from "../../../types/opsMl";

const api = opsApi as unknown as {
    getTrainConfigSchema: jest.Mock;
    createTrainingExperiment: jest.Mock;
    updateTrainingExperiment: jest.Mock;
};

function makeExperiment(overrides: Partial<TrainingExperimentRow> = {}): TrainingExperimentRow {
    return {
        id: "exp-1",
        source: "custom",
        display_name: "My experiment",
        description: "desc",
        config_type: "yolov8_detection",
        params: { epochs: 10 },
        active: true,
        revision: 3,
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
        ...overrides,
    };
}

beforeEach(() => {
    api.getTrainConfigSchema.mockResolvedValue({
        config_type: "yolov8_detection",
        schema: {
            type: "object",
            properties: {
                model: { type: "string", default: "yolov8n.pt", ui_group: "model", ui_order: 10 },
                epochs: { type: "integer", default: 300, ui_group: "model", ui_order: 20 },
                batch: { type: "integer", default: 16, ui_group: "model", ui_order: 30 },
            },
        },
    });
    api.createTrainingExperiment.mockImplementation((_p, _s, payload) =>
        Promise.resolve(makeExperiment({ id: "new-exp", ...payload })),
    );
    api.updateTrainingExperiment.mockImplementation((_p, _s, id) =>
        Promise.resolve(makeExperiment({ id })),
    );
});

afterEach(() => jest.clearAllMocks());

test("create mode calls createTrainingExperiment with the form values", async () => {
    const user = userEvent.setup();
    const onSaved = jest.fn();
    render(
        <TrainingExperimentDrawer
            open
            mode="create"
            pipelineId="p1"
            specId="spec-1"
            experiment={null}
            onClose={jest.fn()}
            onSaved={onSaved}
        />,
    );

    const nameInput = await screen.findByLabelText("Display name");
    await user.type(nameInput, "Fresh run");
    await user.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(api.createTrainingExperiment).toHaveBeenCalled());
    const payload = api.createTrainingExperiment.mock.calls[0][2];
    expect(payload.display_name).toBe("Fresh run");
    expect(payload.params).toMatchObject({
        model: "yolov8n.pt",
        epochs: 300,
        batch: 16,
    });
    expect(onSaved).toHaveBeenCalled();
});

test("create mode prefills schema defaults in the form fields", async () => {
    render(
        <TrainingExperimentDrawer
            open
            mode="create"
            pipelineId="p1"
            specId="spec-1"
            experiment={null}
            onClose={jest.fn()}
            onSaved={jest.fn()}
        />,
    );

    await waitFor(() => {
        expect(screen.getByLabelText("model")).toHaveValue("yolov8n.pt");
    });
    expect(screen.getByLabelText("epochs")).toHaveValue("300");
    expect(screen.getByLabelText("batch")).toHaveValue("16");
});

test("edit mode sends expected_revision for optimistic concurrency", async () => {
    const user = userEvent.setup();
    render(
        <TrainingExperimentDrawer
            open
            mode="edit"
            pipelineId="p1"
            specId="spec-1"
            experiment={makeExperiment({ revision: 7 })}
            onClose={jest.fn()}
            onSaved={jest.fn()}
        />,
    );

    await screen.findByLabelText("Display name");
    await user.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(api.updateTrainingExperiment).toHaveBeenCalled());
    const payload = api.updateTrainingExperiment.mock.calls[0][3];
    expect(payload.expected_revision).toBe(7);
});

test("view / locked experiment is read-only (no Save button, disabled fields)", async () => {
    render(
        <TrainingExperimentDrawer
            open
            mode="view"
            pipelineId="p1"
            specId="spec-1"
            experiment={makeExperiment({
                capabilities: {
                    can_edit: false,
                    can_delete: false,
                    can_duplicate: true,
                    can_launch: true,
                    can_archive: true,
                    lock_reason: "locked",
                },
            })}
            onClose={jest.fn()}
            onSaved={jest.fn()}
        />,
    );

    const nameInput = await screen.findByLabelText("Display name");
    expect(nameInput).toBeDisabled();
    expect(screen.queryByRole("button", { name: "Save" })).not.toBeInTheDocument();
});
