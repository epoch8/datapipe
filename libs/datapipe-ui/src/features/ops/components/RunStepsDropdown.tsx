import React from "react";
import { Button, Dropdown, Menu } from "antd";

type StageRef = { stage: string };

type RunStepsDropdownProps = {
    stages: StageRef[];
    onStart: (labels: [string, string][]) => void;
    disabled?: boolean;
    primary?: boolean;
};

export function RunStepsDropdown({
    stages,
    onStart,
    disabled,
    primary = true,
}: RunStepsDropdownProps) {
    const menu = (
        <Menu>
            <Menu.Item key="__all__" onClick={() => onStart([])}>
                All labels
            </Menu.Item>
            <Menu.Divider />
            {stages.map((s) => (
                <Menu.Item key={s.stage} onClick={() => onStart([["stage", s.stage]])}>
                    {s.stage}
                </Menu.Item>
            ))}
        </Menu>
    );

    return (
        <Dropdown overlay={menu} disabled={disabled}>
            <Button type={primary ? "primary" : "default"}>Run steps</Button>
        </Dropdown>
    );
}
