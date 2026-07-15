import React, { useState } from "react";
import { Alert, AlertProps } from "antd";
import { Table } from "../../table";
import type { PipeTable } from "../../../types";

type Props = {
    table: PipeTable;
    knownRowCount?: number | null;
    hideRunStep?: boolean;
    pipelineId?: string;
    initialColumnFilter?: { column: string; value: string };
};

export function TableDataPanel({
    table,
    knownRowCount = null,
    hideRunStep = false,
    pipelineId,
    initialColumnFilter,
}: Props) {
    const [alertMsg, setAlertMsg] = useState<AlertProps | null>(null);

    return (
        <div>
            {alertMsg && (
                <Alert
                    style={{ marginBottom: 12 }}
                    type={alertMsg.type}
                    message={alertMsg.message}
                    closable
                    onClose={() => setAlertMsg(null)}
                />
            )}
            <Table
                current={table}
                setAlertMsg={setAlertMsg}
                knownRowCount={knownRowCount}
                hideRunStep={hideRunStep}
                pipelineId={pipelineId}
                initialColumnFilter={initialColumnFilter}
            />
        </div>
    );
}
