import React, { useState } from "react";
import { Alert, AlertProps } from "antd";
import { Table } from "../../table";
import type { PipeTable } from "../../../types";

type Props = {
    table: PipeTable;
};

export function TableDataPanel({ table }: Props) {
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
            <Table current={table} setAlertMsg={setAlertMsg} />
        </div>
    );
}
