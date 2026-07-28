import React from "react";
import { Alert } from "antd";
import { getApiErrorDescription, getApiErrorMessage } from "../api/http";

type Props = {
    error: unknown;
    style?: React.CSSProperties;
};

export function ApiErrorAlert({ error, style }: Props) {
    return (
        <Alert
            type="error"
            showIcon
            style={style}
            message={getApiErrorMessage(error)}
            description={getApiErrorDescription(error)}
        />
    );
}
