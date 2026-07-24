import React from "react";
import { Alert } from "antd";
import { useOnlineStatus } from "../hooks/useOnlineStatus";

export function ConnectivityBanner() {
    const online = useOnlineStatus();

    if (online) {
        return null;
    }

    return (
        <Alert
            type="warning"
            showIcon
            banner
            message="You are offline"
            description="Datapipe Ops cannot reach the API until your network connection is restored."
            style={{ borderRadius: 0 }}
        />
    );
}
