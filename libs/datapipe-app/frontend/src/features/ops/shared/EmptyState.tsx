import React from "react";
import { Alert, Empty, Spin } from "antd";

type Props = {
    loading?: boolean;
    error?: string | null;
    empty?: boolean;
    emptyMessage?: string;
    // When true, keep children mounted during loading and overlay a subtle
    // spinner instead of blanking the whole area (avoids the full white flash on
    // every filter/sort change once data already exists).
    keepChildrenWhileLoading?: boolean;
    children: React.ReactNode;
};

export function EmptyState({
    loading,
    error,
    empty,
    emptyMessage = "No data available",
    keepChildrenWhileLoading,
    children,
}: Props) {
    if (loading && keepChildrenWhileLoading) {
        return (
            <div className="ops-loading-wrap">
                <div className="ops-loading-overlay">
                    <Spin />
                </div>
                {children}
            </div>
        );
    }
    if (loading) {
        return (
            <div className="ops-empty-state">
                <Spin size="large" />
            </div>
        );
    }
    if (error) {
        return (
            <div className="ops-empty-state">
                <Alert type="error" message={error} showIcon />
            </div>
        );
    }
    if (empty) {
        return (
            <div className="ops-empty-state">
                <Empty description={emptyMessage} />
            </div>
        );
    }
    return <>{children}</>;
}
