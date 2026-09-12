import React from "react";
import { Spin } from "antd";
import { useNavigate, useSearchParams } from "react-router-dom";
import { opsApi, getRefreshIntervalMs } from "../../api/client";
import { ApiErrorAlert } from "../../components/ApiErrorAlert";
import type { PipelineDetail } from "../../types/ops";
import { PageHeader } from "./shared";
import { PipelineOverviewGraphCard } from "./pipeline/PipelineOverviewGraphCard";

/**
 * General / Overview page: label graph for the single cloud pipeline.
 * Runs / health chrome from full Ops builds is omitted.
 */
export function Overview() {
    const navigate = useNavigate();
    const [searchParams] = useSearchParams();
    const [detail, setDetail] = React.useState<PipelineDetail | null>(null);
    const [error, setError] = React.useState<unknown>(null);
    const labelKeyParam = searchParams.get("label_key");

    const load = React.useCallback(() => {
        opsApi
            .getPipeline(labelKeyParam ? { label_key: labelKeyParam } : undefined)
            .then(setDetail)
            .catch((e) => setError(e));
    }, [labelKeyParam]);

    React.useEffect(() => {
        load();
        const timer = setInterval(load, getRefreshIntervalMs());
        return () => clearInterval(timer);
    }, [load]);

    if (error) return <ApiErrorAlert error={error} />;
    if (!detail) return <Spin />;

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[{ label: "General" }]}
                title="General"
                onRefresh={load}
            />

            <PipelineOverviewGraphCard
                detail={detail}
                onLabelSelect={(label, labelKey) => {
                    const params = new URLSearchParams();
                    if (labelKey && labelKey !== "stage") params.set("label_key", labelKey);
                    params.set("stage", label);
                    navigate(`/graph?${params.toString()}`);
                }}
            />
        </div>
    );
}
