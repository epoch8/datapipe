import React from "react";
import { Tabs } from "antd";
import { useParams, useSearchParams } from "react-router-dom";
import { opsApi } from "@datapipe/ui-ml/api/client";
import { usePipelineId } from "@datapipe/ui/hooks/usePipelineId";
import type { OpsSpecDetail } from "../../../types/opsSpecs";
import { EmptyState, PageHeader } from "../shared";
import { TrainingRequestsTab } from "./TrainingRequestsTab";
import { TrainingExperimentsTab } from "./TrainingExperimentsTab";
import { NewTrainingRunDrawer } from "./NewTrainingRunDrawer";
import "./trainingExperiments.css";

type TabKey = "requests" | "experiments";

function normalizeTab(value: string | null): TabKey {
    // Accept legacy ?tab=runs as an alias for requests.
    if (value === "requests" || value === "runs") return "requests";
    return "experiments";
}

export function TrainingSpecPage() {
    const { specId = "" } = useParams();
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [searchParams, setSearchParams] = useSearchParams();

    const [spec, setSpec] = React.useState<OpsSpecDetail | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<unknown>(null);
    const [refreshToken, setRefreshToken] = React.useState(0);

    const tab = normalizeTab(searchParams.get("tab"));
    const drawerOpen = searchParams.get("drawer") === "run";
    const preselectExperimentId = searchParams.get("experiment") ?? undefined;

    const patchParams = React.useCallback(
        (patch: Record<string, string | undefined>) => {
            setSearchParams(
                (prev) => {
                    const next = new URLSearchParams(prev);
                    Object.entries(patch).forEach(([key, value]) => {
                        if (value) next.set(key, value);
                        else next.delete(key);
                    });
                    return next;
                },
                { replace: true },
            );
        },
        [setSearchParams],
    );

    const load = React.useCallback(() => {
        if (!pipelineId || !specId) return;
        setLoading(true);
        setError(null);
        opsApi
            .getOpsSpec(pipelineId, specId)
            .then(setSpec)
            .catch((e) => setError(e))
            .finally(() => setLoading(false));
    }, [pipelineId, specId]);

    React.useEffect(() => {
        load();
    }, [load]);

    return (
        <div className="ops-page ops-spec-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Datapipe Ops", href: "/" },
                    { label: "Training", href: "/training" },
                    { label: spec?.title ?? specId },
                ]}
                title={`${spec?.title ?? specId} - Training`}
                subtitle="Training requests and custom experiments for this specification"
                statusChips={[{ label: spec?.title ?? specId, variant: "purple" }]}
                onRefresh={load}
                primaryAction={{
                    label: "New Training Run",
                    onClick: () => patchParams({ drawer: "run", experiment: undefined }),
                }}
            />

            <EmptyState loading={pidLoading || loading} error={error} empty={!spec && !loading}>
                {spec && pipelineId ? (
                    <>
                        <Tabs activeKey={tab} onChange={(key) => patchParams({ tab: key })}>
                            <Tabs.TabPane tab="Experiments" key="experiments">
                                <TrainingExperimentsTab
                                    pipelineId={pipelineId}
                                    specId={specId}
                                    refreshToken={refreshToken}
                                />
                            </Tabs.TabPane>
                            <Tabs.TabPane tab="Requests" key="requests">
                                <TrainingRequestsTab
                                    pipelineId={pipelineId}
                                    specId={specId}
                                    refreshToken={refreshToken}
                                />
                            </Tabs.TabPane>
                        </Tabs>

                        <NewTrainingRunDrawer
                            open={drawerOpen}
                            pipelineId={pipelineId}
                            specId={specId}
                            preselectExperimentId={preselectExperimentId}
                            onClose={() => patchParams({ drawer: undefined })}
                            onLaunched={() => {
                                patchParams({ drawer: undefined });
                                setRefreshToken((token) => token + 1);
                            }}
                        />
                    </>
                ) : null}
            </EmptyState>
        </div>
    );
}
