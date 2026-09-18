import React from "react";
import { useParams } from "react-router-dom";
import { opsApi } from "../api/client";
import { useOptionalDatapipeUiConfig } from "../context/DatapipeUiContext";

/**
 * Resolve the active pipeline / agent id.
 *
 * Priority: route ``:id`` → cloud mount ``pipelineId`` / ``agentId`` → ``/capabilities``.
 * CloudShell routes are ``/runs`` without ``:id``, so mount config must supply the id.
 */
export function usePipelineId(): { pipelineId: string; loading: boolean } {
    const { id } = useParams<{ id?: string }>();
    const ui = useOptionalDatapipeUiConfig();
    const fromMount =
        ui?.pipelineId ||
        (typeof window !== "undefined"
            ? (window as unknown as { __DP_PIPELINE__?: { agentId?: string } }).__DP_PIPELINE__
                  ?.agentId
            : undefined) ||
        "";
    const [pipelineId, setPipelineId] = React.useState(id ?? fromMount);
    const [loading, setLoading] = React.useState(!id && !fromMount);

    React.useEffect(() => {
        if (id) {
            setPipelineId(id);
            setLoading(false);
            return;
        }
        if (fromMount) {
            setPipelineId(fromMount);
            setLoading(false);
            return;
        }
        opsApi
            .getCapabilities()
            .then((c) => {
                setPipelineId(c.pipeline_id ?? "");
                setLoading(false);
            })
            .catch(() => setLoading(false));
    }, [id, fromMount]);

    return { pipelineId, loading };
}
