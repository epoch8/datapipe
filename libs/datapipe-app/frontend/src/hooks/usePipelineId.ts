import React from "react";
import { useParams } from "react-router-dom";
import { opsApi } from "../api/ops";

export function usePipelineId(): { pipelineId: string; loading: boolean } {
    const { id } = useParams<{ id?: string }>();
    const [pipelineId, setPipelineId] = React.useState(id ?? "");
    const [loading, setLoading] = React.useState(!id);

    React.useEffect(() => {
        if (id) {
            setPipelineId(id);
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
    }, [id]);

    return { pipelineId, loading };
}
