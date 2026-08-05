import React from "react";
import { Navigate, useSearchParams } from "react-router-dom";

/** Redirect legacy compare URL to unified training page with selected runs. */
export function TrainingCompare() {
    const [params] = useSearchParams();
    const runKeys = params.get("run_keys") ?? "";
    return <Navigate to={`/training?run_keys=${encodeURIComponent(runKeys)}`} replace />;
}
