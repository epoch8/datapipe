import { opsApi } from "@datapipe/ui-ml/api/client";

/**
 * Resolve which ops spec exposes custom training experiments for a pipeline
 * (spec §35). A single {@link DatapipeOpsSpec} can carry both a frozen-dataset
 * and a training config, so we first check the current spec, then fall back to
 * scanning the pipeline's ops specs for training-capable ones and prefer the
 * first that actually has an experiments registry configured.
 *
 * Returns ``null`` when the pipeline has no training-capable spec.
 */
export async function resolveTrainingSpecId(
    pipelineId: string,
    currentSpecId?: string,
): Promise<string | null> {
    const candidates: string[] = [];
    if (currentSpecId) candidates.push(currentSpecId);

    let trainingSpecIds: string[] = [];
    try {
        const specs = await opsApi.getOpsSpecs(pipelineId);
        trainingSpecIds = specs.specs.filter((s) => s.has_training).map((s) => s.id);
    } catch {
        trainingSpecIds = [];
    }
    for (const id of trainingSpecIds) {
        if (!candidates.includes(id)) candidates.push(id);
    }

    // Prefer a spec that actually has an experiments registry configured.
    for (const id of candidates) {
        try {
            const detail = await opsApi.getOpsSpec(pipelineId, id);
            if (detail.training?.experiments) return id;
        } catch {
            // Ignore and continue scanning.
        }
    }

    // Fall back to the first training-capable spec (may only have built-ins).
    if (trainingSpecIds.length) return trainingSpecIds[0];
    return currentSpecId ?? null;
}
