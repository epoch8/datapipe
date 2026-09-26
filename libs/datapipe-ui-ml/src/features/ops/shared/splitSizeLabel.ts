export function splitSizeLabel(row: { split_counts?: { train?: number; val?: number; test?: number } }) {
    const s = row.split_counts ?? {};
    return `${s.train ?? 0} / ${s.val ?? 0} / ${s.test ?? 0}`;
}
