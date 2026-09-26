export function formatFrozenAt(iso?: string): string {
    if (!iso) return "—";
    const normalized = iso.replace("Z", "").replace(/\.\d+$/, "");
    const [date, time] = normalized.split("T");
    if (!time) return `${date} 00:00:00`;
    const [h = "00", m = "00", s = "00"] = time.split(":");
    return `${date} ${h.padStart(2, "0")}:${m.padStart(2, "0")}:${s.padStart(2, "0")}`;
}
