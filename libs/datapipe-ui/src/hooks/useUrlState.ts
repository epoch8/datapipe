import { useSearchParams } from "react-router-dom";

export function useUrlState(key: string, defaultValue = ""): [string, (v: string) => void] {
    const [params, setParams] = useSearchParams();
    const value = params.get(key) ?? defaultValue;
    const setValue = (v: string) => {
        const next = new URLSearchParams(params);
        if (v) next.set(key, v);
        else next.delete(key);
        setParams(next, { replace: true });
    };
    return [value, setValue];
}

export function useUrlNumber(key: string, defaultValue: number): [number, (v: number) => void] {
    const [params, setParams] = useSearchParams();
    const raw = params.get(key);
    const value = raw ? parseInt(raw, 10) : defaultValue;
    const setValue = (v: number) => {
        const next = new URLSearchParams(params);
        next.set(key, String(v));
        setParams(next, { replace: true });
    };
    return [Number.isFinite(value) ? value : defaultValue, setValue];
}
