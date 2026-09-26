import { useEffect, useState } from "react";
import { isBrowserOffline } from "../api/http";

export function useOnlineStatus(): boolean {
    const [online, setOnline] = useState(() => !isBrowserOffline());

    useEffect(() => {
        const onOnline = () => setOnline(true);
        const onOffline = () => setOnline(false);
        window.addEventListener("online", onOnline);
        window.addEventListener("offline", onOffline);
        return () => {
            window.removeEventListener("online", onOnline);
            window.removeEventListener("offline", onOffline);
        };
    }, []);

    return online;
}
