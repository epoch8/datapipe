import React from "react";

export type LocalChromeActions = {
    onRefreshPage: () => void;
    onRunSteps: () => void;
    canStart: boolean;
    starting: boolean;
};

const LocalChromeActionsContext = React.createContext<LocalChromeActions | null>(null);

export function LocalChromeActionsProvider({
    value,
    children,
}: {
    value: LocalChromeActions;
    children: React.ReactNode;
}) {
    return (
        <LocalChromeActionsContext.Provider value={value}>
            {children}
        </LocalChromeActionsContext.Provider>
    );
}

export function useLocalChromeActions(): LocalChromeActions | null {
    return React.useContext(LocalChromeActionsContext);
}
