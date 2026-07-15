/** Operator Light — semantic graph palette (mirrors CSS variables in operatorLight.css). */
export const graphColors = {
    canvas: { bg: "#FBFDFF", dot: "#DCE7F5" },
    table: {
        bg: "#FFF3E5",
        bg2: "#FFE5C7",
        border: "#FF7A1A",
        icon: "#FF6B00",
        text: "#3E2407",
    },
    transform: {
        bg: "#EFFFF3",
        bg2: "#DCF8E5",
        border: "#16A34A",
        icon: "#0FA83D",
        text: "#083B18",
    },
    group: {
        bg: "#EDF5FF",
        bg2: "#DBEAFE",
        border: "#1677FF",
        icon: "#1677FF",
        text: "#074899",
        expandedBg: "rgba(237, 245, 255, 0.48)",
        expandedBorder: "rgba(22, 119, 255, 0.72)",
    },
} as const;

export const edgeColors = {
    default: "#52677F",
    active: "#1677FF",
    error: "#D92D20",
} as const;
