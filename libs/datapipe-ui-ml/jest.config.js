const path = require("path");

const monorepoRoot = path.resolve(__dirname, "../..");
const uiSrc = path.resolve(__dirname, "../datapipe-ui/src");
const babelTransform = path.join(monorepoRoot, "node_modules/react-scripts/config/jest/babelTransform.js");

module.exports = {
    rootDir: path.join(__dirname, "src"),
    testMatch: ["**/*.test.ts", "**/*.test.tsx"],
    testEnvironment: "jsdom",
    setupFiles: ["<rootDir>/testSetup.ts"],
    moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json"],
    moduleNameMapper: {
        "^@datapipe/ui-ml/(.*)$": "<rootDir>/$1",
        "^@datapipe/ui/(.*)$": `${uiSrc}/$1`,
        "\\.(css|less|scss|sass)$": "identity-obj-proxy",
    },
    transform: {
        "^.+\\.(ts|tsx|js|jsx)$": babelTransform,
    },
    transformIgnorePatterns: [
        "[/\\\\]node_modules[/\\\\].+\\.(js|jsx|mjs|cjs|ts|tsx)$",
        "^.+\\.module\\.(css|sass|scss)$",
    ],
};
