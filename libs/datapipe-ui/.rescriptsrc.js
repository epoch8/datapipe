const path = require("path");

const uiSrc = path.join(__dirname, "src");
const uiCoreSrc = path.join(__dirname, "..", "..", "packages", "ui-core", "src");
const apiClientSrc = path.join(__dirname, "..", "..", "packages", "api-client", "src");
const workspaceSrcDirs = [uiCoreSrc, apiClientSrc];

const workspaceAliases = (config) => {
    config.resolve.alias = {
        ...(config.resolve.alias || {}),
        "@datapipe/ui": uiSrc,
        // Directory aliases so `@datapipe/ui-core/tokens.css` resolves correctly.
        "@datapipe/ui-core": uiCoreSrc,
        "@datapipe/api-client": apiClientSrc,
    };
    return config;
};

const removeModuleScope = (config) => {
    config.resolve.plugins = (config.resolve.plugins || []).filter(
        (plugin) => plugin.constructor.name !== "ModuleScopePlugin",
    );
    return config;
};

/** CRA only babel-transpiles app `src/`; workspace TS packages need the same. */
const transpileWorkspacePackages = (config) => {
    const rules = config.module.rules || [];
    for (const rule of rules) {
        const usesSourceMap =
            rule.enforce === "pre" &&
            ((typeof rule.loader === "string" && rule.loader.includes("source-map-loader")) ||
                (Array.isArray(rule.use) &&
                    rule.use.some(
                        (u) =>
                            (typeof u === "string" && u.includes("source-map-loader")) ||
                            (u && typeof u.loader === "string" && u.loader.includes("source-map-loader")),
                    )));
        if (usesSourceMap) {
            rule.exclude = [...(Array.isArray(rule.exclude) ? rule.exclude : rule.exclude ? [rule.exclude] : []), ...workspaceSrcDirs];
        }
        if (!rule.oneOf) continue;
        for (const one of rule.oneOf) {
            const loader = one.loader || (one.use && one.use.loader);
            if (typeof loader === "string" && loader.includes("babel-loader") && one.include) {
                one.include = Array.isArray(one.include)
                    ? [...one.include, ...workspaceSrcDirs]
                    : [one.include, ...workspaceSrcDirs];
            }
        }
    }
    return config;
};

module.exports = {
    webpack: [workspaceAliases, removeModuleScope, transpileWorkspacePackages],
    jest: (config) => {
        config.moduleNameMapper = {
            ...(config.moduleNameMapper || {}),
            "^@datapipe/ui/(.*)$": "<rootDir>/src/$1",
            "^@datapipe/ui-core$": "<rootDir>/../../packages/ui-core/src/index.ts",
            "^@datapipe/ui-core/tokens\\.css$": "<rootDir>/../../packages/ui-core/src/tokens.css",
            "^@datapipe/api-client$": "<rootDir>/../../packages/api-client/src/index.ts",
        };
        return config;
    },
};
