const path = require("path");

const uiRoot = path.resolve(__dirname, "../datapipe-ui");
const uiSrc = path.join(uiRoot, "src");
const mlSrc = path.join(__dirname, "src");

const workspaceAliases = (config) => {
    config.resolve.alias = {
        ...(config.resolve.alias || {}),
        "@datapipe/ui": uiSrc,
        "@datapipe/ui-ml": mlSrc,
    };

    const babelRule = config.module.rules.find((rule) => rule.oneOf)?.oneOf?.find(
        (rule) => rule.loader && String(rule.loader).includes("babel-loader"),
    );
    if (babelRule) {
        const include = Array.isArray(babelRule.include)
            ? babelRule.include
            : babelRule.include
              ? [babelRule.include]
              : [mlSrc];
        babelRule.include = [...include, uiSrc, mlSrc];
    }

    return config;
};

const removeModuleScope = (config) => {
    config.resolve.plugins = (config.resolve.plugins || []).filter(
        (plugin) => plugin.constructor.name !== "ModuleScopePlugin",
    );
    return config;
};

module.exports = {
    webpack: [workspaceAliases, removeModuleScope],
    jest: (config) => {
        config.moduleNameMapper = {
            ...(config.moduleNameMapper || {}),
            "^@datapipe/ui/(.*)$": `${uiSrc}/$1`,
            "^@datapipe/ui-ml/(.*)$": "<rootDir>/src/$1",
        };
        return config;
    },
};
