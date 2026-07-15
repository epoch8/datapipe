const path = require("path");

const includeMl = process.env.REACT_APP_INCLUDE_ML !== "false";
const uiRoot = __dirname;
const uiSrc = path.join(uiRoot, "src");
const mlSrc = path.resolve(uiRoot, "../datapipe-ui-ml/src");

const workspaceAliases = (config) => {
    config.resolve.alias = {
        ...(config.resolve.alias || {}),
        "@datapipe/ui": uiSrc,
        "@datapipe/ui-ml": includeMl ? mlSrc : path.join(uiSrc, "stubs/ui-ml"),
    };

    const babelRule = config.module.rules.find((rule) => rule.oneOf)?.oneOf?.find(
        (rule) => rule.loader && String(rule.loader).includes("babel-loader"),
    );
    if (babelRule) {
        const include = Array.isArray(babelRule.include)
            ? babelRule.include
            : babelRule.include
              ? [babelRule.include]
              : [uiSrc];
        if (includeMl) {
            babelRule.include = [...include, mlSrc];
        }
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
            "^@datapipe/ui/(.*)$": "<rootDir>/src/$1",
            "^@datapipe/ui-ml/(.*)$": "<rootDir>/../datapipe-ui-ml/src/$1",
        };
        return config;
    },
};
