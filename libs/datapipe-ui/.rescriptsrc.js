const path = require("path");

const uiSrc = path.join(__dirname, "src");

const workspaceAliases = (config) => {
    config.resolve.alias = {
        ...(config.resolve.alias || {}),
        "@datapipe/ui": uiSrc,
    };
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
        };
        return config;
    },
};
