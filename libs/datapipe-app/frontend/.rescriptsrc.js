const glob = require("glob-all");
const PurgecssPlugin = require("purgecss-webpack-plugin");

const disableCssExtract = (config) => {
    delete config["MiniCssExtractPlugin"];
    const plugins = [];
    config.module.rules.forEach((rule) => {
        if (rule.oneOf) {
            rule.oneOf.forEach((rule) => {
                if (String(rule.test) === String(/\.css$/)) {
                    rule.use.splice(0, 1);
                }
            });
        }
    });

    config.plugins.forEach((pl) => {
        if (pl.constructor.name === "MiniCssExtractPlugin") return;
        plugins.push(pl);
    });
    config.plugins = plugins;
    return config;
};

const purgeCss = (config) => {
    let manifestIndex = 0;
    config.plugins.forEach((pl, index) => {
        if (pl.constructor.name === "WebpackManifestPlugin") {
            manifestIndex = index;
        }
    });

    config.plugins.splice(
        manifestIndex,
        0,
        new PurgecssPlugin({
            paths: [
                "public/index.html",
                ...glob.sync(`src/**/*`, { nodir: true }),
            ],
        }),
    );

    console.log(config.plugins);
    return config;
};

module.exports = [disableCssExtract, purgeCss];
