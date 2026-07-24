#!/usr/bin/env node
/**
 * Production build smoke check for the ML Ops SPA host.
 */
import { execSync } from "child_process";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const mlRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = "build-verify";

function runBuild() {
    execSync("yarn build", {
        cwd: mlRoot,
        env: {
            ...process.env,
            CI: "true",
            BUILD_PATH: outDir,
        },
        stdio: "inherit",
    });
}

function readMainBundle() {
    const jsDir = path.join(mlRoot, outDir, "static/js");
    const mainFile = fs
        .readdirSync(jsDir)
        .find((f) => f.startsWith("main.") && f.endsWith(".js") && !f.endsWith(".map"));
    if (!mainFile) {
        throw new Error(`no main.*.js in ${jsDir}`);
    }
    return fs.readFileSync(path.join(jsDir, mainFile), "utf8");
}

function assertBundle(bundle) {
    if (!bundle.includes("Pipeline Metrics Overview")) {
        throw new Error("ML bundle must contain plugin metrics pages");
    }
    if (!bundle.includes("metrics/frozen-datasets")) {
        throw new Error("ML bundle must contain plugin metrics API paths");
    }
}

try {
    fs.rmSync(path.join(mlRoot, outDir), { recursive: true, force: true });
    runBuild();
    assertBundle(readMainBundle());
    process.stdout.write("[build-verify] ML SPA: OK\n");
} catch (err) {
    process.stderr.write(`[build-verify] ML SPA: FAIL — ${err.message}\n`);
    process.exit(1);
} finally {
    fs.rmSync(path.join(mlRoot, outDir), { recursive: true, force: true });
}
