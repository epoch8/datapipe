import React from "react";
import ReactDOM from "react-dom/client";
import "./pluginBootstrap";
import "@datapipe/ui/index.css";
import App from "@datapipe/ui/App";

const root = ReactDOM.createRoot(
    document.getElementById("root") as HTMLElement,
);

root.render(
    <React.Fragment>
        <App />
    </React.Fragment>,
);
