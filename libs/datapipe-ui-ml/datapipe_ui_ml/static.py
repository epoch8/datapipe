from __future__ import annotations

from importlib.resources import files


def resolve_frontend_dir() -> str:
    """Return the directory that contains the built SPA (index.html + static/)."""
    static_root = files("datapipe_ui_ml") / "static"
    index = static_root / "index.html"
    if not index.is_file():
        raise FileNotFoundError(
            "datapipe-ui-ml static assets are missing. "
            "Build the frontend (`yarn build` in libs/datapipe-ui-ml) and copy the output "
            "to datapipe_ui_ml/static before packaging or installing."
        )
    return str(static_root)
