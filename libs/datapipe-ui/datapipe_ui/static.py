from __future__ import annotations

from importlib.resources import files


def resolve_frontend_dir() -> str:
    """Return the directory that contains the built SPA (index.html + static/)."""
    static_root = files("datapipe_ui") / "static"
    index = static_root / "index.html"
    if not index.is_file():
        raise FileNotFoundError(
            "datapipe-ui static assets are missing. "
            "Build the frontend (`yarn build` in libs/datapipe-ui) and copy the output "
            "to datapipe_ui/static before packaging or installing."
        )
    return str(static_root)
