from pathlib import Path

import pytest


def test_frontend_static_entry_point_without_ui_package(monkeypatch):
    from datapipe_app import frontend_static

    monkeypatch.setattr(frontend_static, "entry_points", lambda **kwargs: [])
    assert frontend_static.resolve_frontend_dir() is None


def test_frontend_static_entry_point_with_built_ui():
    pytest.importorskip("datapipe_ui")
    from datapipe_app.frontend_static import resolve_frontend_dir

    static_dir = resolve_frontend_dir()
    if static_dir is None:
        pytest.skip("datapipe-ui entry point not registered or static assets missing")
    assert Path(static_dir, "index.html").is_file()
