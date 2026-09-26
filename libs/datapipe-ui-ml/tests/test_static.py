from pathlib import Path

import pytest


def test_static_entry_point_with_built_spa():
    pytest.importorskip("datapipe_ui_ml")
    from datapipe_ui_ml.static import resolve_frontend_dir

    static_dir = resolve_frontend_dir()
    assert Path(static_dir, "index.html").is_file()
