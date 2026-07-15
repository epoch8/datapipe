from pathlib import Path

import pytest


def test_resolve_frontend_dir_requires_built_assets(monkeypatch, tmp_path: Path):
    import datapipe_ui.static as static_mod

    class _Files:
        def __truediv__(self, other: str):
            return tmp_path / other

    monkeypatch.setattr(static_mod, "files", lambda _pkg: _Files())

    with pytest.raises(FileNotFoundError, match="static assets are missing"):
        static_mod.resolve_frontend_dir()


def test_resolve_frontend_dir_with_built_assets():
    from datapipe_ui.static import resolve_frontend_dir

    static_dir = Path(resolve_frontend_dir())
    assert (static_dir / "index.html").is_file()
