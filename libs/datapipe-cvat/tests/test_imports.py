def test_cvat_modules_import() -> None:
    import datapipe_cvat.cvat_step
    import datapipe_cvat.utils

    assert datapipe_cvat.cvat_step is not None
    assert datapipe_cvat.utils is not None
