from pathlib import Path

from datapipe_ml.training.staging import (
    copied_file_to_local_temp,
    copy_file_to_local_staging,
    make_local_staging_dir,
)


def test_local_staging_dir_cleans_non_managed_directory(tmp_path: Path) -> None:
    staging = make_local_staging_dir(
        tmp_folder=str(tmp_path),
        name="stage",
        use_managed_tmp=False,
        remove_on_cleanup=True,
    )
    assert staging.path.exists()

    staging.cleanup()

    assert not staging.path.exists()


def test_copy_file_to_local_staging_and_named_temp(tmp_path: Path) -> None:
    src = tmp_path / "source.keras"
    src.write_bytes(b"model")
    staging = make_local_staging_dir(
        tmp_folder=str(tmp_path),
        name="stage",
        use_managed_tmp=False,
        remove_on_cleanup=True,
    )

    staged = copy_file_to_local_staging(str(src), staging, label="test file")
    assert staged.read_bytes() == b"model"

    with copied_file_to_local_temp(str(src), suffix=".keras", label="test temp") as temp_path:
        assert temp_path.read_bytes() == b"model"

    staging.cleanup()
