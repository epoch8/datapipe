from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable

import pytest

from datapipe_ml.training.sky_vast.serialization import (
    dumps_to_text,
    loads_from_text,
    rewrite_value,
    to_remote_request,
)
from datapipe_ml.training.specs import (
    SkyVastTrainingLauncherConfig,
    TrainingLaunchRequest,
    build_training_launcher,
)
from tests.helpers.training_smoke import assert_yolov8_training_artifacts

from tests.helpers.test_env import load_test_env

pytestmark = [pytest.mark.slow, pytest.mark.training]


load_test_env()

DEFAULT_SKY_VAST_CANDIDATE_LIMIT = 5
_BAD_OFFER_IDS: set[str] = set()


def _sky_cli() -> str:
    return shutil.which("sky") or "sky"


def _live_vast_offer_query() -> str:
    parts = [
        "verified=true",
        "rentable=true",
        "rented=false",
        f"disk_space>={int(os.getenv('DATAPIPE_ML_SKY_VAST_DISK_SIZE_GB', '20'))}",
        "num_gpus=1",
        f'cpu_ram>="{float(os.getenv("DATAPIPE_ML_SKY_VAST_MEMORY_GB", "16"))}"',
    ]
    if os.getenv("DATAPIPE_ML_SKY_VAST_SECURE_ONLY", "1") == "1":
        parts.append("datacenter=true")
    return " ".join(parts)


def _sky_vast_country_filter() -> str:
    return os.getenv("DATAPIPE_ML_SKY_VAST_COUNTRY", "US").strip().upper()


def _offer_matches_country(offer: dict[str, Any]) -> bool:
    country = _sky_vast_country_filter()
    if not country:
        return True
    geolocation = str(offer.get("geolocation") or "").strip().upper()
    return geolocation.endswith(f", {country}") or geolocation == country


def _sky_vast_instance_type(offer: dict[str, Any]) -> str:
    query_ram_mb = int(os.getenv("DATAPIPE_ML_SKY_VAST_QUERY_RAM_MB", "1024"))
    offer_id = int(offer.get("id") or 0)
    cpu_token = int(offer.get("cpu_cores") or 1) * 1_000_000_000 + offer_id
    return (
        f"{int(offer.get('num_gpus') or 1)}x-{str(offer['gpu_name']).replace(' ', '_')}-" f"{cpu_token}-{query_ram_mb}"
    )


def _write_live_sky_vast_catalog(sky_home: Path) -> None:
    if not _sky_vast_available():
        return
    try:
        import pandas as pd
        import vastai_sdk
    except Exception:
        return

    offers: list[dict[str, Any]] = vastai_sdk.VastAI().search_offers(
        query=_live_vast_offer_query(),
        limit=int(os.getenv("DATAPIPE_ML_SKY_VAST_CATALOG_OFFER_LIMIT", "200")),
        order="dph_total+",
    )
    rows = []
    seen = set()
    for offer in offers:
        if not _offer_matches_country(offer):
            continue
        gpu_name = offer.get("gpu_name")
        if not gpu_name:
            continue
        if os.getenv("DATAPIPE_ML_SKY_VAST_ALLOW_TI", "0") != "1" and re.search(r"\b(Ti|TI)\b", str(gpu_name)):
            continue
        accelerator_name = _sky_accelerator_name(str(gpu_name))
        if accelerator_name not in _sky_vast_catalog_accelerators():
            continue
        instance_type = _sky_vast_instance_type(offer)
        region = str(offer.get("geolocation") or "").strip()
        if not region or (instance_type, region) in seen:
            continue
        seen.add((instance_type, region))
        gpu_total_ram = int(offer.get("gpu_total_ram") or 0)
        rows.append(
            {
                "InstanceType": instance_type,
                "AcceleratorName": accelerator_name,
                "AcceleratorCount": int(offer.get("num_gpus") or 1),
                "vCPUs": int(offer.get("cpu_cores") or 1),
                "MemoryGiB": float(offer.get("cpu_ram") or 0) / 1024,
                "GpuInfo": json.dumps(
                    {
                        "Gpus": [
                            {
                                "Name": accelerator_name,
                                "Count": int(offer.get("num_gpus") or 1),
                                "MemoryInfo": {"SizeInMiB": gpu_total_ram},
                            }
                        ],
                        "TotalGpuMemoryInMiB": gpu_total_ram,
                    }
                ).replace('"', "'"),
                "Price": float(offer.get("dph_total") or 0),
                "SpotPrice": float(offer.get("min_bid") or offer.get("dph_total") or 0),
                "Region": region,
                "HostingType": int(offer.get("hosting_type") or 0),
            }
        )
    catalog_path = sky_home / ".sky" / "catalogs" / "v8" / "vast" / "vms.csv"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.sort_values("Price", ascending=True)
    frame.to_csv(catalog_path, index=False)


def _write_skypilot_config(sky_home: Path) -> None:
    config_dir = sky_home / ".sky"
    config_dir.mkdir(parents=True, exist_ok=True)
    datacenter_only = os.getenv("DATAPIPE_ML_SKY_VAST_SECURE_ONLY", "1") == "1"
    (config_dir / "config.yaml").write_text(f"vast:\n  datacenter_only: {str(datacenter_only).lower()}\n")


def _refresh_sky_vast_catalog() -> None:
    sky_home = Path(os.environ.get("HOME", Path.home()))
    if _sky_vast_available():
        _write_live_sky_vast_catalog(sky_home)


def _write_sky_vast_patch(patch_dir: Path) -> None:
    patch_dir.mkdir(parents=True, exist_ok=True)
    helper_path = Path(__file__).with_name("helpers") / "sky_vast_sitecustomize.py"
    (patch_dir / "sitecustomize.py").write_text(helper_path.read_text())
    # PYTHONPATH alone does not auto-import sitecustomize; a .pth hook does.
    (patch_dir / "datapipe_sky_vast.pth").write_text("import sitecustomize\n")


def _apply_sky_vast_runtime_patches() -> None:
    from tests.helpers import sky_vast_sitecustomize

    sky_vast_sitecustomize._patch_sky_vast_launch()


@pytest.fixture
def sky_vast_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    load_test_env()
    api_key = os.getenv("VAPI_API_KEY")
    original_home = Path.home()
    sky_home = tmp_path / "sky-home"
    sky_home.mkdir()
    patch_dir = tmp_path / "sky-patches"
    _write_sky_vast_patch(patch_dir)
    _apply_sky_vast_runtime_patches()
    pythonpath = str(patch_dir)
    if os.getenv("PYTHONPATH"):
        pythonpath = pythonpath + os.pathsep + os.environ["PYTHONPATH"]
    monkeypatch.setenv("PYTHONPATH", pythonpath)
    use_isolated_sky = _sky_vast_available()
    if use_isolated_sky:
        subprocess.run([_sky_cli(), "api", "stop"], check=False)
    monkeypatch.setenv("HOME", str(sky_home))
    if api_key:
        vast_credentials_path = sky_home / ".config" / "vastai" / "vast_api_key"
        vast_credentials_path.parent.mkdir(parents=True, exist_ok=True)
        vast_credentials_path.write_text(api_key)
    _write_skypilot_config(sky_home)
    _write_live_sky_vast_catalog(sky_home)
    yield
    if use_isolated_sky:
        subprocess.run([_sky_cli(), "api", "stop"], check=False)
    monkeypatch.setenv("HOME", str(original_home))


def _sky_vast_available() -> bool:
    return (
        os.getenv("DATAPIPE_ML_RUN_SKY_VAST") == "1"
        and bool(os.getenv("VAPI_API_KEY"))
        and importlib.util.find_spec("sky") is not None
        and importlib.util.find_spec("sshfs") is not None
    )


def _env_optional(name: str) -> str | None:
    value = os.getenv(name)
    return value if value else None


def _read_bad_offer_ids() -> set[str]:
    return set(_BAD_OFFER_IDS)


def _remember_bad_offer(offer_id: str) -> None:
    _BAD_OFFER_IDS.add(str(offer_id))


def _split_accelerator(accelerator: str) -> tuple[str, int]:
    name, _, count = accelerator.partition(":")
    return name, int(float(count or "1"))


def _vast_gpu_name(accelerator_name: str) -> str:
    explicit_names = {
        "RTXPRO6000WS": "RTX PRO 6000 WS",
        "RTXPRO6000S": "RTX PRO 6000 S",
        "RTXA4000": "RTX A4000",
        "RTXA5000": "RTX A5000",
        "RTXA6000": "RTX A6000",
        "RTX6000ADA": "RTX 6000 Ada",
        "RTX5880ADA": "RTX 5880 Ada",
    }
    if accelerator_name in explicit_names:
        return explicit_names[accelerator_name]
    match = re.fullmatch(r"RTX(\d{4})", accelerator_name)
    if match:
        return f"RTX {match.group(1)}"
    return accelerator_name.replace("_", " ")


def _sky_accelerator_name(vast_gpu_name: str) -> str:
    gpu = re.sub("Ada", "-Ada", re.sub(r"\s", "", vast_gpu_name))
    gpu = re.sub(r"(Ti|PCIE|SXM4|SXM|NVL)$", "", gpu)
    gpu = re.sub(r"(RTX\d0\d0)(S|D)$", r"\1", gpu)
    return {
        "TeslaV100": "V100",
        "TeslaT4": "T4",
        "TeslaP100": "P100",
        "QRTX6000": "RTX6000",
        "QRTX8000": "RTX8000",
    }.get(gpu, gpu)


def _sky_vast_catalog_accelerators() -> set[str]:
    return {
        "A100",
        "H100",
        "H200",
        "L40S",
        "RTX3060",
        "RTX3090",
        "RTX4060",
        "RTX4070",
        "RTX4090",
        "RTX5070",
        "RTX5090",
        "RTX5880-Ada",
        "RTX6000-Ada",
        "RTXA5000",
        "RTXA6000",
        "RTXPRO6000WS",
    }


def _candidate_first_offer_id(infra: str, accelerator: str) -> str | None:
    try:
        import vastai_sdk

        gpu_name, gpu_count = _split_accelerator(accelerator)
        query = ["chunked=true"]
        query.extend(
            [
                f"disk_space>={int(os.getenv('DATAPIPE_ML_SKY_VAST_DISK_SIZE_GB', '20'))}",
                f"num_gpus={gpu_count}",
                f'gpu_name="{_vast_gpu_name(gpu_name)}"',
                f'cpu_ram>="{float(os.getenv("DATAPIPE_ML_SKY_VAST_MEMORY_GB", "16"))}"',
            ]
        )
        if os.getenv("DATAPIPE_ML_SKY_VAST_SECURE_ONLY", "1") == "1":
            query.append("datacenter=true")
        offers: list[dict[str, Any]] = vastai_sdk.VastAI().search_offers(query=" ".join(query), limit=200)
    except Exception:
        return None
    offers = [offer for offer in offers if _offer_matches_country(offer)]
    if not offers:
        return None
    offer_id = offers[0].get("id")
    return str(offer_id) if offer_id is not None else None


SkyVastCandidate = tuple[str, str, str | None, str | None]


def _live_vast_candidates(
    limit: int = DEFAULT_SKY_VAST_CANDIDATE_LIMIT,
    *,
    accelerators: Iterable[str] | None = None,
) -> tuple[SkyVastCandidate, ...]:
    try:
        import vastai_sdk

        offers: list[dict[str, Any]] = vastai_sdk.VastAI().search_offers(
            query=_live_vast_offer_query(),
            limit=limit * 10,
            order="dph_total+",
        )
    except Exception:
        return ()
    bad_offer_ids = _read_bad_offer_ids()
    allowed_gpus = (
        {_split_accelerator(accelerator)[0] for accelerator in accelerators} if accelerators is not None else None
    )
    candidates = []
    seen_offer_ids = set()
    for offer in offers:
        if not _offer_matches_country(offer):
            continue
        offer_id = str(offer.get("id"))
        if not offer_id or offer_id in bad_offer_ids or offer_id in seen_offer_ids:
            continue
        gpu_name = offer.get("gpu_name")
        if not gpu_name:
            continue
        if re.search(r"\b(Ti|TI)\b", str(gpu_name)):
            continue
        sky_gpu_name = _sky_accelerator_name(str(gpu_name))
        if sky_gpu_name not in _sky_vast_catalog_accelerators():
            continue
        if allowed_gpus is not None and sky_gpu_name not in allowed_gpus:
            continue
        gpu_count = int(offer.get("num_gpus") or 1)
        accelerator = f"{sky_gpu_name}:{gpu_count}"
        candidates.append(("vast", accelerator, offer_id, None))
        seen_offer_ids.add(offer_id)
        if len(candidates) >= limit:
            break
    return tuple(candidates)


def _sky_vast_candidates() -> tuple[SkyVastCandidate, ...]:
    _refresh_sky_vast_catalog()
    configured = _env_optional("DATAPIPE_ML_SKY_VAST_ACCELERATORS")
    if configured:
        live_candidates = _live_vast_candidates(accelerators=(configured,))
        if live_candidates:
            return live_candidates
        offer_id = _candidate_first_offer_id("vast", configured)
        if offer_id is None or offer_id in _read_bad_offer_ids():
            return ()
        return (("vast", configured, offer_id, None),)
    return _live_vast_candidates()


def _sky_vast_all_candidates_are_cached() -> bool:
    return not _sky_vast_candidates()


def _is_sky_vast_resource_unavailable(exc: BaseException) -> bool:
    message = str(exc)
    return any(
        marker in message
        for marker in [
            "ResourcesUnavailableError",
            "Failed to acquire resources",
            "ConnectionRefusedError",
            "Connection refused",
            "Error response from daemon",
            "context deadline exceeded",
            "Client.Timeout exceeded",
            "manifest unknown",
            "No resource satisfying",
            "Timed out while calling sky.launch",
            "Timed out while calling sky.stream_and_get(launch)",
            "SSH did not become ready for Sky/Vast cluster",
            "Remote VM did not signal boot",
            "Sky job failed before remote VM boot signal",
            "Timed out waiting for remote Sky/Vast training result",
            "FAILED_DRIVER",
            "FAILED_SETUP",
        ]
    )


def _has_active_datapipe_sky_cluster() -> bool:
    try:
        import sky

        request_id = sky.status(refresh="FORCE", all_users=True)
        statuses = sky.get(request_id)
    except Exception:
        return True
    return any(str(item.get("name") or item.get("cluster_name") or "").startswith("datapipe-ml-") for item in statuses)


def _should_retry_sky_vast_candidate(exc: BaseException) -> bool:
    return _is_sky_vast_resource_unavailable(exc)


def _sky_vast_config(
    cluster_name: str,
    *,
    infra: str = "vast",
    accelerators: str | None = None,
    instance_type: str | None = None,
    run_timeout_s: int = 7200,
    source_install_extras: tuple[str, ...] | None = None,
) -> SkyVastTrainingLauncherConfig:
    if source_install_extras is None:
        source_install_extras = tuple(
            extra.strip()
            for extra in os.getenv("DATAPIPE_ML_SKY_VAST_SOURCE_INSTALL_EXTRAS", "").split(",")
            if extra.strip()
        )
    return SkyVastTrainingLauncherConfig(
        cluster_name=cluster_name,
        infra=infra,
        instance_type=instance_type,
        accelerators="" if instance_type else (accelerators if accelerators is not None else os.getenv("DATAPIPE_ML_SKY_VAST_ACCELERATORS", "")),
        cpus=os.getenv("DATAPIPE_ML_SKY_VAST_CPUS", "1+"),
        memory=os.getenv("DATAPIPE_ML_SKY_VAST_MEMORY", "1+"),
        disk_size=os.getenv("DATAPIPE_ML_SKY_VAST_DISK_SIZE", "20GB"),
        image_id=os.getenv("DATAPIPE_ML_SKY_VAST_IMAGE_ID", "vastai/base-image:@vastai-automatic-tag"),
        idle_minutes=int(os.getenv("DATAPIPE_ML_SKY_VAST_IDLE_MINUTES", "60")),
        max_reconnect=int(os.getenv("DATAPIPE_ML_SKY_VAST_MAX_RECONNECT", "0")),
        run_timeout_s=run_timeout_s,
        sky_launch_timeout_s=int(os.getenv("DATAPIPE_ML_SKY_LAUNCH_TIMEOUT_S", "180")),
        sky_status_timeout_s=int(os.getenv("DATAPIPE_ML_SKY_STATUS_TIMEOUT_S", "60")),
        source_install_extras=source_install_extras,
        source_install_backend=os.getenv("DATAPIPE_ML_SKY_VAST_SOURCE_INSTALL_BACKEND", "uv"),
        source_install_deps=os.getenv("DATAPIPE_ML_SKY_VAST_SOURCE_INSTALL_DEPS", "1") == "1",
        stream_logs=os.getenv("DATAPIPE_ML_SKY_VAST_STREAM_LOGS", "1") == "1",
        down_on_finish=True,
    )


def _launch_minimal_worker_with_candidates(candidates: Iterable[SkyVastCandidate]) -> dict:
    from datapipe_ml.training.sky_vast.worker_entrypoint import remote_smoke_process

    resource_errors = []
    for infra, accelerator, offer_id, instance_type in candidates:
        launcher = build_training_launcher(
            _sky_vast_config(
                cluster_name=f"datapipe-ml-sky-bootstrap-{uuid.uuid4().hex[:8]}",
                infra=infra,
                accelerators=accelerator,
                instance_type=instance_type,
                run_timeout_s=900,
            )
        )
        try:
            return launcher.launch(
                TrainingLaunchRequest(
                    target=remote_smoke_process,
                    args=(),
                    cluster_suffix="minimal-worker",
                )
            )
        except Exception as exc:
            if not _should_retry_sky_vast_candidate(exc):
                raise
            if offer_id is not None:
                _remember_bad_offer(offer_id)
            resource_errors.append(f"{infra}|{accelerator}|{instance_type}: {exc}")
            print(f"[sky-vast] retry candidate after provisioning failure: {resource_errors[-1]}", flush=True)
    pytest.skip("No US GPU offers were currently available on Vast:\n" + "\n".join(resource_errors))


def _run_with_accelerator_candidates(
    tmp_path: Path,
    run_for_accelerator: Callable[[Path, str, str, str | None], None],
) -> None:
    resource_errors = []
    for infra, accelerator, offer_id, instance_type in _sky_vast_candidates():
        attempt_path = tmp_path / accelerator.replace(":", "-").lower()
        attempt_path.mkdir(parents=True, exist_ok=True)
        try:
            run_for_accelerator(attempt_path, infra, accelerator, instance_type)
            return
        except Exception as exc:
            if not _should_retry_sky_vast_candidate(exc):
                raise
            if offer_id is not None:
                _remember_bad_offer(offer_id)
            resource_errors.append(f"{infra}|{accelerator}|{instance_type}: {exc}")
            print(f"[sky-vast] retry candidate after provisioning failure: {resource_errors[-1]}", flush=True)
    pytest.skip("No US GPU offers were currently available on Vast:\n" + "\n".join(resource_errors))


def test_sky_vast_config_builds_launcher_without_credentials():
    config = SkyVastTrainingLauncherConfig(cluster_name="test-sky-vast")
    launcher = build_training_launcher(config)
    assert launcher.__class__.__name__ == "SkyVastTrainingLauncher"


def test_sky_vast_yolo_training_config_installs_torch_extra():
    from datapipe_ml.training.sky_vast.task_builder import _datapipe_ml_install_target

    config = _sky_vast_config(cluster_name="unit", source_install_extras=("torch",))
    assert _datapipe_ml_install_target(config) == "/workspace/datapipe_ml/source/datapipe-ml[torch]"


def test_sky_vast_worker_runs_from_uv_venv_by_default():
    from datapipe_ml.training.sky_vast.task_builder import _worker_python

    config = SkyVastTrainingLauncherConfig(cluster_name="unit")
    assert _worker_python(config) == "/workspace/datapipe_ml/.venv/bin/python"


def test_sky_vast_source_install_can_use_pip():
    from datapipe_ml.training.sky_vast.task_builder import _source_install_commands, _worker_python

    config = SkyVastTrainingLauncherConfig(cluster_name="unit", source_install_backend="pip")
    assert _source_install_commands(config, "/workspace/core", "/workspace/ml") == [
        "python3 -m pip install --user -e '/workspace/core'",
        "python3 -m pip install --user -e '/workspace/ml'",
    ]
    assert _worker_python(config) == "python3"


def test_sky_vast_wait_result_fails_when_sky_job_failed():
    from datapipe_ml.training.sky_vast.launcher import SkyVastTrainingError, SkyVastTrainingLauncher

    class EmptySignalFS:
        def exists(self, path: str) -> bool:
            return False

    launcher = SkyVastTrainingLauncher(
        SkyVastTrainingLauncherConfig(
            cluster_name="unit",
            poll_s=0,
            run_timeout_s=60,
            output_sync_interval_s=None,
        )
    )
    launcher._current_cluster_name = "unit-worker"
    launcher._job_failed = lambda cluster_name: True  # type: ignore[method-assign]

    with pytest.raises(SkyVastTrainingError, match="Sky job failed before remote training result"):
        launcher._wait_for_result(EmptySignalFS(), TrainingLaunchRequest(target=str, args=(), cluster_suffix="worker"))  # type: ignore[arg-type]


def test_sky_vast_failed_output_copy_is_best_effort():
    from datapipe_ml.training.sky_vast.launcher import SkyVastTrainingLauncher

    launcher = SkyVastTrainingLauncher(SkyVastTrainingLauncherConfig(cluster_name="unit"))
    calls = {"count": 0}

    def fail_copy_outputs(sshfs, request, *, label="output"):  # noqa: ANN001
        calls["count"] += 1
        assert label == "failed output"
        raise RuntimeError("copy failed")

    launcher._copy_outputs = fail_copy_outputs  # type: ignore[method-assign]

    launcher._copy_failed_outputs_best_effort(
        object(),  # type: ignore[arg-type]
        TrainingLaunchRequest(target=str, args=(), cluster_suffix="worker"),
    )

    assert calls["count"] == 1


def test_sky_vast_remote_request_rewrites_paths(tmp_path):
    local_data_dir = tmp_path / "data"
    request = TrainingLaunchRequest(
        target=str,
        args=({"path": str(local_data_dir / "file.txt")},),
        cluster_suffix="unit",
        input_dirs=(str(local_data_dir),),
        output_dirs=((str(tmp_path / "models"), str(tmp_path / "models")),),
        path_rewrites=((str(local_data_dir), "/workspace/input/data"),),
    )
    remote = to_remote_request(request)
    assert remote.args == ({"path": "/workspace/input/data/file.txt"},)
    assert loads_from_text(dumps_to_text(remote)).args == remote.args


def test_sky_vast_rewrites_remote_results_back_to_local_paths(tmp_path):
    local_models_dir = tmp_path / "models"
    result = {"model_path": "/workspace/datapipe_ml/output/models/run/weights/best.pt"}
    assert rewrite_value(
        result,
        (("/workspace/datapipe_ml/output/models", str(local_models_dir)),),
    ) == {"model_path": str(local_models_dir / "run" / "weights" / "best.pt")}


def test_sky_vast_periodic_output_sync_copies_remote_outputs(tmp_path):
    import fsspec

    from datapipe_ml.training.sky_vast.launcher import SkyVastTrainingLauncher
    from datapipe_ml.training.sync import PeriodicSyncScheduler

    sshfs = fsspec.filesystem("memory")
    remote_model_path = "/workspace/datapipe_ml/output/models/run/weights/best.pt"
    sshfs.makedirs("/workspace/datapipe_ml/output/models/run/weights", exist_ok=True)
    with sshfs.open(remote_model_path, "wb") as out:
        out.write(b"checkpoint")

    local_models_dir = tmp_path / "models"
    request = TrainingLaunchRequest(
        target=str,
        args=(),
        cluster_suffix="unit",
        output_dirs=((str(local_models_dir), "/workspace/datapipe_ml/output/models"),),
    )
    launcher = SkyVastTrainingLauncher(SkyVastTrainingLauncherConfig(cluster_name="unit", output_sync_interval_s=600))

    output_sync = PeriodicSyncScheduler(launcher.config.output_sync_interval_s)
    output_sync._next_sync_at = 0
    output_sync.maybe_run(
        lambda: launcher._copy_outputs(sshfs, request, label="periodic output"),
        label="periodic output",
    )

    assert (local_models_dir / "run" / "weights" / "best.pt").read_bytes() == b"checkpoint"


def test_core_files_copy_tree_skips_copy_when_paths_are_same():
    import fsspec

    from datapipe_ml.core.files import copy_tree_between_fs

    fs = fsspec.filesystem("memory")
    fs.makedirs("/same/path", exist_ok=True)
    with fs.open("/same/path/file.txt", "wb") as out:
        out.write(b"original")

    copy_tree_between_fs(fs, "/same/path", fs, "/same/path", concurrency=4)

    with fs.open("/same/path/file.txt", "rb") as src:
        assert src.read() == b"original"


def test_core_files_parallel_copy_copies_tree(tmp_path):
    import fsspec

    from datapipe_ml.core.files import copy_tree_between_fs

    src_fs = fsspec.filesystem("memory")
    dst_fs = fsspec.filesystem("file")
    for index in range(10):
        src_fs.makedirs(f"/parallel/src/nested/{index}", exist_ok=True)
        with src_fs.open(f"/parallel/src/nested/{index}/file.txt", "wb") as out:
            out.write(f"data-{index}".encode())

    copy_tree_between_fs(src_fs, "/parallel/src", dst_fs, str(tmp_path / "dst"), concurrency=4)

    for index in range(10):
        assert (tmp_path / "dst" / "nested" / str(index) / "file.txt").read_text() == f"data-{index}"


def test_sky_vast_transport_retry_recovers_after_transient_error():
    from datapipe_ml.training.sky_vast.launcher import SkyVastTrainingLauncher

    launcher = SkyVastTrainingLauncher(
        SkyVastTrainingLauncherConfig(
            cluster_name="unit",
            transport_retries=2,
            transport_retry_sleep_s=0,
        )
    )
    attempts = 0

    def flaky_operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("temporary network problem")
        return "ok"

    assert launcher._retry_transport(flaky_operation, label="unit transport") == "ok"
    assert attempts == 2


@pytest.mark.sky_vast
def test_sky_vast_runtime_bootstrap_is_available(sky_vast_environment):
    load_test_env()
    missing = [
        name
        for name, module_name in [
            ("skypilot", "sky"),
            ("sshfs", "sshfs"),
            ("asyncssh", "asyncssh"),
        ]
        if importlib.util.find_spec(module_name) is None
    ]
    assert missing == []
    assert os.getenv("VAPI_API_KEY"), (
        "Set VAPI_API_KEY in tests/.env.test.local (see tests/.env.test.local.example) "
        "or export it in the shell for Sky/Vast integration tests"
    )


@pytest.mark.sky_vast
def test_sky_vast_launches_minimal_worker_from_scratch(sky_vast_environment):
    if not _sky_vast_available():
        pytest.skip(
            "Set DATAPIPE_ML_RUN_SKY_VAST=1 and VAPI_API_KEY "
            "(tests/.env.test.local or shell env) to run this test."
        )
    if _sky_vast_all_candidates_are_cached():
        pytest.skip("All US GPU offers on Vast were already marked bad in this test process.")

    result = _launch_minimal_worker_with_candidates(_sky_vast_candidates())
    assert result == {"ok": True}


@pytest.mark.torch
@pytest.mark.sky_vast
def test_yolov8_detection_training_smoke_sky_vast(tmp_path, sky_vast_environment):
    if not _sky_vast_available():
        pytest.skip(
            "Set DATAPIPE_ML_RUN_SKY_VAST=1 and VAPI_API_KEY "
            "(tests/.env.test.local or shell env) to run this test."
        )
    if _sky_vast_all_candidates_are_cached():
        pytest.skip("All US GPU offers on Vast were already marked bad in this test process.")

    from tests.helpers.training_smoke import (
        assert_model_artifact,
        detection_freeze_step,
        detection_train_step,
        make_runtime,
        run_pipeline,
    )

    def _run(attempt_path: Path, infra: str, accelerator: str, instance_type: str | None) -> None:
        runtime = make_runtime(attempt_path)
        train_step = detection_train_step(attempt_path)
        train_step.training_launcher_config = _sky_vast_config(
            cluster_name=f"datapipe-ml-smoke-{uuid.uuid4().hex[:8]}",
            infra=infra,
            accelerators=accelerator,
            instance_type=instance_type,
            run_timeout_s=900,
            source_install_extras=("torch",),
        )

        run_pipeline(runtime, [detection_freeze_step(attempt_path), train_step])
        assert_model_artifact(
            runtime,
            "detection_model",
            "detection_model__type",
            "detection_model__model_path",
            "yolov8",
        )
        assert_yolov8_training_artifacts(runtime)

    _run_with_accelerator_candidates(tmp_path, _run)
