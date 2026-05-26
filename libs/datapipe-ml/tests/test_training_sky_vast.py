from __future__ import annotations

import importlib.util
import json
import os
import re
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

pytestmark = [pytest.mark.slow, pytest.mark.training]


def _load_tests_env() -> None:
    env_path = Path(__file__).with_name(".env")
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


_load_tests_env()

DEFAULT_SKY_VAST_CANDIDATE_LIMIT = 5
DEFAULT_SKY_VAST_ACCELERATOR_CANDIDATES = (
    "RTX4060:1",
    "RTX3060:1",
    "RTX3070:1",
    "RTXA4000:1",
    "T4:1",
    "P100:1",
)
DEFAULT_SKY_VAST_PREFERRED_GPUS = {"RTX4060", "RTX3060", "RTX3070", "RTXA4000", "T4", "P100"}
_BAD_OFFER_IDS: set[str] = set()


def _sky_cli() -> str:
    return str(Path(sys.executable).with_name("sky"))


def _live_vast_offer_query() -> str:
    return (
        "verified=true rentable=true rented=false "
        f"disk_space>={int(os.getenv('DATAPIPE_ML_SKY_VAST_DISK_SIZE_GB', '20'))} "
        "num_gpus=1 "
        f'cpu_ram>="{float(os.getenv("DATAPIPE_ML_SKY_VAST_MEMORY_GB", "16"))}"'
    )


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
    pd.DataFrame(rows).to_csv(catalog_path, index=False)


def _write_sky_vast_patch(patch_dir: Path) -> None:
    patch_dir.mkdir(parents=True, exist_ok=True)
    (patch_dir / "sitecustomize.py").write_text("""
from __future__ import annotations


def _patch_sky_vast_launch() -> None:
    try:
        from sky.provision.vast import utils as vast_utils
    except Exception:
        return
    if getattr(vast_utils, "_datapipe_live_vast_patch", False):
        return

    original_launch = vast_utils.launch

    def patched_launch(
        name,
        instance_type,
        region,
        disk_size,
        image_name,
        ports,
        preemptible,
        secure_only,
        private_docker_registry=None,
        login=None,
        create_instance_kwargs=None,
        ssh_public_key=None,
    ):
        original_vast_factory = vast_utils.vast.vast

        class LiveVastClient:
            def __init__(self, client):
                self._client = client

            def __getattr__(self, name):
                return getattr(self._client, name)

            def search_offers(self, query=None, *args, **kwargs):
                cpu_ram = float(instance_type.split("-")[-1]) / 1024
                gpu_name = instance_type.split("-")[1].replace("_", " ")
                num_gpus = int(instance_type.split("-")[0].replace("x", ""))
                live_query = [
                    "verified=true",
                    "rentable=true",
                    "rented=false",
                    f"disk_space>={disk_size}",
                    f"num_gpus={num_gpus}",
                    f"gpu_name=\\"{gpu_name}\\"",
                    f"cpu_ram>=\\"{cpu_ram}\\"",
                ]
                if secure_only:
                    live_query.extend(["datacenter=true", "hosting_type>=1"])
                kwargs.setdefault("order", "dph_total+")
                return self._client.search_offers(query=" ".join(live_query), *args, **kwargs)

        def live_vast_factory():
            return LiveVastClient(original_vast_factory())

        vast_utils.vast.vast = live_vast_factory
        try:
            return original_launch(
                name=name,
                instance_type=instance_type,
                region=region,
                disk_size=disk_size,
                image_name=image_name,
                ports=ports,
                preemptible=preemptible,
                secure_only=secure_only,
                private_docker_registry=private_docker_registry,
                login=login,
                create_instance_kwargs=create_instance_kwargs,
                ssh_public_key=ssh_public_key,
            )
        finally:
            vast_utils.vast.vast = original_vast_factory

    vast_utils.launch = patched_launch
    vast_utils._datapipe_live_vast_patch = True


_patch_sky_vast_launch()
""".lstrip())


@pytest.fixture
def sky_vast_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _load_tests_env()
    api_key = os.getenv("VAPI_API_KEY")
    original_home = Path.home()
    sky_home = tmp_path / "sky-home"
    sky_home.mkdir()
    patch_dir = tmp_path / "sky-patches"
    _write_sky_vast_patch(patch_dir)
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


def _vast_region_code_from_infra(infra: str) -> str | None:
    if "/" not in infra:
        return None
    return infra.split("/", 1)[1].strip().split(",")[-1].strip()


def _candidate_first_offer_id(infra: str, accelerator: str) -> str | None:
    try:
        import vastai_sdk

        gpu_name, gpu_count = _split_accelerator(accelerator)
        query = ["chunked=true"]
        region_code = _vast_region_code_from_infra(infra)
        if region_code:
            query.extend(["georegion=true", f'geolocation="{region_code}"'])
        query.extend(
            [
                f"disk_space>={int(os.getenv('DATAPIPE_ML_SKY_VAST_DISK_SIZE_GB', '20'))}",
                f"num_gpus={gpu_count}",
                f'gpu_name="{_vast_gpu_name(gpu_name)}"',
                f'cpu_ram>="{float(os.getenv("DATAPIPE_ML_SKY_VAST_MEMORY_GB", "16"))}"',
            ]
        )
        offers: list[dict[str, Any]] = vastai_sdk.VastAI().search_offers(query=" ".join(query), limit=1)
    except Exception:
        return None
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
    if accelerators is None:
        preferred_gpus = set(
            os.getenv("DATAPIPE_ML_SKY_VAST_PREFERRED_GPUS", ",".join(DEFAULT_SKY_VAST_PREFERRED_GPUS)).split(",")
        )
    else:
        preferred_gpus = {_split_accelerator(accelerator)[0] for accelerator in accelerators}
    candidates = []
    seen = set()
    for offer in offers:
        offer_id = str(offer.get("id"))
        if not offer_id or offer_id in bad_offer_ids:
            continue
        gpu_name = offer.get("gpu_name")
        gpu_count = int(offer.get("num_gpus") or 1)
        if not gpu_name:
            continue
        if re.search(r"\b(Ti|TI)\b", str(gpu_name)):
            continue
        sky_gpu_name = _sky_accelerator_name(str(gpu_name))
        if sky_gpu_name not in _sky_vast_catalog_accelerators():
            continue
        if preferred_gpus and sky_gpu_name not in preferred_gpus:
            continue
        accelerator = f"{sky_gpu_name}:{gpu_count}"
        infra = "vast"
        instance_type = _sky_vast_instance_type(offer)
        candidate = (infra, accelerator, instance_type)
        if candidate in seen:
            continue
        seen.add(candidate)
        candidates.append((infra, accelerator, offer_id, instance_type))
        if len(candidates) >= limit:
            break
    return tuple(candidates)


def _sky_vast_candidates() -> tuple[SkyVastCandidate, ...]:
    configured = _env_optional("DATAPIPE_ML_SKY_VAST_ACCELERATORS")
    if configured:
        live_candidates = _live_vast_candidates(accelerators=(configured,))
        if live_candidates:
            return live_candidates
        raw_items = (f"{os.getenv('DATAPIPE_ML_SKY_VAST_INFRA', 'vast')}|{configured}",)
    else:
        raw_candidates = os.getenv("DATAPIPE_ML_SKY_VAST_CANDIDATES") or os.getenv(
            "DATAPIPE_ML_SKY_VAST_ACCELERATOR_CANDIDATES"
        )
        if raw_candidates:
            separator = ";" if ";" in raw_candidates else ","
            raw_items = tuple(candidate.strip() for candidate in raw_candidates.split(separator) if candidate.strip())
        else:
            live_candidates = _live_vast_candidates()
            if live_candidates:
                return live_candidates
            raw_items = tuple(
                f"{os.getenv('DATAPIPE_ML_SKY_VAST_INFRA', 'vast')}|{candidate}"
                for candidate in DEFAULT_SKY_VAST_ACCELERATOR_CANDIDATES
            )
    bad_offer_ids = _read_bad_offer_ids()
    candidates = []
    for item in raw_items:
        if "|" in item:
            infra, accelerator = item.split("|", 1)
        else:
            infra, accelerator = os.getenv("DATAPIPE_ML_SKY_VAST_INFRA", "vast"), item
        infra, accelerator = infra.strip(), accelerator.strip()
        offer_id = _candidate_first_offer_id(infra, accelerator) if infra and accelerator else None
        if infra and accelerator and (offer_id is None or offer_id not in bad_offer_ids):
            candidates.append((infra, accelerator, offer_id, None))
    return tuple(candidates)


def _sky_vast_all_candidates_are_cached() -> bool:
    configured = _env_optional("DATAPIPE_ML_SKY_VAST_ACCELERATORS")
    if configured:
        return not _sky_vast_candidates()
    raw_candidates = os.getenv("DATAPIPE_ML_SKY_VAST_CANDIDATES") or os.getenv(
        "DATAPIPE_ML_SKY_VAST_ACCELERATOR_CANDIDATES"
    )
    if raw_candidates:
        separator = ";" if ";" in raw_candidates else ","
        raw_items = tuple(candidate.strip() for candidate in raw_candidates.split(separator) if candidate.strip())
        return bool(raw_items) and not _sky_vast_candidates()
    return not _sky_vast_candidates()


def _is_sky_vast_resource_unavailable(exc: BaseException) -> bool:
    message = str(exc)
    return any(
        marker in message
        for marker in [
            "ResourcesUnavailableError",
            "Failed to acquire resources",
            "Error response from daemon",
            "context deadline exceeded",
            "Client.Timeout exceeded",
            "manifest unknown",
            "No resource satisfying",
            "Sky job failed before remote VM boot signal",
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
) -> SkyVastTrainingLauncherConfig:
    return SkyVastTrainingLauncherConfig(
        cluster_name=cluster_name,
        infra=infra,
        instance_type=instance_type,
        accelerators=accelerators if accelerators is not None else os.getenv("DATAPIPE_ML_SKY_VAST_ACCELERATORS", ""),
        cpus=os.getenv("DATAPIPE_ML_SKY_VAST_CPUS", "1+"),
        memory=os.getenv("DATAPIPE_ML_SKY_VAST_MEMORY", "1+"),
        disk_size=os.getenv("DATAPIPE_ML_SKY_VAST_DISK_SIZE", "20GB"),
        image_id=os.getenv("DATAPIPE_ML_SKY_VAST_IMAGE_ID", "vastai/base-image:@vastai-automatic-tag"),
        idle_minutes=int(os.getenv("DATAPIPE_ML_SKY_VAST_IDLE_MINUTES", "60")),
        max_reconnect=int(os.getenv("DATAPIPE_ML_SKY_VAST_MAX_RECONNECT", "0")),
        run_timeout_s=run_timeout_s,
        sky_launch_timeout_s=int(os.getenv("DATAPIPE_ML_SKY_LAUNCH_TIMEOUT_S", "180")),
        sky_status_timeout_s=int(os.getenv("DATAPIPE_ML_SKY_STATUS_TIMEOUT_S", "60")),
        source_install_extras=tuple(
            extra.strip()
            for extra in os.getenv("DATAPIPE_ML_SKY_VAST_SOURCE_INSTALL_EXTRAS", "torch").split(",")
            if extra.strip()
        ),
        stream_logs=os.getenv("DATAPIPE_ML_SKY_VAST_STREAM_LOGS", "1") == "1",
        down_on_finish=True,
    )


def _launch_minimal_worker_with_candidates(candidates: Iterable[SkyVastCandidate]) -> dict:
    from datapipe_ml.training.sky_vast.smoke import remote_smoke_process

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
    pytest.skip("No configured Sky/Vast accelerator candidate was currently available:\n" + "\n".join(resource_errors))


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
    pytest.skip("No configured Sky/Vast accelerator candidate was currently available:\n" + "\n".join(resource_errors))


def test_sky_vast_config_builds_launcher_without_credentials():
    config = SkyVastTrainingLauncherConfig(cluster_name="test-sky-vast")
    launcher = build_training_launcher(config)
    assert launcher.__class__.__name__ == "SkyVastTrainingLauncher"


def test_sky_vast_remote_request_rewrites_paths():
    request = TrainingLaunchRequest(
        target=str,
        args=({"path": "/tmp/local/data/file.txt"},),
        cluster_suffix="unit",
        input_dirs=("/tmp/local/data",),
        output_dirs=(("/tmp/local/models", "/tmp/local/models"),),
        path_rewrites=(("/tmp/local/data", "/workspace/input/data"),),
    )
    remote = to_remote_request(request)
    assert remote.args == ({"path": "/workspace/input/data/file.txt"},)
    assert loads_from_text(dumps_to_text(remote)).args == remote.args


def test_sky_vast_rewrites_remote_results_back_to_local_paths():
    result = {"model_path": "/workspace/datapipe_ml/output/models/run/weights/best.pt"}
    assert rewrite_value(
        result,
        (("/workspace/datapipe_ml/output/models", "/tmp/local/models"),),
    ) == {"model_path": "/tmp/local/models/run/weights/best.pt"}


def test_sky_vast_periodic_output_sync_copies_remote_outputs(tmp_path):
    import fsspec

    from datapipe_ml.training.sky_vast.launcher import (
        SkyVastTrainingLauncher,
        _PeriodicOutputSync,
    )

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

    output_sync = _PeriodicOutputSync(launcher, sshfs, request)
    output_sync._next_sync_at = 0
    output_sync.maybe_sync()

    assert (local_models_dir / "run" / "weights" / "best.pt").read_bytes() == b"checkpoint"


def test_sky_vast_transport_skips_copy_when_paths_are_same():
    import fsspec

    from datapipe_ml.training.sky_vast.transport import copy_tree_between_fs

    fs = fsspec.filesystem("memory")
    fs.makedirs("/same/path", exist_ok=True)
    with fs.open("/same/path/file.txt", "wb") as out:
        out.write(b"original")

    copy_tree_between_fs(fs, "/same/path", fs, "/same/path", concurrency=4)

    with fs.open("/same/path/file.txt", "rb") as src:
        assert src.read() == b"original"


def test_sky_vast_transport_parallel_copy_copies_tree(tmp_path):
    import fsspec

    from datapipe_ml.training.sky_vast.transport import copy_tree_between_fs

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
    _load_tests_env()
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
    assert os.getenv("VAPI_API_KEY"), "tests/.env must provide VAPI_API_KEY for Sky/Vast integration tests"


@pytest.mark.sky_vast
def test_sky_vast_launches_minimal_worker_from_scratch(sky_vast_environment):
    if not _sky_vast_available():
        pytest.skip("Set DATAPIPE_ML_RUN_SKY_VAST=1 and VAPI_API_KEY to run this test.")
    if _sky_vast_all_candidates_are_cached():
        pytest.skip("All configured Sky/Vast candidates point to bad offers already seen in this test process.")

    result = _launch_minimal_worker_with_candidates(_sky_vast_candidates())
    assert result == {"ok": True}


@pytest.mark.torch
@pytest.mark.sky_vast
def test_yolov8_detection_training_smoke_sky_vast(tmp_path, sky_vast_environment):
    if not _sky_vast_available():
        pytest.skip("Set DATAPIPE_ML_RUN_SKY_VAST=1 and VAPI_API_KEY to run this test.")
    if _sky_vast_all_candidates_are_cached():
        pytest.skip("All configured Sky/Vast candidates point to bad offers already seen in this test process.")

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
        )

        run_pipeline(runtime, [detection_freeze_step(attempt_path), train_step])
        assert_model_artifact(
            runtime,
            "detection_model",
            "detection_model__type",
            "detection_model__model_path",
            "yolov8",
        )

    _run_with_accelerator_candidates(tmp_path, _run)


@pytest.mark.torch
@pytest.mark.sky_vast
def test_yolov8n_detection_training_sky_vast_rtx3060(tmp_path, sky_vast_environment):
    if not _sky_vast_available():
        pytest.skip("Set DATAPIPE_ML_RUN_SKY_VAST=1 and VAPI_API_KEY to run this test.")
    if _sky_vast_all_candidates_are_cached():
        pytest.skip("All configured Sky/Vast candidates point to bad offers already seen in this test process.")

    from datapipe_ml.frameworks.yolo.yolov8.runner import YoloV8_TrainingConfig
    from datapipe_ml.tasks.detection.train.yolov8 import Train_YoloV8_DetectionModel
    from tests.helpers.training_smoke import (
        PRIMARY_KEYS,
        assert_model_artifact,
        detection_freeze_step,
        make_runtime,
        run_pipeline,
    )

    def _run(attempt_path: Path, infra: str, accelerator: str, instance_type: str | None) -> None:
        runtime = make_runtime(attempt_path)
        cluster_name = f"datapipe-ml-yolov8n-test-{uuid.uuid4().hex[:8]}"
        train_step = Train_YoloV8_DetectionModel(
            input__detection_frozen_dataset="detection_frozen_dataset",
            input__detection_frozen_dataset__has__image_gt="detection_frozen_dataset__has__image_gt",
            output__yolov8_train_config="yolov8_train_config",
            output__detection_size_for_resize="yolov8_detection_size_for_resize",
            output__detection_frozen_dataset__resized_image_file="yolov8_detection_resized_image_file",
            output__detection_frozen_dataset__yolo_txt="yolov8_detection_yolo_txt",
            output__detection_frozen_dataset__class_names="yolov8_detection_class_names",
            output__detection_model="detection_model",
            output__detection_model_is_trained_on_detection_frozen_dataset="detection_model_link",
            working_dir=str(attempt_path),
            yolov8_train_configs=[
                YoloV8_TrainingConfig(
                    model="yolov8n.pt",
                    imgsz=64,
                    batch=4,
                    epochs=20,
                    seed=42,
                    device="0",
                    workers=0,
                    patience=20,
                    amp=False,
                    val=True,
                    plots=False,
                )
            ],
            primary_keys=PRIMARY_KEYS,
            create_table=True,
            ignore_errors_sample_sizes=True,
            tmp_folder=str(attempt_path / "tmp"),
            model_suffix="_sky_vast_yolov8n",
            training_launcher_config=_sky_vast_config(
                cluster_name=cluster_name,
                infra=infra,
                accelerators=accelerator,
                instance_type=instance_type,
                run_timeout_s=900,
            ),
        )

        run_pipeline(runtime, [detection_freeze_step(attempt_path), train_step])
        assert_model_artifact(
            runtime,
            "detection_model",
            "detection_model__type",
            "detection_model__model_path",
            "yolov8",
        )

    _run_with_accelerator_candidates(tmp_path, _run)
