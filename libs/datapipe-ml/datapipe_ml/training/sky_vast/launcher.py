from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional, Protocol, Sequence, runtime_checkable

from fsspec import AbstractFileSystem

from datapipe_ml.core.files import (
    copy_fs_to_url,
    copy_url_to_fs,
)
from datapipe_ml.training.sky_vast.constants import (
    REMOTE_INPUT,
    REMOTE_OUTPUT,
    REMOTE_PAYLOAD,
    REMOTE_RESULT,
    REMOTE_ROOT,
    REMOTE_SIGNALS,
    REMOTE_SOURCE_ARCHIVE,
    REMOTE_WORKER_ENTRYPOINT,
    TrainingSignal,
)
from datapipe_ml.training.sky_vast.serialization import (
    dumps_to_text,
    loads_from_text,
    rewrite_value,
    to_remote_request,
)
from datapipe_ml.training.sky_vast.source_archive import copy_project_source_to_remote
from datapipe_ml.training.sky_vast.task_builder import build_task
from datapipe_ml.training.sky_vast.utils import (
    Timeout,
    retry,
)
from datapipe_ml.training.specs import (
    SkyVastTrainingLauncherConfig,
    TrainingLaunchRequest,
)
from datapipe_ml.training.sync import PeriodicSyncScheduler

logger = logging.getLogger("datapipe.ml.sky_vast")


@runtime_checkable
class _SkyClusterHandle(Protocol):
    head_ip: str
    stable_ssh_ports: Sequence[int]


class SkyVastTrainingError(RuntimeError):
    pass


def _safe_cluster_part(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9-]+", "-", value)
    value = value.strip("-").lower()
    return value[:48] or "train"


class _SkyLogsStreamer:
    def __init__(self, cluster_name: str, *, enabled: bool):
        self.cluster_name = cluster_name
        self.enabled = enabled
        self._process: Optional[subprocess.Popen[str]] = None

    def __enter__(self):
        if self.enabled:
            venv_sky = Path(sys.executable).with_name("sky")
            sky_executable = str(venv_sky) if venv_sky.exists() else shutil.which("sky")
            if sky_executable is None:
                raise SkyVastTrainingError("Could not find sky executable for log streaming")
            self._process = subprocess.Popen(
                [sky_executable, "logs", self.cluster_name, "1"],
                text=True,
                env=os.environ.copy(),
            )
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._process is not None and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
        return False


class SkyVastTrainingLauncher:
    def __init__(self, config: SkyVastTrainingLauncherConfig):
        self.config = config
        self._current_cluster_name: Optional[str] = None

    def _retry_transport(self, fn: Callable[[], Any], *, label: str) -> Any:
        return retry(
            fn,
            attempts=self.config.transport_retries,
            sleep_s=self.config.transport_retry_sleep_s,
            label=label,
        )

    def launch(self, request: TrainingLaunchRequest) -> Any:
        cluster_name = f"{_safe_cluster_part(self.config.cluster_name)}-{_safe_cluster_part(request.cluster_suffix)}"
        cluster_name = cluster_name[:63].strip("-")
        self._current_cluster_name = cluster_name
        logger.info("[sky-vast] build task: %s", cluster_name)
        task = self._build_task()
        logger.info("[sky-vast] launch cluster: %s", cluster_name)
        try:
            self._launch_cluster(task, cluster_name)
        except BaseException:
            if self.config.down_on_finish:
                self._down_cluster(cluster_name)
            raise
        logger.info("[sky-vast] connect ssh: %s", cluster_name)
        sshfs = self._connect_ssh(cluster_name)
        try:
            with _SkyLogsStreamer(cluster_name, enabled=self.config.stream_logs):
                logger.info("[sky-vast] prepare remote: %s", cluster_name)
                self._prepare_remote(sshfs, request)
                logger.info("[sky-vast] wait result: %s", cluster_name)
                self._wait_for_result(sshfs, request)
            logger.info("[sky-vast] copy outputs: %s", cluster_name)
            self._copy_outputs(sshfs, request)
            logger.info("[sky-vast] read result: %s", cluster_name)
            result_text = self._read_remote_text(sshfs, str(REMOTE_RESULT), label="read result")
            result = loads_from_text(result_text)
            result = rewrite_value(result, tuple((remote, local) for local, remote in request.path_rewrites))
            logger.info("[sky-vast] done: %s", cluster_name)
            return result
        except BaseException:
            logger.info("[sky-vast] failed: %s", cluster_name)
            self._print_remote_diagnostics(sshfs)
            self._copy_failed_outputs_best_effort(sshfs, request)
            raise
        finally:
            try:
                self._write_remote_text(sshfs, str(TrainingSignal.path(TrainingSignal.SHUTDOWN)), "", label="shutdown signal")
            except Exception:
                logger.exception("Failed to write Sky/Vast shutdown signal")
            if self.config.down_on_finish:
                self._down_cluster(cluster_name)

    def _build_task(self):
        return build_task(self.config)

    def _launch_cluster(self, task: Any, cluster_name: str) -> None:
        import sky

        with Timeout(self.config.sky_launch_timeout_s, "calling sky.launch"):
            request_id = retry(
                lambda: sky.launch(
                    task,
                    cluster_name=cluster_name,
                    down=True,
                    idle_minutes_to_autostop=self.config.idle_minutes,
                ),
                attempts=self.config.max_reconnect,
                sleep_s=self.config.reconnect_sleep_s,
                label="sky.launch",
            )
            retry(
                lambda: sky.stream_and_get(request_id),
                attempts=self.config.max_reconnect,
                sleep_s=self.config.reconnect_sleep_s,
                label="sky.stream_and_get(launch)",
            )

    def _cluster_status(self, cluster_name: str) -> Optional[dict[str, Any]]:
        import sky

        with Timeout(self.config.sky_status_timeout_s, "calling sky.status"):
            req_id = retry(
                lambda: sky.status(
                    cluster_names=[cluster_name],
                    refresh=sky.StatusRefreshMode.FORCE,
                    all_users=True,
                    _include_credentials=True,
                ),
                attempts=self.config.max_reconnect,
                sleep_s=self.config.reconnect_sleep_s,
                label="sky.status",
            )
        with Timeout(self.config.sky_status_timeout_s, "calling sky.get(status)"):
            statuses = retry(
                lambda: sky.get(req_id),
                attempts=self.config.max_reconnect,
                sleep_s=self.config.reconnect_sleep_s,
                label="sky.get(status)",
            )
        return next(
            (item for item in statuses if item.get("name") == cluster_name or item.get("cluster_name") == cluster_name),
            None,
        )

    def _connect_ssh(self, cluster_name: str) -> AbstractFileSystem:
        import asyncssh
        from sshfs import SSHFileSystem

        deadline = time.time() + self.config.ssh_connect_timeout_s
        while time.time() < deadline:
            status = self._cluster_status(cluster_name)
            if status is None:
                time.sleep(self.config.poll_s)
                continue
            handle = status.get("handle")
            if not isinstance(handle, _SkyClusterHandle):
                time.sleep(self.config.poll_s)
                continue
            host = handle.head_ip
            ports = list(handle.stable_ssh_ports)
            if not host or not ports:
                time.sleep(self.config.poll_s)
                continue
            creds = status["credentials"]
            key_obj = (
                asyncssh.import_private_key(creds["ssh_private_key_content"])
                if creds.get("ssh_private_key_content")
                else None
            )
            return SSHFileSystem(
                host=host,
                port=int(ports[0]),
                username=creds["ssh_user"],
                client_keys=[key_obj],
                known_hosts=None,
                proxy_command=creds.get("ssh_proxy_command"),
            )
        raise SkyVastTrainingError(f"SSH did not become ready for Sky/Vast cluster {cluster_name!r}")

    def _prepare_remote(self, sshfs: AbstractFileSystem, request: TrainingLaunchRequest) -> None:
        cluster_name = self._current_cluster_name
        deadline = time.time() + self.config.ssh_connect_timeout_s
        while time.time() < deadline:
            if sshfs.exists(str(TrainingSignal.path(TrainingSignal.VM_BOOT))):
                break
            if cluster_name and self._job_failed(cluster_name):
                raise SkyVastTrainingError(f"Sky job failed before remote VM boot signal for {cluster_name!r}")
            time.sleep(self.config.poll_s)
        else:
            raise SkyVastTrainingError("Remote VM did not signal boot")
        sshfs.makedirs(str(REMOTE_ROOT), exist_ok=True)
        sshfs.makedirs(str(REMOTE_INPUT), exist_ok=True)
        sshfs.makedirs(str(REMOTE_OUTPUT), exist_ok=True)

        def _copy_source() -> None:
            copy_project_source_to_remote(sshfs, str(REMOTE_SOURCE_ARCHIVE))

        self._retry_transport(_copy_source, label="copy project source")
        for src, dst in request.path_rewrites:
            if src in request.input_dirs or any(src.startswith(input_dir) for input_dir in request.input_dirs):

                def _copy_input(src: str = src, dst: str = dst) -> None:
                    copy_url_to_fs(
                        src,
                        sshfs,
                        dst,
                        label="[sky-vast] input",
                        concurrency=self.config.transfer_concurrency,
                    )

                self._retry_transport(_copy_input, label=f"copy input {src}")
        remote_request = to_remote_request(request)
        worker_entrypoint_path = Path(__file__).with_name("worker_entrypoint.py")
        self._write_remote_text(sshfs, str(REMOTE_PAYLOAD), dumps_to_text(remote_request), label="payload")
        self._write_remote_text(
            sshfs,
            str(REMOTE_WORKER_ENTRYPOINT),
            worker_entrypoint_path.read_text(),
            label="worker entrypoint",
        )

    def _job_failed(self, cluster_name: str) -> bool:
        import sky

        try:
            queue_id = sky.queue(cluster_name=cluster_name, skip_finished=False, all_users=True)
            jobs = sky.get(queue_id)
        except Exception:
            return False
        terminal_failure_statuses = {"FAILED", "FAILED_DRIVER", "FAILED_SETUP", "CANCELLED"}
        return any(str(job.get("status")).split(".")[-1] in terminal_failure_statuses for job in jobs)

    def _wait_for_result(self, sshfs: AbstractFileSystem, request: TrainingLaunchRequest) -> None:
        deadline = None if self.config.run_timeout_s is None else time.time() + self.config.run_timeout_s
        output_sync = PeriodicSyncScheduler(self.config.output_sync_interval_s)
        cluster_name = self._current_cluster_name
        while deadline is None or time.time() < deadline:
            if sshfs.exists(str(TrainingSignal.path(TrainingSignal.TRAIN_DONE))):
                return
            if sshfs.exists(str(TrainingSignal.path(TrainingSignal.SCRIPT_FAILED))):
                traceback_path = REMOTE_ROOT / "traceback.txt"
                details = ""
                if sshfs.exists(str(traceback_path)):
                    details = self._read_remote_text(sshfs, str(traceback_path), label="traceback")
                raise SkyVastTrainingError(f"Remote Sky/Vast training failed.\n{details}")
            if cluster_name and self._job_failed(cluster_name):
                raise SkyVastTrainingError(f"Sky job failed before remote training result for {cluster_name!r}")
            def _periodic_output_sync() -> None:
                logger.info("[sky-vast] periodic output sync")
                self._copy_outputs(sshfs, request, label="periodic output")

            output_sync.maybe_run(_periodic_output_sync, label="periodic output")
            time.sleep(self.config.poll_s)
        raise SkyVastTrainingError("Timed out waiting for remote Sky/Vast training result")

    def _copy_outputs(
        self, sshfs: AbstractFileSystem, request: TrainingLaunchRequest, *, label: str = "output"
    ) -> None:
        for local_path, remote_path in request.output_dirs:

            def _copy_output(local_path: str = local_path, remote_path: str = remote_path) -> None:
                copy_fs_to_url(
                    sshfs,
                    remote_path,
                    local_path,
                    label=f"[sky-vast] {label}",
                    concurrency=self.config.transfer_concurrency,
                )

            self._retry_transport(_copy_output, label=f"copy {label} {remote_path}")

    def _copy_failed_outputs_best_effort(self, sshfs: AbstractFileSystem, request: TrainingLaunchRequest) -> None:
        try:
            self._copy_outputs(sshfs, request, label="failed output")
        except Exception:
            logger.exception("Failed to copy Sky/Vast outputs after remote failure")

    def _read_remote_text(self, sshfs: AbstractFileSystem, path: str, *, label: str) -> str:
        def _read() -> str:
            with sshfs.open(path, "r") as src:
                return src.read()

        return self._retry_transport(_read, label=label)

    def _write_remote_text(self, sshfs: AbstractFileSystem, path: str, text: str, *, label: str) -> None:
        def _write() -> None:
            with sshfs.open(path, "w") as out:
                out.write(text)

        self._retry_transport(_write, label=label)

    def _print_remote_diagnostics(self, sshfs: AbstractFileSystem) -> None:
        for path in (REMOTE_ROOT / "traceback.txt", REMOTE_RESULT):
            try:
                if sshfs.exists(str(path)):
                    details = self._read_remote_text(sshfs, str(path), label=f"diagnostic {path.name}")
                    logger.info("[sky-vast] remote %s:\n%s", path.name, details)
            except Exception:
                logger.exception("Failed to read remote diagnostic file %s", path)

    def _down_cluster(self, cluster_name: str) -> None:
        import sky

        try:
            down_id = retry(
                lambda: sky.down(cluster_name),
                attempts=self.config.max_reconnect,
                sleep_s=self.config.reconnect_sleep_s,
                label="sky.down",
            )
            retry(
                lambda: sky.stream_and_get(down_id),
                attempts=self.config.max_reconnect,
                sleep_s=self.config.reconnect_sleep_s,
                label="sky.stream_and_get(down)",
            )
        except Exception:
            logger.exception("Failed to down Sky/Vast cluster %s", cluster_name)
