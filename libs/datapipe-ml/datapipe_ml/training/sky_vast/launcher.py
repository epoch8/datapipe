from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

from fsspec import AbstractFileSystem

from datapipe_ml.training.sky_vast.constants import (
    REMOTE_INPUT,
    REMOTE_OUTPUT,
    REMOTE_PAYLOAD,
    REMOTE_RESULT,
    REMOTE_ROOT,
    REMOTE_SIGNALS,
    REMOTE_SOURCE_ARCHIVE,
    REMOTE_WORKER_ENTRYPOINT,
)
from datapipe_ml.training.sky_vast.serialization import (
    dumps_to_text,
    loads_from_text,
    rewrite_value,
    to_remote_request,
)
from datapipe_ml.training.sky_vast.task_builder import build_task
from datapipe_ml.training.sky_vast.transport import (
    Timeout,
    copy_fs_to_local_or_cloud,
    copy_local_or_cloud_to_fs,
    copy_project_source_to_remote,
    retry,
)
from datapipe_ml.training.specs import (
    SkyVastTrainingLauncherConfig,
    TrainingLaunchRequest,
)

logger = logging.getLogger("datapipe.ml.sky_vast")


class SkyVastTrainingError(RuntimeError):
    pass


class _PeriodicOutputSync:
    def __init__(
        self,
        launcher: "SkyVastTrainingLauncher",
        sshfs: AbstractFileSystem,
        request: TrainingLaunchRequest,
    ):
        self.launcher = launcher
        self.sshfs = sshfs
        self.request = request
        self.interval_s = launcher.config.output_sync_interval_s
        self._next_sync_at = time.time() + self.interval_s if self.interval_s and self.interval_s > 0 else None

    def maybe_sync(self) -> None:
        if self._next_sync_at is None or time.time() < self._next_sync_at:
            return
        self._next_sync_at = time.time() + self.interval_s  # type: ignore[operator]
        try:
            print("[sky-vast] periodic output sync", flush=True)
            self.launcher._copy_outputs(self.sshfs, self.request, label="periodic output")
        except Exception:
            logger.exception("Periodic Sky/Vast output sync failed")


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
        print(f"[sky-vast] build task: {cluster_name}", flush=True)
        task = self._build_task()
        print(f"[sky-vast] launch cluster: {cluster_name}", flush=True)
        try:
            self._launch_cluster(task, cluster_name)
        except BaseException:
            if self.config.down_on_finish:
                self._down_cluster(cluster_name)
            raise
        print(f"[sky-vast] connect ssh: {cluster_name}", flush=True)
        sshfs = self._connect_ssh(cluster_name)
        try:
            with _SkyLogsStreamer(cluster_name, enabled=self.config.stream_logs):
                print(f"[sky-vast] prepare remote: {cluster_name}", flush=True)
                self._prepare_remote(sshfs, request)
                print(f"[sky-vast] wait result: {cluster_name}", flush=True)
                self._wait_for_result(sshfs, request)
            print(f"[sky-vast] copy outputs: {cluster_name}", flush=True)
            self._copy_outputs(sshfs, request)
            print(f"[sky-vast] read result: {cluster_name}", flush=True)
            result_text = self._read_remote_text(sshfs, str(REMOTE_RESULT), label="read result")
            result = loads_from_text(result_text)
            result = rewrite_value(result, tuple((remote, local) for local, remote in request.path_rewrites))
            print(f"[sky-vast] done: {cluster_name}", flush=True)
            return result
        except BaseException:
            print(f"[sky-vast] failed: {cluster_name}", flush=True)
            self._print_remote_diagnostics(sshfs)
            raise
        finally:
            try:
                self._write_remote_text(sshfs, str(REMOTE_SIGNALS / "SHUTDOWN"), "", label="shutdown signal")
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
            host = getattr(handle, "head_ip", None) if handle is not None else None
            ports = getattr(handle, "stable_ssh_ports", []) if handle is not None else []
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
        cluster_name = getattr(self, "_current_cluster_name", None)
        deadline = time.time() + self.config.ssh_connect_timeout_s
        while time.time() < deadline:
            if sshfs.exists(str(REMOTE_SIGNALS / "VM_BOOT")):
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
                    copy_local_or_cloud_to_fs(
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
        output_sync = _PeriodicOutputSync(self, sshfs, request)
        while deadline is None or time.time() < deadline:
            if sshfs.exists(str(REMOTE_SIGNALS / "TRAIN_DONE")):
                return
            if sshfs.exists(str(REMOTE_SIGNALS / "SCRIPT_FAILED")):
                traceback_path = REMOTE_ROOT / "traceback.txt"
                details = ""
                if sshfs.exists(str(traceback_path)):
                    details = self._read_remote_text(sshfs, str(traceback_path), label="traceback")
                raise SkyVastTrainingError(f"Remote Sky/Vast training failed.\n{details}")
            output_sync.maybe_sync()
            time.sleep(self.config.poll_s)
        raise SkyVastTrainingError("Timed out waiting for remote Sky/Vast training result")

    def _copy_outputs(
        self, sshfs: AbstractFileSystem, request: TrainingLaunchRequest, *, label: str = "output"
    ) -> None:
        for local_path, remote_path in request.output_dirs:

            def _copy_output(local_path: str = local_path, remote_path: str = remote_path) -> None:
                copy_fs_to_local_or_cloud(
                    sshfs,
                    remote_path,
                    local_path,
                    label=f"[sky-vast] {label}",
                    concurrency=self.config.transfer_concurrency,
                )

            self._retry_transport(_copy_output, label=f"copy {label} {remote_path}")

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
                    print(f"[sky-vast] remote {path.name}:\n{details}", flush=True)
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
