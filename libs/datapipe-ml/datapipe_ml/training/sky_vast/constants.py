from pathlib import Path

REMOTE_ROOT = Path("/workspace/datapipe_ml")
REMOTE_INPUT = REMOTE_ROOT / "input"
REMOTE_OUTPUT = REMOTE_ROOT / "output"
REMOTE_SIGNALS = REMOTE_ROOT / "signals"
REMOTE_SOURCE = REMOTE_ROOT / "source"
REMOTE_SOURCE_ARCHIVE = REMOTE_ROOT / "source.tar.gz"
REMOTE_PAYLOAD = REMOTE_ROOT / "payload.txt"
REMOTE_RESULT = REMOTE_ROOT / "result.txt"
REMOTE_WORKER_ENTRYPOINT = REMOTE_ROOT / "worker_entrypoint.py"
