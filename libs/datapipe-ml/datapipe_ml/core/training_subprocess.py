from __future__ import annotations

import logging
from typing import Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

_TRAINING_FAILURE_EXCEPTIONS = (
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
    ImportError,
    FileNotFoundError,
    AttributeError,
)


def run_training_subprocess_body(
    action: Callable[[], T],
    on_failure: Callable[[str], None],
    failure_message: str = "Training model failed.",
) -> tuple[Optional[T], Optional[str]]:
    """Run training work and capture tracebacks for the multiprocessing queue contract."""
    try:
        return action(), None
    except (KeyboardInterrupt, SystemExit):
        raise
    except _TRAINING_FAILURE_EXCEPTIONS as exc:
        traceback_logs = _format_traceback(exc)
        on_failure(traceback_logs)
        logger.error("%s", failure_message)
        return None, traceback_logs
    except Exception as exc:
        traceback_logs = _format_traceback(exc)
        on_failure(traceback_logs)
        logger.exception("%s Unexpected error:", failure_message)
        return None, traceback_logs


def _format_traceback(exc: BaseException) -> str:
    from traceback_with_variables import format_exc

    return format_exc(exc)
